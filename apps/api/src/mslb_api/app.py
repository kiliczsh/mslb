"""Logical REST operations adapt shared services without executing simulations."""

import asyncio
import re
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, override
from uuid import UUID

from fastapi import APIRouter, FastAPI, Query, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPBearer
from mslb_application.errors import OperationError
from mslb_application.event_feed import EventFeed
from mslb_application.files import ManagedFiles
from mslb_application.queries import ArtifactName, ChartMetric, Queries
from mslb_application.submissions import Submissions
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.catalog import ServerView
from mslb_contracts.durability import PersistedEvent
from mslb_contracts.http import (
    CompareRequest,
    ErrorEnvelope,
    FieldError,
    RunDetail,
    ServerPatch,
    SimulationAccepted,
    SimulationCreate,
    StrategyView,
)
from mslb_contracts.inputs import InputError, ServerInput, normalize_identifier, read_servers
from mslb_contracts.queries import (
    ChartView,
    ComparisonView,
    DatasetView,
    MetricsView,
    Page,
    RunView,
    ServerMetricView,
)
from mslb_contracts.runs import RunSettings
from mslb_engine.models import SimulationError
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database, StoreError
from mslb_persistence.jobs import Jobs
from pydantic import BaseModel, ValidationError
from python_multipart.exceptions import MultipartParseError
from sqlalchemy.exc import DBAPIError
from starlette.datastructures import FormData, UploadFile
from starlette.exceptions import HTTPException
from starlette.formparsers import MultiPartException, MultiPartParser
from starlette.responses import Response

from mslb_api.errors import error_response
from mslb_api.guard import Guard
from mslb_api.settings import Settings
from mslb_api.streams import ArtifactResponse, EventResponse, finish

type Limit = Annotated[int, Query(ge=1, le=500)]
type Cursor = Annotated[str | None, Query(max_length=2048)]
type Tick = Annotated[int, Query(ge=0, le=1_000_000)]


class _Multipart(MultiPartParser):
    @override
    async def parse(self) -> FormData:
        try:
            return await super().parse()
        except BaseException:
            # Starlette closes these spools for its own multipart exceptions.
            # Also close them when our receive budget or client cancellation fires.
            for spool in self._files_to_close_on_error:
                await finish(spool.close)
            raise


def json_response(
    model: BaseModel, status: int = 200, headers: dict[str, str] | None = None
) -> Response:
    payload = model.model_dump_json().encode()
    if len(payload) > 1024**2:
        raise OperationError("RESOURCE_LIMIT_EXCEEDED", "Response exceeds its encoded byte budget")
    return Response(payload, status_code=status, media_type="application/json", headers=headers)


def seed() -> tuple[ServerInput, ...]:
    with Path("/fixtures/core/servers.json").open("rb") as stream:
        return read_servers(stream)


def version(request: Request) -> int:
    match = re.fullmatch(r'"([1-9][0-9]{0,9})"', request.headers.get("if-match", ""))
    if match is None:
        raise OperationError("CONFLICT", 'Supply the current strong ETag in If-Match, such as "1"')
    return int(match[1])


def create_app(
    settings: Settings,
    database: Database,
    inputs: ManagedFiles,
    outputs: ManagedFiles,
    signing_key: bytes,
    close_database: bool = False,
) -> FastAPI:
    catalog, jobs = Catalog(database), Jobs(database)
    submissions, queries = Submissions(catalog, inputs), Queries(database, outputs, signing_key)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        await catalog.seed_once(await asyncio.to_thread(seed))
        try:
            yield
        finally:
            if close_database:
                await database.close()

    app = FastAPI(
        title="mslb API",
        version="0.1.0",
        lifespan=lifespan,
        openapi_url="/api/v1/openapi.json",
        docs_url=None,
        redoc_url=None,
    )
    app.add_middleware(Guard, settings=settings)
    responses: dict[int | str, dict[str, object]] = {
        status: {"model": ErrorEnvelope} for status in (401, 403, 404, 409, 413, 422, 429, 500, 503)
    }
    router = APIRouter(
        prefix="/api/v1", dependencies=[Security(HTTPBearer(auto_error=False))], responses=responses
    )

    async def failure(request: Request, error: Exception) -> Response:
        identifier = str(getattr(request.state, "request_id", "unavailable"))
        if isinstance(error, (OperationError, StoreError, SimulationError)):
            return error_response(error.code, str(error), identifier)
        if isinstance(error, InputError):
            return error_response(
                "VALIDATION_ERROR",
                "Input validation failed",
                identifier,
                (FieldError(field=error.field[:256], reason=error.reason[:128]),),
            )
        if isinstance(error, (RequestValidationError, ValidationError)):
            details = tuple(
                FieldError(
                    field=".".join(str(part) for part in item["loc"])[:256],
                    reason=item["type"][:128],
                )
                for item in error.errors()[:32]
            )
            return error_response(
                "VALIDATION_ERROR", "Request validation failed", identifier, details
            )
        if isinstance(error, HTTPException):
            code = "NOT_FOUND" if error.status_code == 404 else "VALIDATION_ERROR"
            return error_response(
                code,
                "Route not found" if code == "NOT_FOUND" else "Malformed or unsupported request",
                identifier,
            )
        if isinstance(error, (MultiPartException, MultipartParseError)):
            return error_response("VALIDATION_ERROR", "Malformed multipart upload", identifier)
        if isinstance(error, (DBAPIError, OSError, TimeoutError)):
            return error_response(
                "RESOURCE_PRESSURE",
                "A required dependency or managed file is unavailable",
                identifier,
            )
        return error_response("INTERNAL_ERROR", "The operation could not be completed", identifier)

    for exception in (
        OperationError,
        StoreError,
        SimulationError,
        InputError,
        RequestValidationError,
        ValidationError,
        HTTPException,
        DBAPIError,
        OSError,
        TimeoutError,
        Exception,
        MultiPartException,
        MultipartParseError,
    ):
        app.add_exception_handler(exception, failure)

    @app.get("/api/v1/health/live", include_in_schema=False)
    async def live() -> Response:
        return Response('{"live":true}', media_type="application/json")

    @app.get("/api/v1/health/ready", include_in_schema=False)
    async def ready() -> Response:
        api_ready = await database.ready()
        return Response(
            canonical_bytes(
                dict(
                    ready=False,
                    api_ready=api_ready,
                    complete_product=False,
                    capabilities=dict(
                        logical_rest="ready" if api_ready else "unavailable",
                        mcp="not_implemented",
                        dashboard="not_implemented",
                        docker_traffic="not_implemented",
                        telemetry="not_implemented",
                        resource_guard="not_implemented",
                    ),
                )
            ),
            status_code=503,
            media_type="application/json",
        )

    async def server(server_id: str) -> ServerView:
        try:
            normalized = normalize_identifier(server_id)
        except ValueError:
            raise OperationError("VALIDATION_ERROR", "Invalid server identifier") from None
        for value in await catalog.list_servers():
            if value.id == normalized:
                return value
        raise OperationError("NOT_FOUND", "Server does not exist")

    @router.get("/servers", response_model=Page[ServerView])
    async def servers() -> Response:
        return json_response(Page[ServerView](items=await catalog.list_servers()))

    @router.post("/servers", status_code=201, response_model=ServerView)
    async def create_server(body: ServerInput) -> Response:
        value = await catalog.create_server(body)
        return json_response(value, 201, {"ETag": f'"{value.version}"'})

    @router.get("/servers/{server_id:path}", response_model=ServerView)
    async def get_server(server_id: str) -> Response:
        value = await server(server_id)
        return json_response(value, headers={"ETag": f'"{value.version}"'})

    @router.patch("/servers/{server_id:path}", response_model=ServerView)
    async def patch_server(server_id: str, body: ServerPatch, request: Request) -> Response:
        expected = version(request)
        previous = await server(server_id)
        values = previous.model_dump(exclude={"version"}) | body.model_dump(exclude_unset=True)
        value = await catalog.update_server(ServerInput.model_validate(values), expected)
        return json_response(value, headers={"ETag": f'"{value.version}"'})

    @router.delete("/servers/{server_id:path}", status_code=204)
    async def delete_server(server_id: str, request: Request) -> Response:
        expected = version(request)
        previous = await server(server_id)
        await catalog.delete_server(previous.id, expected)
        return Response(status_code=204)

    @router.post(
        "/datasets",
        status_code=201,
        response_model=DatasetView,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "multipart/form-data": {
                        "schema": {
                            "type": "object",
                            "required": ["file"],
                            "properties": {"file": {"type": "string", "format": "binary"}},
                            "additionalProperties": False,
                        }
                    }
                },
            }
        },
    )
    async def upload(request: Request) -> Response:
        if not request.headers.get("content-type", "").startswith("multipart/form-data;"):
            raise OperationError("VALIDATION_ERROR", "Upload one multipart CSV file named file")
        parser = _Multipart(
            request.headers, request.stream(), max_files=1, max_fields=0, max_part_size=10 * 1024**2
        )
        form = await parser.parse()
        try:
            if len(form.multi_items()) != 1 or not isinstance(form.get("file"), UploadFile):
                raise OperationError("VALIDATION_ERROR", "Upload exactly one CSV file named file")
            source = form["file"]
            if (
                not isinstance(source, UploadFile)
                or source.size is None
                or source.size > 10 * 1024**2
            ):
                raise OperationError("PAYLOAD_TOO_LARGE", "CSV upload exceeds 10 MiB")
            data = await submissions.upload(source.file, source.filename or "requests.csv")
            return json_response(await queries.dataset(data.id), 201)
        finally:
            await form.close()

    @router.get("/datasets", response_model=Page[DatasetView])
    async def datasets(limit: Limit = 100, cursor: Cursor = None) -> Response:
        return json_response(await queries.datasets(limit, cursor))

    @router.get("/datasets/{dataset_id}", response_model=DatasetView)
    async def dataset(dataset_id: UUID) -> Response:
        return json_response(await queries.dataset(dataset_id))

    @router.get("/strategies", response_model=Page[StrategyView])
    async def strategies() -> Response:
        values = [
            dict(id=name, display_name=title, description=description)
            for name, title, description in (
                ("first_fit", "First Fit", "Choose the first eligible server in ID order."),
                ("round_robin", "Round Robin", "Rotate through eligible servers in ID order."),
                (
                    "least_connections",
                    "Least Connections",
                    "Choose the eligible server with fewest active requests.",
                ),
                (
                    "most_available_memory",
                    "Most Available Memory",
                    "Choose the eligible server with most free memory.",
                ),
            )
        ]
        return json_response(
            Page[StrategyView](items=tuple(StrategyView.model_validate(value) for value in values))
        )

    # Declare collection comparison before the parameter routes.
    @router.post("/simulations/compare", response_model=ComparisonView)
    async def compare(body: CompareRequest) -> Response:
        return json_response(await queries.compare(body.run_ids))

    @router.post("/simulations", status_code=202, response_model=SimulationAccepted)
    async def submit(body: SimulationCreate, request: Request) -> Response:
        settings = RunSettings.model_validate(body.model_dump(exclude={"dataset_id"}))
        run = await submissions.submit(
            body.dataset_id, settings, request.headers.get("idempotency-key")
        )
        status = (await queries.run(run.id)).status
        return json_response(
            SimulationAccepted(run_id=run.id, status=status, snapshot_digest=run.snapshot_digest),
            202,
            {"Location": f"/api/v1/simulations/{run.id}"},
        )

    @router.get("/simulations", response_model=Page[RunView])
    async def history(limit: Limit = 100, cursor: Cursor = None) -> Response:
        return json_response(await queries.runs(limit, cursor))

    @router.get("/simulations/{run_id}", response_model=RunDetail)
    async def detail(run_id: UUID) -> Response:
        run = await queries.run(run_id)
        validation = (await queries.completed(run_id))[1] if run.status == "completed" else None
        value = RunDetail(
            **run.model_dump(),
            mode=run.configuration.mode,
            strategy=run.configuration.strategy,
            validation=validation,
        )
        return json_response(value)

    @router.post("/simulations/{run_id}/cancel", response_model=RunView)
    async def cancel(run_id: UUID) -> Response:
        await jobs.cancel(run_id)
        return json_response(await queries.run(run_id))

    @router.get("/simulations/{run_id}/events", response_model=Page[PersistedEvent])
    async def events(
        run_id: UUID,
        limit: Limit = 100,
        cursor: Cursor = None,
        start_tick: Tick = 0,
        end_tick: Tick = 1_000_000,
    ) -> Response:
        return json_response(await queries.events(run_id, limit, cursor, start_tick, end_tick))

    @router.get("/simulations/{run_id}/metrics", response_model=MetricsView | ChartView)
    async def metrics(
        run_id: UUID,
        metric: ChartMetric | None = None,
        start_tick: Tick = 0,
        end_tick: Tick = 1_000_000,
        max_points: Annotated[int, Query(ge=1, le=2000)] = 2000,
        server_id: Annotated[str | None, Query(max_length=128)] = None,
    ) -> Response:
        if metric is not None:
            return json_response(
                await queries.chart(run_id, metric, start_tick, end_tick, max_points, server_id)
            )
        return json_response(
            await queries.metrics(run_id, (server_id,) if server_id is not None else ())
        )

    @router.get("/simulations/{run_id}/metrics/servers", response_model=Page[ServerMetricView])
    async def server_metrics(run_id: UUID, limit: Limit = 100, cursor: Cursor = None) -> Response:
        return json_response(await queries.server_metrics(run_id, limit, cursor))

    @router.get(
        "/simulations/{run_id}/artifact",
        response_class=Response,
        responses={
            200: {
                "content": {
                    "application/x-ndjson": {"schema": {"type": "string"}},
                    "text/csv": {"schema": {"type": "string"}},
                    "application/json": {"schema": {"type": "object"}},
                }
            }
        },
    )
    async def artifact(run_id: UUID, name: ArtifactName = "run.jsonl") -> Response:
        return ArtifactResponse((await queries.download(run_id, name),))

    @router.get(
        "/simulations/{run_id}/inputs",
        response_class=Response,
        responses={
            200: {
                "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}}
            }
        },
    )
    async def run_inputs(run_id: UUID) -> Response:
        downloads = tuple(
            [
                await queries.download(run_id, name)
                for name in ("servers.json", "requests.csv", "snapshot.json", "manifest.json")
            ]
        )
        return ArtifactResponse(downloads, archive=True)

    @router.get(
        "/simulations/{run_id}/stream",
        response_class=Response,
        responses={200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}}},
    )
    async def stream(run_id: UUID, request: Request) -> Response:
        last = request.headers.get("last-event-id", "0")
        if not last.isascii() or not last.isdecimal() or len(last) > 5:
            raise OperationError("VALIDATION_ERROR", "Invalid Last-Event-ID")
        feed = EventFeed(database, run_id, int(last))
        initial = await queries.run(run_id)
        return EventResponse(
            feed.stream(initial),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    app.include_router(router)
    return app
