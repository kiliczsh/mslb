"""Authenticated semantic control API; never forwards caller daemon requests."""

import asyncio
import secrets
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from mslb_persistence.database import Database, StoreError
from mslb_persistence.docker_records import ControlLease, ResourceRecord
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from mslb_controller.engine import Engine, EngineError
from mslb_controller.lifecycle import Lifecycle
from mslb_controller.resource_probe import ProbeEngine
from mslb_controller.routing import BackendHttp, TrafficError


class LeaseRef(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)
    run_id: UUID
    fence: Annotated[int, Field(ge=1, le=9223372036854775807)]
    owner: Annotated[str, Field(min_length=1, max_length=128)]
    phase: Literal["running", "cleanup"]

    def lease(self) -> ControlLease:
        # Expiry is read and enforced in PostgreSQL, never accepted from the caller.
        return ControlLease(
            run_id=self.run_id,
            fence=self.fence,
            owner=self.owner,
            expires_at=datetime.min.replace(tzinfo=UTC),
            phase=self.phase,
        )


class Empty(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")


def reply(*, record: ResourceRecord, running: object = None) -> JSONResponse:
    return JSONResponse(
        {
            "id": str(record.id),
            "state": record.state,
            "name": record.name,
            "container_id": record.container_id,
            "running": running,
        }
    )


class Refusal(Exception):
    def __init__(self, status: int, code: str) -> None:
        self.status, self.code = status, code
        super().__init__(code)


async def body[T: BaseModel](*, request: Request, model: type[T]) -> T:
    if request.headers.get("content-type", "").split(";")[0] != "application/json":
        raise Refusal(415, "JSON_REQUIRED")
    if request.headers.get("content-encoding", "identity") != "identity":
        raise Refusal(415, "ENCODING_REJECTED")
    raw = bytearray()
    try:
        async with asyncio.timeout(2):
            async for chunk in request.stream():
                if len(raw) + len(chunk) > 4096:
                    raise Refusal(413, "BODY_LIMIT")
                raw.extend(chunk)
    except (TimeoutError, ClientDisconnect):
        raise Refusal(408, "BODY_TIMEOUT") from None
    try:
        return model.model_validate_json(raw)
    except ValidationError:
        raise Refusal(422, "INVALID_CONTROL") from None


def create_app(
    *,
    database: Database,
    token: str,
    engine: Engine | None = None,
    guard_token: str | None = None,
    probe_engine: ProbeEngine | None = None,
) -> FastAPI:
    daemon, http = engine or Engine(), BackendHttp()
    inspector = probe_engine or ProbeEngine()
    lifecycle = Lifecycle(database=database, engine=daemon, http=http)
    active = 0
    pending: set[asyncio.Task[JSONResponse]] = set()
    probing: set[asyncio.Task[JSONResponse]] = set()

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        try:
            yield
        finally:
            if pending or probing:
                await asyncio.gather(*pending, *probing, return_exceptions=True)
            await daemon.close()
            await inspector.close()
            await http.client.aclose()
            await database.close()

    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.middleware("http")
    async def admission(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        nonlocal active
        if request.headers.get("host") not in {"docker-proxy:8081", "127.0.0.1:8081"}:
            return JSONResponse({"error": "HOST_REJECTED"}, status_code=400)
        if "origin" in request.headers or request.url.query:
            return JSONResponse({"error": "ORIGIN_OR_QUERY_REJECTED"}, status_code=403)
        if request.url.path == "/health" and request.method == "GET":
            return JSONResponse({"ready": True})
        supplied = request.headers.get("authorization", "")
        expected = (
            guard_token if request.url.path == "/guard/probe" and request.method == "GET" else token
        )
        if (
            expected is None
            or len(supplied) != 71
            or not secrets.compare_digest(supplied.encode(), ("Bearer " + expected).encode())
        ):
            return JSONResponse({"error": "UNAUTHORIZED"}, status_code=401)
        if active >= 8:
            return JSONResponse({"error": "CONTROL_BUSY"}, status_code=429)
        active += 1
        try:
            return await call_next(request)
        finally:
            active -= 1

    @app.exception_handler(Refusal)
    async def refusal(_request: Request, error: Refusal) -> JSONResponse:
        return JSONResponse({"error": error.code}, status_code=error.status)

    @app.exception_handler(RequestValidationError)
    async def invalid(_request: Request, _error: RequestValidationError) -> JSONResponse:
        return JSONResponse({"error": "INVALID_CONTROL"}, status_code=422)

    @app.exception_handler(HTTPException)
    async def missing(_request: Request, _error: HTTPException) -> JSONResponse:
        return JSONResponse({"error": "CONTROL_NOT_FOUND"}, status_code=404)

    async def execute(
        *, operation: Callable[[], Awaitable[JSONResponse]], readonly: bool = False
    ) -> JSONResponse:
        # The single process keeps a sent mutation alive even if its HTTP caller disconnects.
        # No waiter queue: the slot is held until the task settles, including error receipts.
        owning = probing if readonly else pending
        if owning:
            raise Refusal(429, "CONTROL_BUSY")

        async def bounded() -> JSONResponse:
            try:
                async with asyncio.timeout(delay=45):
                    return await operation()
            except StoreError as error:
                status = (
                    404
                    if error.code == "NOT_FOUND"
                    else 429
                    if error.code == "RATE_LIMITED"
                    else 409
                )
                return JSONResponse({"error": error.code}, status_code=status)
            except (EngineError, TrafficError) as error:
                status = 409 if error.code in {"OWNERSHIP_CONFLICT", "INVALID_STATE"} else 502
                return JSONResponse({"error": error.code}, status_code=status)
            except (SQLAlchemyError, TimeoutError):
                return JSONResponse({"error": "CONTROL_UNAVAILABLE"}, status_code=503)

        task = asyncio.create_task(coro=bounded())
        owning.add(task)
        task.add_done_callback(owning.discard)
        return await asyncio.shield(arg=task)

    @app.get(path="/guard/probe")
    async def resource_probe() -> JSONResponse:
        async def operation() -> JSONResponse:
            probe = await inspector.probe()
            return JSONResponse(content=probe.model_dump(mode="json"))

        return await execute(operation=operation, readonly=True)

    @app.post("/control/enroll")
    async def enroll(request: Request) -> JSONResponse:
        await body(request=request, model=Empty)

        async def operation() -> JSONResponse:
            value = await lifecycle.enroll()
            return JSONResponse(
                {"id": str(value.id), "definition": value.definition.model_dump(mode="json")}
            )

        return await execute(operation=operation)

    @app.post("/control/resources/{identifier}/{action}")
    async def resource(
        request: Request, identifier: UUID, action: Literal["create", "inspect", "start", "cleanup"]
    ) -> JSONResponse:
        lease = (await body(request=request, model=LeaseRef)).lease()

        async def operation() -> JSONResponse:
            if action == "inspect":
                record, info = await lifecycle.observe(lease=lease, identifier=identifier)
                return reply(record=record, running=info.State.get("Running"))
            method = {
                "create": lifecycle.create,
                "start": lifecycle.start,
                "cleanup": lifecycle.cleanup,
            }[action]
            return reply(record=await method(lease=lease, identifier=identifier))

        return await execute(operation=operation)

    return app
