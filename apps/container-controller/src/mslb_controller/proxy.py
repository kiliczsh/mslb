"""Authenticated semantic control API; never forwards caller daemon requests."""

import asyncio
import secrets
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import asynccontextmanager
from http import HTTPStatus
from typing import Literal
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from mslb_persistence.database import Database
from mslb_persistence.docker_records import ResourceRecord
from mslb_persistence.errors import StoreError
from pydantic import BaseModel, ValidationError
from sqlalchemy.exc import SQLAlchemyError
from starlette.exceptions import HTTPException
from starlette.requests import ClientDisconnect

from mslb_controller.backend_http import BackendHttp
from mslb_controller.engine import Engine
from mslb_controller.errors import EngineError, TrafficError
from mslb_controller.errors import Refusal as Refusal
from mslb_controller.lifecycle import Lifecycle
from mslb_controller.proxy_models import Empty as Empty
from mslb_controller.proxy_models import LeaseRef as LeaseRef
from mslb_controller.resource_probe import ProbeEngine


def reply(*, record: ResourceRecord, running: object = None) -> JSONResponse:
    return JSONResponse(
        content={
            "id": str(object=record.id),
            "state": record.state,
            "name": record.name,
            "container_id": record.container_id,
            "running": running,
        }
    )


async def body[T: BaseModel](*, request: Request, model: type[T]) -> T:
    if request.headers.get("content-type", "").split(sep=";")[0] != "application/json":
        raise Refusal(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE.value, code="JSON_REQUIRED")
    if request.headers.get("content-encoding", "identity") != "identity":
        raise Refusal(status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE.value, code="ENCODING_REJECTED")
    raw = bytearray()
    try:
        async with asyncio.timeout(delay=2):
            async for chunk in request.stream():
                if len(raw) + len(chunk) > 4096:
                    raise Refusal(status=HTTPStatus.CONTENT_TOO_LARGE.value, code="BODY_LIMIT")
                raw.extend(chunk)
    except (TimeoutError, ClientDisconnect):
        raise Refusal(status=HTTPStatus.REQUEST_TIMEOUT.value, code="BODY_TIMEOUT") from None
    try:
        return model.model_validate_json(json_data=raw)
    except ValidationError:
        raise Refusal(
            status=HTTPStatus.UNPROCESSABLE_CONTENT.value, code="INVALID_CONTROL"
        ) from None


def create_app(
    *,
    database: Database,
    token: str,
    engine: Engine | None = None,
    guard_token: str | None = None,
    probe_engine: ProbeEngine | None = None,
) -> FastAPI:
    """Compose the authenticated semantic proxy without exposing general daemon requests."""
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

    @app.middleware(middleware_type="http")
    async def admission(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        """Enforce private host/origin/authentication and bounded request admission."""
        nonlocal active
        if request.headers.get("host") not in {"docker-proxy:8081", "127.0.0.1:8081"}:
            return JSONResponse(
                content={"error": "HOST_REJECTED"}, status_code=HTTPStatus.BAD_REQUEST.value
            )
        if "origin" in request.headers or request.url.query:
            return JSONResponse(
                content={"error": "ORIGIN_OR_QUERY_REJECTED"},
                status_code=HTTPStatus.FORBIDDEN.value,
            )
        if request.url.path == "/health" and request.method == "GET":
            return JSONResponse(content={"ready": True})
        supplied = request.headers.get("authorization", "")
        expected = (
            guard_token if request.url.path == "/guard/probe" and request.method == "GET" else token
        )
        if (
            expected is None
            or len(supplied) != 71
            or not secrets.compare_digest(supplied.encode(), ("Bearer " + expected).encode())
        ):
            return JSONResponse(
                content={"error": "UNAUTHORIZED"}, status_code=HTTPStatus.UNAUTHORIZED.value
            )
        if active >= 8:
            return JSONResponse(
                content={"error": "CONTROL_BUSY"}, status_code=HTTPStatus.TOO_MANY_REQUESTS.value
            )
        active += 1
        try:
            return await call_next(request)
        finally:
            active -= 1

    @app.exception_handler(exc_class_or_status_code=Refusal)
    async def refusal(_request: Request, error: Refusal) -> JSONResponse:
        return JSONResponse(content={"error": error.code}, status_code=error.status)

    @app.exception_handler(exc_class_or_status_code=RequestValidationError)
    async def invalid(_request: Request, _error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            content={"error": "INVALID_CONTROL"}, status_code=HTTPStatus.UNPROCESSABLE_CONTENT.value
        )

    @app.exception_handler(exc_class_or_status_code=HTTPException)
    async def missing(_request: Request, _error: HTTPException) -> JSONResponse:
        return JSONResponse(
            content={"error": "CONTROL_NOT_FOUND"}, status_code=HTTPStatus.NOT_FOUND.value
        )

    async def execute(
        *, operation: Callable[[], Awaitable[JSONResponse]], readonly: bool = False
    ) -> JSONResponse:
        # The single process keeps a sent mutation alive even if its HTTP caller disconnects.
        # No waiter queue: the slot is held until the task settles, including error receipts.
        """Retain a sent control action until bounded settlement despite caller cancellation."""
        owning = probing if readonly else pending
        if owning:
            raise Refusal(status=HTTPStatus.TOO_MANY_REQUESTS.value, code="CONTROL_BUSY")

        async def bounded() -> JSONResponse:
            """Translate safe control failures while retaining the owned operation slot."""
            try:
                async with asyncio.timeout(delay=45):
                    return await operation()
            except StoreError as error:
                status = (
                    HTTPStatus.NOT_FOUND.value
                    if error.code == "NOT_FOUND"
                    else HTTPStatus.TOO_MANY_REQUESTS.value
                    if error.code == "RATE_LIMITED"
                    else HTTPStatus.CONFLICT.value
                )
                return JSONResponse(content={"error": error.code}, status_code=status)
            except (EngineError, TrafficError) as error:
                status = (
                    HTTPStatus.CONFLICT.value
                    if error.code in {"OWNERSHIP_CONFLICT", "INVALID_STATE"}
                    else HTTPStatus.BAD_GATEWAY.value
                )
                return JSONResponse(content={"error": error.code}, status_code=status)
            except (SQLAlchemyError, TimeoutError):
                return JSONResponse(
                    content={"error": "CONTROL_UNAVAILABLE"},
                    status_code=HTTPStatus.SERVICE_UNAVAILABLE.value,
                )

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

    @app.post(path="/control/enroll")
    async def enroll(request: Request) -> JSONResponse:
        await body(request=request, model=Empty)

        async def operation() -> JSONResponse:
            value = await lifecycle.enroll()
            return JSONResponse(
                content={
                    "id": str(object=value.id),
                    "definition": value.definition.model_dump(mode="json"),
                }
            )

        return await execute(operation=operation)

    @app.post(path="/control/resources/{identifier}/{action}")
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
