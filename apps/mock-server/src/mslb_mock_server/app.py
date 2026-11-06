"""HTTP composition for one bounded private work backend."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from http import HTTPStatus

from fastapi import FastAPI, Request
from mslb_contracts.traffic import WorkRequest
from pydantic import ValidationError
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse

from mslb_mock_server.backend import Backend as Backend
from mslb_mock_server.errors import WorkStopped
from mslb_mock_server.observations import Measurements as Measurements
from mslb_mock_server.observations import _number as _number
from mslb_mock_server.observations import _observations as _observations
from mslb_mock_server.settings import Settings as Settings


def _error(code: str, status: int) -> JSONResponse:
    return JSONResponse({"error": {"code": code}}, status_code=status)


def create_app(settings: Settings, backend: Backend | None = None) -> FastAPI:
    service = backend or Backend(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(lifespan=lifespan, openapi_url=None, docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(service.health().model_dump())

    @app.post("/work")
    async def work(request: Request) -> JSONResponse:
        if request.headers.get("content-type", "").split(";")[0].lower() != "application/json":
            return _error("UNSUPPORTED_MEDIA_TYPE", HTTPStatus.UNSUPPORTED_MEDIA_TYPE.value)
        raw = bytearray()
        try:
            async with asyncio.timeout(2):
                async for chunk in request.stream():
                    if len(raw) + len(chunk) > 64 * 1024:
                        return _error("PAYLOAD_TOO_LARGE", HTTPStatus.CONTENT_TOO_LARGE.value)
                    raw.extend(chunk)
            payload = WorkRequest.model_validate_json(raw)
        except TimeoutError:
            return _error("BODY_TIMEOUT", HTTPStatus.REQUEST_TIMEOUT.value)
        except ClientDisconnect:
            return _error("CLIENT_DISCONNECTED", HTTPStatus.BAD_REQUEST.value)
        except ValidationError:
            return _error("VALIDATION_ERROR", HTTPStatus.UNPROCESSABLE_CONTENT.value)
        try:
            result = await service.run(payload)
        except WorkStopped as error:
            status = (
                HTTPStatus.TOO_MANY_REQUESTS.value
                if error.code == "BACKEND_BUSY"
                else HTTPStatus.SERVICE_UNAVAILABLE.value
                if error.code == "RESOURCE_PRESSURE"
                else HTTPStatus.GATEWAY_TIMEOUT.value
            )
            return _error(error.code, status)
        return JSONResponse(result.model_dump())

    return app
