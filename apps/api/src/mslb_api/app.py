"""Logical REST operations adapt shared services without executing simulations."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, FastAPI, Request, Security
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPBearer
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.queries import Queries
from mslb_application.services import Services
from mslb_application.submissions import Submissions
from mslb_contracts.http import (
    ErrorEnvelope,
    FieldError,
)
from mslb_contracts.inputs import InputError, ServerInput, read_servers
from mslb_engine.errors import SimulationError
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database
from mslb_persistence.errors import StoreError
from mslb_persistence.jobs import Jobs
from pydantic import ValidationError
from python_multipart.exceptions import MultipartParseError
from sqlalchemy.exc import DBAPIError
from starlette.exceptions import HTTPException
from starlette.formparsers import MultiPartException
from starlette.responses import Response
from starlette.routing import Route

from mslb_api.errors import error_response
from mslb_api.guard import Guard
from mslb_api.mcp_adapter import McpAdapter
from mslb_api.multipart import _Multipart as _Multipart
from mslb_api.responses import json_response as json_response
from mslb_api.route_values import Cursor as Cursor
from mslb_api.route_values import Limit as Limit
from mslb_api.route_values import Tick as Tick
from mslb_api.routes_datasets import register_datasets
from mslb_api.routes_health import register_health
from mslb_api.routes_results import register_results
from mslb_api.routes_servers import register_servers
from mslb_api.routes_servers import version as version
from mslb_api.routes_simulations import register_simulations
from mslb_api.settings import Settings


def seed() -> tuple[ServerInput, ...]:
    with Path("/fixtures/core/servers.json").open("rb") as stream:
        return read_servers(stream)


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
    services = Services(catalog, jobs, submissions, queries)
    mcp = McpAdapter(services=services, settings=settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        await catalog.seed_once(await asyncio.to_thread(seed))
        try:
            async with mcp.server.session_manager.run():
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
    app.state.mcp = mcp
    responses: dict[int | str, dict[str, object]] = {
        status: {"model": ErrorEnvelope}
        for status in (
            HTTPStatus.UNAUTHORIZED.value,
            HTTPStatus.FORBIDDEN.value,
            HTTPStatus.NOT_FOUND.value,
            HTTPStatus.CONFLICT.value,
            HTTPStatus.CONTENT_TOO_LARGE.value,
            HTTPStatus.UNPROCESSABLE_CONTENT.value,
            HTTPStatus.TOO_MANY_REQUESTS.value,
            HTTPStatus.INTERNAL_SERVER_ERROR.value,
            HTTPStatus.SERVICE_UNAVAILABLE.value,
        )
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
            code = (
                "NOT_FOUND"
                if error.status_code == HTTPStatus.NOT_FOUND.value
                else "VALIDATION_ERROR"
            )
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

    register_health(app=app, database=database)
    register_servers(router=router, services=services)
    register_datasets(router=router, submissions=submissions, queries=queries)
    register_simulations(router=router, services=services, queries=queries)
    register_results(router=router, queries=queries, database=database)
    app.include_router(router)
    app.router.routes.append(Route("/mcp", endpoint=mcp.app))
    return app


def get_mcp(app: FastAPI) -> McpAdapter:
    value = app.state.mcp
    if not isinstance(value, McpAdapter):
        raise OperationError("INVALID_STATE", "MCP adapter is not installed")
    return value
