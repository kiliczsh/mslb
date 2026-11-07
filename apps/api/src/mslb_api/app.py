"""Logical REST operations adapt shared services without executing simulations."""

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from http import HTTPStatus
from pathlib import Path

from fastapi import APIRouter, FastAPI, Security
from fastapi.exceptions import RequestValidationError
from fastapi.security import HTTPBearer
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.queries import Queries
from mslb_application.services import Services
from mslb_application.submissions import Submissions
from mslb_contracts.http import (
    ErrorEnvelope,
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
from starlette.routing import Route

from mslb_api.docs_ui import register_documentation
from mslb_api.documentation import configure_routes
from mslb_api.exception_handler import failure
from mslb_api.guard import Guard
from mslb_api.mcp_adapter import McpAdapter
from mslb_api.multipart import _Multipart as _Multipart
from mslb_api.responses import json_response as json_response
from mslb_api.route_values import Cursor as Cursor
from mslb_api.route_values import Limit as Limit
from mslb_api.route_values import Tick as Tick
from mslb_api.routes_datasets import register_datasets
from mslb_api.routes_docker import register_docker
from mslb_api.routes_health import register_health
from mslb_api.routes_history import register_history
from mslb_api.routes_results import register_results
from mslb_api.routes_servers import register_servers
from mslb_api.routes_servers import version as version
from mslb_api.routes_simulations import register_simulations
from mslb_api.settings import Settings


def seed() -> tuple[ServerInput, ...]:
    """Read the unchanged initial server fixture for one-time catalog seeding."""
    with Path("/fixtures/core/servers.json").open(mode="rb") as stream:
        return read_servers(stream=stream)


def create_app(
    *,
    settings: Settings,
    database: Database,
    inputs: ManagedFiles,
    outputs: ManagedFiles,
    signing_key: bytes,
    close_database: bool = False,
) -> FastAPI:
    """Compose shared services, access/error boundaries, REST routes and MCP lifespan."""
    if not database.capacity_profile == settings.capacity_profile == inputs.limits.capacity_profile:
        raise OperationError(
            code="VALIDATION_ERROR", message="Installation capacity profiles differ"
        )
    catalog, jobs = Catalog(database=database), Jobs(database=database)
    submissions, queries = (
        Submissions(catalog=catalog, files=inputs),
        Queries(database=database, outputs=outputs, cursor_secret=signing_key),
    )
    services = Services(catalog=catalog, jobs=jobs, submissions=submissions, queries=queries)
    mcp = McpAdapter(services=services, settings=settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
        await catalog.seed_once(servers=await asyncio.to_thread(seed))
        try:
            async with mcp.server.session_manager.run():
                yield
        finally:
            if close_database:
                await database.close()

    app = FastAPI(
        title="mslb API",
        version="0.1.0",
        description=(
            "Deterministic logical simulations and guarded real Docker HTTP traffic. "
            "Server edits affect future snapshots. Upload CSV inputs, queue durable work, "
            "then read committed progress and verified result artifacts. "
            "Sequential execution without scaling supports the supplied evaluator; "
            "concurrent and scaled results use independent replay. "
            "The normal installation accepts up to 10,000 requests and 10 MiB CSV uploads."
        ),
        lifespan=lifespan,
        openapi_url="/api/v1/openapi.json",
        docs_url=None,
        redoc_url=None,
    )
    app.add_middleware(middleware_class=Guard, settings=settings)
    register_documentation(app=app)
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
        prefix="/api/v1",
        dependencies=[Security(dependency=HTTPBearer(auto_error=False))],
        responses=responses,
    )

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
        app.add_exception_handler(exc_class_or_status_code=exception, handler=failure)

    register_health(
        app=app,
        database=database,
        capacity_profile=settings.capacity_profile,
        isolation_id=settings.isolation_id,
    )
    register_servers(router=router, services=services)
    register_datasets(router=router, submissions=submissions, queries=queries)
    register_history(router=router, queries=queries)
    register_simulations(router=router, services=services, queries=queries)
    register_docker(router=router, services=services.docker)
    register_results(router=router, queries=queries, database=database)
    configure_routes(router=router, auth_enabled=settings.auth_enabled)
    app.include_router(router=router)
    app.router.routes.append(Route(path="/mcp", endpoint=mcp.app))
    return app


def get_mcp(app: FastAPI) -> McpAdapter:
    """Return the installed MCP adapter or reject an incomplete application composition."""
    value = app.state.mcp
    if not isinstance(value, McpAdapter):
        raise OperationError(code="INVALID_STATE", message="MCP adapter is not installed")
    return value
