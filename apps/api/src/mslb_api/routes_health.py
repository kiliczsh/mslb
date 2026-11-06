"""REST health routes over shared application services."""

from http import HTTPStatus

from fastapi import FastAPI
from mslb_contracts.canonical import canonical_bytes
from mslb_persistence.database import Database
from starlette.responses import Response


def register_health(*, app: FastAPI, database: Database) -> None:
    """Register the health endpoints on the composed application router."""

    @app.get("/api/v1/health/live", include_in_schema=False)
    async def live() -> Response:
        return Response('{"live":true}', media_type="application/json")

    @app.get("/api/v1/health/ready", include_in_schema=False)
    async def ready() -> Response:
        api_ready = await database.ready()
        return Response(
            canonical_bytes(
                {
                    "ready": False,
                    "api_ready": api_ready,
                    "complete_product": False,
                    "capabilities": {
                        "logical_rest": "ready" if api_ready else "unavailable",
                        "mcp": "ready",
                        "dashboard": "incomplete",
                        "docker_traffic": "not_implemented",
                        "telemetry": "not_implemented",
                        "resource_guard": "not_implemented",
                    },
                }
            ),
            status_code=HTTPStatus.SERVICE_UNAVAILABLE.value,
            media_type="application/json",
        )
