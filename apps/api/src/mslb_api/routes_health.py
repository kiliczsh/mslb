"""REST health routes over shared application services."""

from http import HTTPStatus
from uuid import UUID

from fastapi import FastAPI
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import CapacityProfile
from mslb_persistence.database import Database
from starlette.responses import Response


def register_health(
    *,
    app: FastAPI,
    database: Database,
    capacity_profile: CapacityProfile = "standard",
    isolation_id: UUID | None = None,
) -> None:
    """Register the health endpoints on the composed application router."""

    @app.get(path="/api/v1/health/live", include_in_schema=False)
    async def live() -> Response:
        """Report process liveness without requiring dependent services."""
        return Response(
            content='{"live":true}'
            if isolation_id is None
            else canonical_bytes(
                value={
                    "live": True,
                    "capacity_profile": capacity_profile,
                    "isolation_id": str(object=isolation_id),
                }
            ),
            media_type="application/json",
        )

    @app.get(path="/api/v1/health/ready", include_in_schema=False)
    async def ready() -> Response:
        """Report logical dependency health and honest incomplete-product readiness."""
        api_ready = await database.ready()
        return Response(
            content=canonical_bytes(
                value={
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
