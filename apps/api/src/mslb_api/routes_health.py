"""REST health routes over shared application services."""

from http import HTTPStatus
from uuid import UUID

from fastapi import FastAPI
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import CapacityProfile
from mslb_persistence.database import Database
from mslb_persistence.resource_evidence import ResourceEvidenceStore
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
        report = (
            await ResourceEvidenceStore(database=database).read()
            if api_ready and database.enforce_resource_guard
            else None
        )
        return Response(
            content=canonical_bytes(
                value={
                    "ready": False,
                    "api_ready": api_ready,
                    "complete_product": False,
                    "resource_admission": report.decision.model_dump(mode="json")
                    if report
                    else None,
                    "capabilities": {
                        "logical_rest": "ready" if api_ready else "unavailable",
                        "mcp": "ready" if api_ready else "unavailable",
                        "dashboard": "incomplete",
                        "docker_traffic": "incomplete",
                        "telemetry": "deferred",
                        "resource_guard": "ready"
                        if report and report.decision.admit
                        else "unavailable"
                        if database.enforce_resource_guard
                        else "incomplete",
                    },
                }
            ),
            status_code=HTTPStatus.SERVICE_UNAVAILABLE.value,
            media_type="application/json",
        )
