"""REST simulations routes over shared application services."""

from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Request
from mslb_application.queries import Queries
from mslb_application.services import Services
from mslb_contracts.http import (
    CompareRequest,
    RunDetail,
    SimulationAccepted,
    SimulationCreate,
    StrategyView,
)
from mslb_contracts.metric_views import ComparisonView
from mslb_contracts.pagination import Page
from mslb_contracts.run_views import RunView
from starlette.responses import Response

from mslb_api.responses import json_response
from mslb_api.route_values import Cursor, Limit


def register_simulations(*, router: APIRouter, services: Services, queries: Queries) -> None:
    """Register the simulations endpoints on the composed application router."""

    @router.get("/strategies", response_model=Page[StrategyView])
    async def strategies() -> Response:
        """List the same four deterministic strategies used by REST and MCP."""
        return json_response(services.strategies())

    @router.post("/simulations/compare", response_model=ComparisonView)
    async def compare(body: CompareRequest) -> Response:
        """Compare completed runs only when their frozen workloads are compatible."""
        return json_response(await queries.compare(body.run_ids))

    @router.post(
        "/simulations", status_code=HTTPStatus.ACCEPTED.value, response_model=SimulationAccepted
    )
    async def submit(body: SimulationCreate, request: Request) -> Response:
        """Durably queue immutable logical work and support idempotent retries."""
        run = await services.submit(body, request.headers.get("idempotency-key"))
        return json_response(
            run,
            HTTPStatus.ACCEPTED.value,
            {"Location": f"/api/v1/simulations/{run.run_id}"},
        )

    @router.get("/simulations", response_model=Page[RunView])
    async def history(limit: Limit = 100, cursor: Cursor = None) -> Response:
        """Page saved run history in stable creation-time and ID order."""
        return json_response(await queries.runs(limit, cursor))

    @router.get("/simulations/{run_id}", response_model=RunDetail)
    async def detail(run_id: UUID) -> Response:
        """Read frozen policy, durable progress and published validation results."""
        return json_response(await services.detail(run_id))

    @router.post("/simulations/{run_id}/cancel", response_model=RunView)
    async def cancel(run_id: UUID) -> Response:
        """Request durable cancellation with priority over later completion."""
        return json_response(await services.cancel(run_id))
