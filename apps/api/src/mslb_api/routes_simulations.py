"""REST simulations routes over shared application services."""

from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Request
from mslb_application.queries import Queries
from mslb_application.services import Services
from mslb_contracts.docker_views import DockerSimulationCreate, SimulationRequest
from mslb_contracts.http import (
    CompareRequest,
    RunDetail,
    SimulationAccepted,
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

    @router.get(path="/strategies", response_model=Page[StrategyView])
    async def strategies() -> Response:
        """List the same four deterministic strategies used by REST and MCP."""
        return json_response(model=services.strategies())

    @router.post(path="/simulations/compare", response_model=ComparisonView)
    async def compare(body: CompareRequest) -> Response:
        """Compare completed runs only when their frozen workloads are compatible."""
        return json_response(model=await queries.compare(run_ids=body.run_ids))

    @router.post(
        path="/simulations",
        status_code=HTTPStatus.ACCEPTED.value,
        response_model=SimulationAccepted,
    )
    async def submit(body: SimulationRequest, request: Request) -> Response:
        """Queue a frozen logical or operational variant with idempotent retries."""
        operational = isinstance(body, DockerSimulationCreate)
        run = (
            await services.docker.submit(body=body, key=request.headers.get("idempotency-key"))
            if isinstance(body, DockerSimulationCreate)
            else await services.submit(body=body, key=request.headers.get("idempotency-key"))
        )
        return json_response(
            model=run,
            status=HTTPStatus.ACCEPTED.value,
            headers={
                "Location": f"/api/v1/{'docker/' if operational else ''}simulations/{run.run_id}"
            },
        )

    @router.get(path="/simulations", response_model=Page[RunView])
    async def history(limit: Limit = 100, cursor: Cursor = None) -> Response:
        """Page saved run history in stable creation-time and ID order."""
        return json_response(model=await queries.runs(limit=limit, cursor=cursor))

    @router.get(path="/simulations/{run_id}", response_model=RunDetail)
    async def detail(run_id: UUID) -> Response:
        """Read frozen policy, durable progress and published validation results."""
        return json_response(model=await services.detail(run_id=run_id))

    @router.post(path="/simulations/{run_id}/cancel", response_model=RunView)
    async def cancel(run_id: UUID) -> Response:
        """Request durable cancellation with priority over later completion."""
        return json_response(model=await services.cancel(run_id=run_id))
