"""Public operational routes reuse shared services and verified streaming responses."""

from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Request
from mslb_application.docker_services import DockerServices
from mslb_contracts.docker_artifacts import HttpRecord
from mslb_contracts.docker_views import (
    DockerArtifactName,
    DockerRunDetail,
    DockerRunView,
    DockerSimulationCreate,
)
from mslb_contracts.http import SimulationAccepted
from mslb_contracts.pagination import Page
from starlette.responses import Response

from mslb_api.responses import json_response
from mslb_api.route_values import Cursor, Limit
from mslb_api.streams import ArtifactResponse


def register_docker(*, router: APIRouter, services: DockerServices) -> None:
    """Expose bounded operational work without privileged daemon access in the API."""

    @router.post(
        path="/docker/simulations",
        status_code=HTTPStatus.ACCEPTED.value,
        response_model=SimulationAccepted,
    )
    async def submit_docker(body: DockerSimulationCreate, request: Request) -> Response:
        """Queue real HTTP work using only server-enrolled backend identities."""
        accepted = await services.submit(body=body, key=request.headers.get("idempotency-key"))
        return json_response(
            model=accepted,
            status=HTTPStatus.ACCEPTED.value,
            headers={"Location": f"/api/v1/docker/simulations/{accepted.run_id}"},
        )

    @router.get(path="/docker/simulations", response_model=Page[DockerRunView])
    async def docker_history(limit: Limit = 100, cursor: Cursor = None) -> Response:
        """Page operational history separately from deterministic logical comparison."""
        return json_response(model=await services.history(limit=limit, cursor=cursor))

    @router.get(path="/docker/simulations/{run_id}", response_model=DockerRunDetail)
    async def docker_detail(run_id: UUID) -> Response:
        """Read frozen settings, durable lifecycle, explicit pressure and verified results."""
        return json_response(model=await services.detail(run_id=run_id))

    @router.post(path="/docker/simulations/{run_id}/cancel", response_model=DockerRunDetail)
    async def cancel_docker(run_id: UUID) -> Response:
        """Request cancellation while retaining ownership until cleanup resolves."""
        return json_response(model=await services.cancel(run_id=run_id))

    @router.get(path="/docker/simulations/{run_id}/events", response_model=Page[HttpRecord])
    async def docker_events(run_id: UUID, limit: Limit = 100, cursor: Cursor = None) -> Response:
        """Page verified completed HTTP observations in real-time sequence order."""
        return json_response(model=await services.events(run_id=run_id, limit=limit, cursor=cursor))

    @router.get(
        path="/docker/simulations/{run_id}/artifact",
        response_class=Response,
        responses={
            HTTPStatus.OK.value: {
                "content": {
                    "application/x-ndjson": {"schema": {"type": "string"}},
                    "application/json": {"schema": {"type": "object"}},
                    "text/csv": {"schema": {"type": "string"}},
                }
            }
        },
    )
    async def docker_artifact(
        run_id: UUID, name: DockerArtifactName = "http_events.jsonl"
    ) -> Response:
        """Stream only a typed completed artifact after independent scope/hash verification."""
        return ArtifactResponse(downloads=(await services.download(run_id=run_id, name=name),))

    @router.get(
        path="/docker/simulations/{run_id}/inputs",
        response_class=Response,
        responses={
            HTTPStatus.OK.value: {
                "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}}
            }
        },
    )
    async def docker_package(run_id: UUID) -> Response:
        """Stream matching frozen inputs/configuration and all three operational outputs."""
        downloads = await services.downloads(run_id=run_id)
        return ArtifactResponse(downloads=downloads, archive=True)
