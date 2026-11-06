"""Strict reply DTOs for the private controller RPC client."""

from uuid import UUID

from mslb_contracts.docker_runs import DeploymentSpec, DockerId
from pydantic import BaseModel, ConfigDict


class ResourceReply(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    id: UUID
    state: str
    name: str
    container_id: DockerId | None
    running: bool | None


class EnrollmentReply(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")
    id: UUID
    definition: DeploymentSpec
