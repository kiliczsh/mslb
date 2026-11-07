"""Strict reply DTOs for the private controller RPC client."""

from uuid import UUID

from mslb_contracts.docker_base import DockerId
from mslb_contracts.docker_inventory import DeploymentSpec
from pydantic import BaseModel, ConfigDict


class ResourceReply(BaseModel):
    """Decode a bounded semantic resource response from the private controller."""

    model_config = ConfigDict(strict=True, extra="forbid")
    id: UUID
    state: str
    name: str
    container_id: DockerId | None
    running: bool | None


class EnrollmentReply(BaseModel):
    """Return the controller-verified deployment identity and enrollment result."""

    model_config = ConfigDict(strict=True, extra="forbid")
    id: UUID
    definition: DeploymentSpec
