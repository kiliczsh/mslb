"""Trusted deployment inventory and immutable Docker run snapshots."""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from mslb_contracts.docker_base import DockerId, DockerModel, ImageId
from mslb_contracts.docker_settings import DockerSettings
from mslb_contracts.inputs import PositiveValue
from mslb_contracts.runs import Digest


class StaticBackend(DockerModel):
    name: Literal["backend-a", "backend-b"]
    container_id: DockerId
    logical_admission_mem_mb: PositiveValue = 1024
    max_inflight: Annotated[int, Field(ge=4, le=4)] = 4


class DeploymentSpec(DockerModel):
    """Server-enrolled inspected identity, not a public run-creation DTO."""

    project: Literal["mslb"] = "mslb"
    image_id: ImageId
    network_id: DockerId
    static: Annotated[tuple[StaticBackend, ...], Field(min_length=2, max_length=2)]

    @model_validator(mode="after")
    def members(self) -> Self:
        if (
            tuple(item.name for item in self.static) != ("backend-a", "backend-b")
            or len({item.container_id for item in self.static}) != 2
        ):
            raise ValueError("Static inventory must contain the two distinct ordered backends")
        return self


class DockerSnapshot(DockerModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    controller_version: Literal["0.1.0"] = "0.1.0"
    dataset_id: UUID
    dataset_digest: Digest
    request_count: Annotated[int, Field(ge=1, le=10_000)]
    latest_arrival: Annotated[int, Field(ge=0, le=119)]
    deployment_id: UUID
    deployment_digest: Digest
    configuration: DockerSettings

    @model_validator(mode="after")
    def arrival(self) -> Self:
        if self.latest_arrival >= self.configuration.duration_seconds:
            raise ValueError("Dataset arrivals exceed the real-time run duration")
        return self
