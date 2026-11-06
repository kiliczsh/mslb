"""Scoped physical Docker artifact records; no deterministic replay compatibility claim."""

from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, field_validator, model_validator

from mslb_contracts.docker_runs import CreationIntent, DockerModel, StaticReservation
from mslb_contracts.docker_runtime import HttpEvent, TrafficSummary
from mslb_contracts.runs import Digest

type ResourceState = Literal[
    "prepared",
    "creating",
    "observed",
    "starting",
    "started",
    "draining",
    "removed",
    "released",
    "abandoned",
    "unresolved",
    "conflict",
]


class ScopeRecord(DockerModel):
    run_id: UUID
    fence: Annotated[int, Field(ge=1)]
    snapshot_digest: Digest
    observed_at: AwareDatetime

    @field_validator("observed_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError("Artifact timestamps must use UTC")
        return value


class HttpRecord(HttpEvent, ScopeRecord):
    """Bind an actual HTTP observation to its run/fence/snapshot and UTC observation time."""


class LifecycleRecord(ScopeRecord):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    mode: Literal["docker"] = "docker"
    sequence: Annotated[int, Field(ge=1, le=1024)]
    resource_id: UUID
    kind: Literal["static", "dynamic"]
    state: ResourceState
    name: Annotated[str, Field(min_length=1, max_length=128)]
    container_id: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None
    definition: CreationIntent | StaticReservation

    @model_validator(mode="after")
    def identity(self) -> Self:
        definition = self.definition
        if isinstance(definition, CreationIntent):
            if (
                self.kind != "dynamic"
                or self.resource_id != definition.intent_id
                or self.name != definition.name
                or definition.creator_fence > self.fence
                or self.state in {"released"}
            ):
                raise ValueError("Dynamic lifecycle identity differs from its intent")
        elif (
            self.kind != "static"
            or self.resource_id != definition.resource_id
            or self.name != definition.backend.name
            or self.container_id != definition.backend.container_id
            or self.state not in {"observed", "draining", "released"}
        ):
            raise ValueError("Static lifecycle identity differs from its reservation")
        if definition.run_id != self.run_id:
            raise ValueError("Lifecycle run identity differs")
        if (
            self.state in {"observed", "starting", "started", "draining", "removed", "released"}
            and self.container_id is None
        ):
            raise ValueError("Observed lifecycle state requires an immutable container ID")
        return self


class RuntimeRecord(ScopeRecord):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    mode: Literal["docker"] = "docker"
    traffic: TrafficSummary
    lifecycle_events: Annotated[int, Field(ge=1, le=1024)]
