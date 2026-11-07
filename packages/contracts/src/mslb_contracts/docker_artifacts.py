"""Scoped physical Docker artifact records; no deterministic replay compatibility claim."""

from datetime import datetime
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, field_validator, model_validator

from mslb_contracts.docker_base import DockerModel
from mslb_contracts.docker_observations import BackendMeasurements, BackendSample
from mslb_contracts.docker_ownership import CreationIntent, StaticReservation
from mslb_contracts.docker_runtime import HttpEvent, TrafficSummary
from mslb_contracts.durability import StoredFile
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
    """Bind an operational observation to run, fence, snapshot and real UTC time."""

    run_id: UUID
    fence: Annotated[int, Field(ge=1)]
    snapshot_digest: Digest
    observed_at: AwareDatetime

    @field_validator("observed_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        """Require UTC timestamps on operational artifact scope records."""
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError("Artifact timestamps must use UTC")
        return value


class HttpRecord(HttpEvent, ScopeRecord):
    """Bind an actual HTTP observation to its run/fence/snapshot and UTC observation time."""


class LifecycleRecord(ScopeRecord):
    """Record a trusted resource definition and its observed operational lifecycle."""

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
        """Reject resource observations that conflict with their trusted definition."""
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


class DockerProgress(ScopeRecord):
    """One bounded actual controller frame, usable only with current SQL ownership/freshness."""

    schema_version: Literal[1] = 1
    mode: Literal["docker"] = "docker"
    elapsed_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    pending: Annotated[int, Field(ge=0, le=256)]
    inflight: Annotated[int, Field(ge=0, le=16)]
    backends: Annotated[tuple[BackendSample, ...], Field(max_length=4)]
    unavailable_backends: Annotated[tuple[str, ...], Field(max_length=4)]

    @model_validator(mode="after")
    def samples(self) -> Self:
        """Validate bounded unique backend identities and fresh observation timestamps."""
        names = [sample.health.backend_id for sample in self.backends]
        if (
            len(names) + len(self.unavailable_backends) > 4
            or len(set(names)) != len(names)
            or len({sample.resource_id for sample in self.backends}) != len(self.backends)
            or len(set(self.unavailable_backends)) != len(self.unavailable_backends)
            or set(names).intersection(self.unavailable_backends)
            or any(not 1 <= len(name) <= 128 for name in self.unavailable_backends)
            or any(
                not 0 <= (self.observed_at - sample.observed_at).total_seconds() <= 3
                for sample in self.backends
            )
        ):
            raise ValueError("Live backend observations exceed identity/freshness bounds")
        return self


class RuntimeRecord(ScopeRecord):
    """Bind terminal accounting and optional measured windows to the operational scope."""

    schema_version: Literal[1, 2] = 1
    mode: Literal["docker"] = "docker"
    traffic: TrafficSummary
    lifecycle_events: Annotated[int, Field(ge=1, le=1024)]
    observations: Annotated[tuple[BackendMeasurements, ...], Field(max_length=64)] = Field(
        default=(), exclude_if=lambda value: not value
    )

    @model_validator(mode="after")
    def measurements(self) -> Self:
        """Verify runtime measurement identities and version-specific accounting."""
        if (
            (self.schema_version == 1 and self.observations)
            or (
                len({item.first.resource_id for item in self.observations})
                != len(self.observations)
            )
            or any(item.latest.observed_at > self.observed_at for item in self.observations)
        ):
            raise ValueError("Runtime observation version, scope or clock differs")
        return self


class DockerManifest(DockerModel):
    """Bind verified operational artifact references to one fenced publication."""

    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    mode: Literal["docker"] = "docker"
    run_id: UUID
    publication_fence: Annotated[int, Field(ge=1)]
    snapshot_digest: Digest
    total: Annotated[int, Field(ge=1, le=10_000)]
    completed: Annotated[int, Field(ge=0, le=10_000)]
    failed: Annotated[int, Field(ge=0, le=10_000)]
    dropped: Annotated[int, Field(ge=0, le=10_000)]
    unexecuted: Annotated[int, Field(ge=0, le=10_000)]
    artifacts: Annotated[dict[str, StoredFile], Field(min_length=3, max_length=3)]

    @model_validator(mode="after")
    def artifacts_and_accounting(self) -> Self:
        """Require the complete artifact set and internally consistent terminal accounting."""
        names = {"http_events.jsonl", "runtime_summary.json", "container_lifecycle.jsonl"}
        prefix = f"docker/{self.run_id.hex}/{self.publication_fence}/"
        if (
            self.completed + self.failed + self.dropped + self.unexecuted != self.total
            or set(self.artifacts) != names
            or sum(value.size_bytes for value in self.artifacts.values()) > 512 * 1024**2
            or any(value.key != prefix + name for name, value in self.artifacts.items())
        ):
            raise ValueError("Docker artifact namespace or terminal accounting is invalid")
        return self
