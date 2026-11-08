"""Bounded installation resource evidence and fail-closed admission decisions."""

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import AwareDatetime, Field, field_validator, model_validator

from mslb_contracts.docker_base import DockerId, DockerModel

type ServiceName = Literal[
    "web",
    "api",
    "worker",
    "postgres",
    "redis",
    "docker-proxy",
    "backend-a",
    "backend-b",
    "container-controller",
    "resource-guard",
]
type ResourceReason = Literal[
    "MEASUREMENTS_UNAVAILABLE",
    "MEASUREMENTS_STALE",
    "ENGINE_CAPACITY",
    "MISSING_SERVICE",
    "MEMORY_HEADROOM",
    "RESOURCE_PRESSURE",
    "WARMING_UP",
]

SERVICE_LIMITS_MB: dict[ServiceName, int] = {
    "web": 128,
    "api": 384,
    "worker": 1024,
    "postgres": 768,
    "redis": 192,
    "docker-proxy": 160,
    "backend-a": 128,
    "backend-b": 128,
    "container-controller": 192,
    "resource-guard": 128,
}
MIB = 1024**2
# Admission starts below 70%, stops work at 85%, and recovers after three
# distinct samples below 60%. These intentionally leave allocation headroom.
ADMISSION_PRESSURE_PERCENT = 70
STOP_PRESSURE_PERCENT = 85
RECOVERY_PRESSURE_PERCENT = 60
RECOVERY_SAMPLE_COUNT = 3
MEASUREMENT_MAX_AGE_SECONDS = 3
MIN_ENGINE_MEMORY_BYTES = 8 * 1024**3
MIN_MEMORY_RESERVE_BYTES = 2 * 1024**3
ENGINE_RESERVE_DIVISOR = 4  # Reserve at least 25% of engine RAM.
STARTUP_AND_DYNAMIC_BYTES = (128 + 64 + 256) * MIB


class ServiceMemory(DockerModel):
    """Report one approved service's measured memory, health and OOM observations."""

    service: ServiceName
    container_id: DockerId | None
    limit_bytes: Annotated[int, Field(ge=1, le=16 * 1024**3)]
    usage_bytes: Annotated[int, Field(ge=0, le=2**63 - 1)]
    working_set_bytes: Annotated[int, Field(ge=0, le=2**63 - 1)]
    healthy: bool
    oom_killed: bool
    restart_count: Annotated[int, Field(ge=0)]

    @model_validator(mode="after")
    def counters(self) -> Self:
        """Reject impossible working sets or healthy measurements for absent services."""
        if self.working_set_bytes > self.usage_bytes:
            raise ValueError("Working set cannot exceed raw container memory usage")
        if self.container_id is None and (
            self.healthy or self.usage_bytes or self.working_set_bytes
        ):
            raise ValueError("An absent service cannot claim healthy memory measurements")
        return self


class ResourceProbe(DockerModel):
    """Bind bounded Docker engine and service measurements to trusted UTC time."""

    schema_version: Annotated[int, Field(ge=2, le=2)] = 2
    measured_at: AwareDatetime
    engine_memory_bytes: Annotated[int, Field(ge=1, le=2**63 - 1)]
    available_memory_bytes: Annotated[int, Field(ge=0, le=2**63 - 1)]
    desktop_engine: bool
    services: Annotated[tuple[ServiceMemory, ...], Field(min_length=10, max_length=10)]

    @field_validator("measured_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        """Require a UTC timestamp on trusted resource probe frames."""
        offset = value.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError("Resource evidence must use UTC")
        return value

    @model_validator(mode="after")
    def topology(self) -> Self:
        """Require one measurement per approved service with the configured memory cap."""
        if {item.service for item in self.services} != set(
            SERVICE_LIMITS_MB
        ) or self.available_memory_bytes > self.engine_memory_bytes:
            raise ValueError("Resource evidence has an inconsistent topology/capacity")
        ids = [item.container_id for item in self.services if item.container_id is not None]
        if len(set(ids)) != len(ids):
            raise ValueError("A container cannot represent two service memory budgets")
        return self


class ResourceDecision(DockerModel):
    """Report explicit admission/stopping policy and its measured headroom requirement."""

    admit: bool
    stop_running: bool
    reason: ResourceReason | None
    cap_sum_bytes: Annotated[int, Field(ge=0, le=2**63 - 1)]
    required_headroom_bytes: Annotated[int, Field(ge=0, le=2**63 - 1)]

    @model_validator(mode="after")
    def status(self) -> Self:
        """Require a refusal reason and prevent contradictory admission/stop decisions."""
        if self.admit and (self.stop_running or self.reason is not None):
            raise ValueError("An admitted resource decision cannot also refuse or stop work")
        if not self.admit and self.reason is None:
            raise ValueError("Resource refusal requires an explicit reason")
        return self


def memory_bounds(*, probe: ResourceProbe | None) -> tuple[int, int, int]:
    """Return service caps, engine reserve and remaining-growth headroom in bytes."""
    caps = (
        0
        if probe is None
        else sum(item.limit_bytes for item in probe.services) + STARTUP_AND_DYNAMIC_BYTES
    )
    working = 0 if probe is None else sum(item.working_set_bytes for item in probe.services)
    reserve = max(
        MIN_MEMORY_RESERVE_BYTES,
        0 if probe is None else probe.engine_memory_bytes // ENGINE_RESERVE_DIVISOR,
    )
    return caps, reserve, max(0, caps - working) + reserve


def safe_to_admit(*, probe: ResourceProbe) -> bool:
    """Require measured Docker headroom and healthy bounded consumers; check age separately."""
    caps, reserve, headroom = memory_bounds(probe=probe)
    return (
        probe.engine_memory_bytes >= MIN_ENGINE_MEMORY_BYTES
        and caps + reserve <= probe.engine_memory_bytes
        and probe.available_memory_bytes >= headroom
        and all(
            (item.service == "redis" or (item.healthy and item.container_id is not None))
            and not item.oom_killed
            and item.working_set_bytes * 100 < item.limit_bytes * ADMISSION_PRESSURE_PERCENT
            for item in probe.services
        )
    )


def parse_resource_guard(*, value: str) -> bool:
    """Parse a trusted service flag; production entrypoints default to enforcement."""
    if value not in {"true", "false"}:
        raise ValueError("RESOURCE_GUARD_ENABLED must be true or false")
    return value == "true"
