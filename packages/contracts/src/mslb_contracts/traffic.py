"""Bounded real HTTP contracts, separate from canonical logical simulation events."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from mslb_contracts.inputs import PositiveValue, normalize_identifier


class WorkRequest(BaseModel):
    """Logical memory is an admission reservation, never an allocation instruction."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    request_id: Annotated[str, Field(max_length=256)]
    work_units: PositiveValue
    mem_mb: PositiveValue

    @field_validator("request_id")
    @classmethod
    def normalize_id(cls, value: str) -> str:
        return normalize_identifier(value=value)


class WorkCompleted(BaseModel):
    """An actual completed backend call; a timeout/error cannot use this response."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    backend_id: Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{0,127}$")]
    request_id: Annotated[str, Field(min_length=1, max_length=128)]
    outcome: Literal["completed"] = "completed"
    operations: Annotated[int, Field(ge=1, le=2_000_000)]
    checksum: Annotated[str, Field(pattern=r"^[0-9a-f]{8}$")]
    duration_ms: Annotated[float, Field(ge=0, le=3000, allow_inf_nan=False)]


class BackendHealth(BaseModel):
    """Current backend admission state and distinct measured runtime observations."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    backend_id: Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{0,127}$")]
    ready: bool
    logical_admission_mem_mb: PositiveValue
    max_inflight: Annotated[int, Field(ge=1, le=4)]
    inflight: Annotated[int, Field(ge=0, le=4)]
    reserved_mem_mb: Annotated[int, Field(ge=0, le=1_000_000_000)]
    operations_per_unit: Annotated[int, Field(ge=2048, le=2048)] = 2048
    max_operations: Annotated[int, Field(ge=2_000_000, le=2_000_000)] = 2_000_000
    work_timeout_ms: Annotated[int, Field(ge=2000, le=2000)] = 2000
    rss_bytes: Annotated[int, Field(ge=0)] | None
    cgroup_memory_bytes: Annotated[int, Field(ge=0)] | None
    cpu_usage_usec: Annotated[int, Field(ge=0)] | None
    throttled_usec: Annotated[int, Field(ge=0)] | None
