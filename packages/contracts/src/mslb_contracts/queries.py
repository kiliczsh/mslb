"""Transport-neutral bounded read models; storage keys never cross this boundary."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from mslb_contracts.runs import Digest, RunSettings

type JsonValue = None | bool | int | str | list[JsonValue] | dict[str, JsonValue]
type JobStatus = Literal[
    "queued", "running", "cancel_requested", "completed", "failed", "cancelled"
]


class ReadModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class Page[T](ReadModel):
    items: Annotated[tuple[T, ...], Field(max_length=500)]
    next_cursor: Annotated[str, Field(max_length=2048)] | None = None


class DatasetView(ReadModel):
    id: UUID
    digest: Digest
    source_filename: Annotated[str, Field(min_length=1, max_length=255)]
    request_count: Annotated[int, Field(ge=1, le=10_000)]
    earliest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    latest_arrival: Annotated[int, Field(ge=0, le=999_999)]
    size_bytes: Annotated[int, Field(ge=1, le=10 * 1024**2)]
    created_at: datetime
    original_digest: Digest | None
    source_size_bytes: Annotated[int, Field(ge=1, le=10 * 1024**2)] | None


class RunProgress(ReadModel):
    revision: Annotated[int, Field(ge=1)]
    kind: Literal["checkpoint", "terminal"]
    next_tick: Annotated[int, Field(ge=0, le=1_000_000)]
    executed_intervals: Annotated[int, Field(ge=0, le=1_000_000)]
    committed_events: Annotated[int, Field(ge=0, le=30_000)]


class RunView(ReadModel):
    id: UUID
    dataset_id: UUID
    snapshot_digest: Digest
    dataset_digest: Digest
    server_snapshot_digest: Digest
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    configuration: RunSettings
    status: JobStatus
    attempts: Annotated[int, Field(ge=0, le=3)]
    created_at: datetime
    completed_at: datetime | None
    error_code: Annotated[str, Field(max_length=64)] | None
    progress: RunProgress | None


class PublishedValidation(ReadModel):
    """Receipt from the verified, durably published manifest; no storage paths."""

    bundle_digest: Digest
    independent_replay: Literal["passed"]
    original_validator: Literal["passed", "not_applicable"]


class RunStatus(ReadModel):
    run_id: UUID
    status: JobStatus
    attempts: Annotated[int, Field(ge=0, le=3)]
    error_code: Annotated[str, Field(max_length=64)] | None
    progress: RunProgress | None
    validation: PublishedValidation | None


class WaitResult(ReadModel):
    statuses: Annotated[tuple[RunStatus, ...], Field(min_length=1, max_length=8)]
    all_terminal: bool
    timed_out: bool


class MetricsView(ReadModel):
    run_id: UUID
    bundle_digest: Digest
    summary: dict[str, JsonValue]
    servers: Annotated[dict[str, JsonValue], Field(max_length=32)]


class ServerMetricView(ReadModel):
    id: Annotated[str, Field(min_length=1, max_length=128)]
    metrics: dict[str, JsonValue]


class ChartView(ReadModel):
    run_id: UUID
    bundle_digest: Digest
    metric: Literal["cpu_utilization", "memory_utilization", "pending_depth", "active_requests"]
    server_id: Annotated[str, Field(min_length=1, max_length=128)] | None
    start_tick: Annotated[int, Field(ge=0, le=999_999)]
    end_tick: Annotated[int, Field(ge=1, le=1_000_000)]
    points: Annotated[tuple[dict[str, JsonValue], ...], Field(max_length=2000)]


class ComparisonRun(ReadModel):
    run_id: UUID
    bundle_digest: Digest
    configuration: RunSettings
    differing_settings: Annotated[tuple[str, ...], Field(max_length=2)]
    metrics: dict[str, JsonValue]


class ComparisonView(ReadModel):
    baseline_run_id: UUID
    runs: Annotated[tuple[ComparisonRun, ...], Field(min_length=2, max_length=8)]
