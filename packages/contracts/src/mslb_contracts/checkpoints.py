"""Versioned bounded transport state for completed-interval recovery."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from mslb_contracts.capacity_profiles import MAX_LOGICAL_EVENTS, STRESS_REQUESTS
from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import PositiveValue, ServerInput
from mslb_contracts.runs import Digest

type Tick = Annotated[int, Field(ge=0, le=1_000_000)]
type Count = Annotated[int, Field(ge=0, le=10_000)]
type RequestCount = Annotated[int, Field(ge=0, le=STRESS_REQUESTS)]
type Identifier = Annotated[str, Field(min_length=1, max_length=128)]
type Integral = Annotated[int, Field(ge=0, le=10**15)]
type PrefixName = Literal[
    "run.jsonl", "autoscaling_decisions.jsonl", "timeline.jsonl", "drop_reasons.jsonl"
]


class CheckpointModel(BaseModel):
    """Provide strict immutable checkpoint records with versioned validation."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ActiveWork(CheckpointModel):
    """Preserve exact remaining work and full memory reservation for an active request."""

    request_id: Identifier
    remaining: FractionValue


class ServerExecution(CheckpointModel):
    """Preserve membership, active work and start counters at a safe boundary."""

    server: ServerInput
    reserved_mem_mb: Annotated[int, Field(ge=0, le=1_000_000_000)]
    start_counter: RequestCount
    active: Annotated[tuple[ActiveWork, ...], Field(max_length=STRESS_REQUESTS)]


class ScalingObservation(CheckpointModel):
    """Preserve one exact autoscaling observation for checkpoint continuation."""

    ending_tick: Tick
    cpu_utilization: FractionValue
    drops: RequestCount
    drop_ratio: FractionValue


class PendingChange(CheckpointModel):
    """Preserve a decided membership change awaiting its effective next boundary."""

    t: Tick
    action: Literal["scale_out", "scale_in"]
    trigger: Annotated[tuple[Annotated[str, Field(max_length=64)], ...], Field(max_length=4)]
    applied_at: Tick
    server_ids_added: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    server_ids_removed: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    reason: None = None


class ScalingExecution(CheckpointModel):
    """Preserve autoscaler windows, counters and pending changes."""

    history: Annotated[tuple[ScalingObservation, ...], Field(max_length=10_000)]
    next_eligible_tick: Annotated[int, Field(ge=0, le=2_000_000)]
    counter: Count
    pending: PendingChange | None


class StartedRequest(CheckpointModel):
    """Preserve a request's assigned server and start tick."""

    request_id: Identifier
    t: Tick
    server_id: Identifier


class EndedRequest(CheckpointModel):
    """Preserve a request's terminal tick and lifecycle identity."""

    request_id: Identifier
    t: Tick


class ServerIntegrals(CheckpointModel):
    """Preserve exact historical CPU/memory integrals for one server identity."""

    server_id: Identifier
    cpu_units_per_tick: PositiveValue
    mem_mb: PositiveValue
    cpu_allocated: FractionValue
    cpu_consumed: FractionValue
    cpu_capacity_ticks: Integral
    memory_reserved_mb_ticks: Integral
    memory_capacity_mb_ticks: Integral


class MetricExecution(CheckpointModel):
    """Preserve bounded exact metric accumulation at a completed interval."""

    arrivals: Annotated[tuple[Identifier, ...], Field(max_length=STRESS_REQUESTS)]
    starts: Annotated[tuple[StartedRequest, ...], Field(max_length=STRESS_REQUESTS)]
    finishes: Annotated[tuple[EndedRequest, ...], Field(max_length=STRESS_REQUESTS)]
    drops: Annotated[tuple[EndedRequest, ...], Field(max_length=STRESS_REQUESTS)]
    servers: Annotated[tuple[ServerIntegrals, ...], Field(min_length=1, max_length=10_000)]
    queue_peak: RequestCount
    queue_integral: Annotated[int, Field(ge=0, le=STRESS_REQUESTS * 1_000_000)]


class ArtifactPrefix(CheckpointModel):
    """Bind a committed artifact prefix to exact bytes, records and SHA-256."""

    name: PrefixName
    size_bytes: Annotated[int, Field(ge=0, le=256 * 1024 * 1024)]
    records: Annotated[int, Field(ge=0, le=1_000_001)]
    sha256: Digest


class IntervalCheckpoint(CheckpointModel):
    """Freeze resumable completed-interval state and all four committed prefixes."""

    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    snapshot_digest: Digest
    next_tick: Annotated[int, Field(ge=1, le=1_000_000)]
    input_cursor: RequestCount
    executed_intervals: Annotated[int, Field(ge=1, le=1_000_000)]
    event_count: Annotated[int, Field(ge=0, le=MAX_LOGICAL_EVENTS)]
    decision_count: Annotated[int, Field(ge=0, le=1_000_000)]
    recorded_samples: Annotated[int, Field(ge=1, le=1_000_000)]
    round_robin_anchor: Identifier | None
    servers: Annotated[tuple[ServerExecution, ...], Field(min_length=1, max_length=64)]
    pending_ids: Annotated[tuple[Identifier, ...], Field(max_length=STRESS_REQUESTS)]
    autoscaler: ScalingExecution
    metrics: MetricExecution
    prefixes: Annotated[tuple[ArtifactPrefix, ...], Field(min_length=4, max_length=4)]
