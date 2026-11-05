"""Versioned bounded transport state for completed-interval recovery."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import PositiveValue, ServerInput
from mslb_contracts.runs import Digest

type Tick = Annotated[int, Field(ge=0, le=1_000_000)]
type Count = Annotated[int, Field(ge=0, le=10_000)]
type Identifier = Annotated[str, Field(min_length=1, max_length=128)]
type Integral = Annotated[int, Field(ge=0, le=10**15)]
type PrefixName = Literal[
    "run.jsonl", "autoscaling_decisions.jsonl", "timeline.jsonl", "drop_reasons.jsonl"
]


class CheckpointModel(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")


class ActiveWork(CheckpointModel):
    request_id: Identifier
    remaining: FractionValue


class ServerExecution(CheckpointModel):
    server: ServerInput
    reserved_mem_mb: Annotated[int, Field(ge=0, le=1_000_000_000)]
    start_counter: Count
    active: Annotated[tuple[ActiveWork, ...], Field(max_length=10_000)]


class ScalingObservation(CheckpointModel):
    ending_tick: Tick
    cpu_utilization: FractionValue
    drops: Count
    drop_ratio: FractionValue


class PendingChange(CheckpointModel):
    t: Tick
    action: Literal["scale_out", "scale_in"]
    trigger: Annotated[tuple[Annotated[str, Field(max_length=64)], ...], Field(max_length=4)]
    applied_at: Tick
    server_ids_added: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    server_ids_removed: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    reason: None = None


class ScalingExecution(CheckpointModel):
    history: Annotated[tuple[ScalingObservation, ...], Field(max_length=10_000)]
    next_eligible_tick: Annotated[int, Field(ge=0, le=2_000_000)]
    counter: Count
    pending: PendingChange | None


class StartedRequest(CheckpointModel):
    request_id: Identifier
    t: Tick
    server_id: Identifier


class EndedRequest(CheckpointModel):
    request_id: Identifier
    t: Tick


class ServerIntegrals(CheckpointModel):
    server_id: Identifier
    cpu_units_per_tick: PositiveValue
    mem_mb: PositiveValue
    cpu_allocated: FractionValue
    cpu_consumed: FractionValue
    cpu_capacity_ticks: Integral
    memory_reserved_mb_ticks: Integral
    memory_capacity_mb_ticks: Integral


class MetricExecution(CheckpointModel):
    arrivals: Annotated[tuple[Identifier, ...], Field(max_length=10_000)]
    starts: Annotated[tuple[StartedRequest, ...], Field(max_length=10_000)]
    finishes: Annotated[tuple[EndedRequest, ...], Field(max_length=10_000)]
    drops: Annotated[tuple[EndedRequest, ...], Field(max_length=10_000)]
    servers: Annotated[tuple[ServerIntegrals, ...], Field(min_length=1, max_length=10_000)]
    queue_peak: Count
    queue_integral: Annotated[int, Field(ge=0, le=10**10)]


class ArtifactPrefix(CheckpointModel):
    name: PrefixName
    size_bytes: Annotated[int, Field(ge=0, le=256 * 1024 * 1024)]
    records: Annotated[int, Field(ge=0, le=1_000_001)]
    sha256: Digest


class IntervalCheckpoint(CheckpointModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    snapshot_digest: Digest
    next_tick: Annotated[int, Field(ge=1, le=1_000_000)]
    input_cursor: Count
    executed_intervals: Annotated[int, Field(ge=1, le=1_000_000)]
    event_count: Annotated[int, Field(ge=0, le=30_000)]
    decision_count: Annotated[int, Field(ge=0, le=1_000_000)]
    recorded_samples: Annotated[int, Field(ge=1, le=1_000_000)]
    round_robin_anchor: Identifier | None
    servers: Annotated[tuple[ServerExecution, ...], Field(min_length=1, max_length=64)]
    pending_ids: Annotated[tuple[Identifier, ...], Field(max_length=10_000)]
    autoscaler: ScalingExecution
    metrics: MetricExecution
    prefixes: Annotated[tuple[ArtifactPrefix, ...], Field(min_length=4, max_length=4)]
