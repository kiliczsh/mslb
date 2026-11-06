"""Exact saved metrics, chart ranges and compatible comparisons."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from mslb_contracts.read_base import JsonValue, ReadModel
from mslb_contracts.runs import Digest, RunSettings


class MetricsView(ReadModel):
    """Expose exact cluster and selected-server metrics from verified saved artifacts."""

    run_id: UUID
    bundle_digest: Digest
    summary: dict[str, JsonValue]
    servers: Annotated[dict[str, JsonValue], Field(max_length=32)]


class ServerMetricView(ReadModel):
    """Expose one server's exact resource and latency metrics."""

    id: Annotated[str, Field(min_length=1, max_length=128)]
    metrics: dict[str, JsonValue]


class ChartView(ReadModel):
    """Expose bounded saved chart buckets with exact source values."""

    run_id: UUID
    bundle_digest: Digest
    metric: Literal["cpu_utilization", "memory_utilization", "pending_depth", "active_requests"]
    server_id: Annotated[str, Field(min_length=1, max_length=128)] | None
    start_tick: Annotated[int, Field(ge=0, le=999_999)]
    end_tick: Annotated[int, Field(ge=1, le=1_000_000)]
    points: Annotated[tuple[dict[str, JsonValue], ...], Field(max_length=2000)]


class ComparisonRun(ReadModel):
    """Bind one strategy's exact metrics to its frozen comparison identity."""

    run_id: UUID
    bundle_digest: Digest
    configuration: RunSettings
    differing_settings: Annotated[tuple[str, ...], Field(max_length=2)]
    metrics: dict[str, JsonValue]


class ComparisonView(ReadModel):
    """Compare compatible completed logical runs without mixing operational results."""

    baseline_run_id: UUID
    runs: Annotated[tuple[ComparisonRun, ...], Field(min_length=2, max_length=8)]
