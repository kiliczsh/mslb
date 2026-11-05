"""Strict logical execution settings and immutable reproducibility snapshots."""

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field

from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import PositiveValue, RequestInput, ServerInput

type Digest = Annotated[str, Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")]


class TemplateSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    cpu_units_per_tick: PositiveValue
    mem_mb: PositiveValue
    rate_limit_per_sec: PositiveValue


class ScalingSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    enabled: bool = False
    window_ticks: Annotated[int, Field(ge=1, le=10_000)] = 5
    scale_out_cpu_threshold: FractionValue = Field(
        default_factory=lambda: FractionValue(numerator="4", denominator="5")
    )
    scale_in_cpu_threshold: FractionValue = Field(
        default_factory=lambda: FractionValue(numerator="1", denominator="5")
    )
    scale_out_drop_rate_threshold: FractionValue = Field(
        default_factory=lambda: FractionValue(numerator="1", denominator="10")
    )
    scale_out_queue_threshold: Annotated[int, Field(ge=0, le=10_000)] = 1
    cooldown_ticks: Annotated[int, Field(ge=0, le=1_000_000)] = 3
    min_servers: Annotated[int, Field(ge=1, le=64)] | None = None
    max_servers: Annotated[int, Field(ge=1, le=64)] | None = None
    scale_out_step: Annotated[int, Field(ge=1, le=64)] = 1
    scale_in_step: Annotated[int, Field(ge=1, le=64)] = 1
    server_template: TemplateSettings | None = None


class RunSettings(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    mode: Literal["concurrent", "sequential_compat"] = "concurrent"
    strategy: Literal["first_fit", "round_robin", "least_connections", "most_available_memory"] = (
        "first_fit"
    )
    queue_timeout_ticks: Annotated[int, Field(ge=1, le=1_000_000)] | None = None
    max_queue_size: Annotated[int, Field(ge=0, le=10_000)] | None = None
    max_simulation_ticks: Annotated[int, Field(ge=1, le=1_000_000)] = 100_000
    max_samples: Annotated[int, Field(ge=1, le=1_000_000)] = 1_000_000
    max_fraction_bits: Annotated[int, Field(ge=1, le=4096)] = 4096
    max_metric_series: Annotated[int, Field(ge=1, le=10_000)] = 10_000
    autoscaling: ScalingSettings = Field(default_factory=ScalingSettings)


class RunSnapshot(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    servers: Annotated[tuple[ServerInput, ...], Field(min_length=1, max_length=64)]
    requests: Annotated[tuple[RequestInput, ...], Field(min_length=1, max_length=10_000)]
    configuration: RunSettings
    dataset_digest: Digest
    server_snapshot_digest: Digest
    snapshot_digest: Digest


class ArtifactMetadata(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    size_bytes: Annotated[int, Field(ge=0, le=256 * 1024 * 1024)]
    sha256: Digest


class BundleManifest(BaseModel):
    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    snapshot_digest: Digest
    bundle_digest: Digest
    terminal_tick: Annotated[int, Field(ge=0, le=1_000_000)]
    event_count: Annotated[int, Field(ge=1, le=30_000)]
    decision_count: Annotated[int, Field(ge=0, le=1_000_000)]
    independent_replay: Literal["passed"]
    original_validator: Literal["passed", "not_applicable"]
    original_validator_digest: Digest
    artifacts: Annotated[dict[str, ArtifactMetadata], Field(min_length=1, max_length=16)]
