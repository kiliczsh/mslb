"""Strict logical execution settings and immutable reproducibility snapshots."""

import hashlib
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import (
    STANDARD_REQUESTS,
    STRESS_REQUESTS,
    CapacityProfile,
    request_limit,
    validate_request_limit,
)
from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import InputError, PositiveValue, RequestInput, ServerInput, input_digest

type Digest = Annotated[str, Field(min_length=64, max_length=64, pattern=r"^[0-9a-f]{64}$")]


class TemplateSettings(BaseModel):
    """Validate the logical capacity template frozen for simulated scaling."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    cpu_units_per_tick: PositiveValue
    mem_mb: PositiveValue
    rate_limit_per_sec: PositiveValue


class ScalingSettings(BaseModel):
    """Validate exact simulated scaling thresholds and bounded observation windows."""

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


class ExecutionSettings(BaseModel):
    """Validate deterministic scheduling, queue, horizon and arithmetic limits."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
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


class RunSettings(ExecutionSettings):
    """Select public logical policy with sequential execution as the default."""

    mode: Literal["concurrent", "sequential"] = "sequential"


class LegacyRunSettings(ExecutionSettings):
    """Read archived settings without changing the bytes bound by their digest."""

    mode: Literal["sequential_compat"]


class StressRunSettings(RunSettings):
    """Freeze an explicitly selected installation stress ceiling outside public create DTOs."""

    capacity_profile: Literal["stress"]
    max_requests: Literal[50_000]


type StoredRunSettings = RunSettings | LegacyRunSettings | StressRunSettings
type ReadRunSettings = RunSettings | StressRunSettings


def stored_request_limit(*, settings: StoredRunSettings) -> int:
    """Read the immutable execution bound while preserving historical standard settings."""
    value = settings.max_requests if isinstance(settings, StressRunSettings) else STANDARD_REQUESTS
    validate_request_limit(value=value)
    return value


def settings_for_profile(*, settings: RunSettings, profile: CapacityProfile) -> StoredRunSettings:
    """Attach a server-selected stress bound; this is not a client request option."""
    if request_limit(profile=profile) == STANDARD_REQUESTS:
        return settings
    return StressRunSettings.model_validate(
        obj=settings.model_dump(mode="python")
        | {"capacity_profile": "stress", "max_requests": STRESS_REQUESTS}
    )


def public_settings(*, settings: StoredRunSettings) -> RunSettings:
    if isinstance(settings, StressRunSettings):
        return RunSettings.model_validate(
            obj=settings.model_dump(mode="python", exclude={"capacity_profile", "max_requests"})
        )
    if isinstance(settings, RunSettings):
        return settings
    return RunSettings.model_validate(
        obj=settings.model_dump(mode="python") | {"mode": "sequential"}
    )


class RunSnapshot(BaseModel):
    """Bind sorted immutable inputs, expanded policy and engine version to canonical digests."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    servers: Annotated[tuple[ServerInput, ...], Field(min_length=1, max_length=64)]
    requests: Annotated[tuple[RequestInput, ...], Field(min_length=1, max_length=STRESS_REQUESTS)]
    configuration: StoredRunSettings
    dataset_digest: Digest
    server_snapshot_digest: Digest
    snapshot_digest: Digest

    @model_validator(mode="after")
    def population(self) -> Self:
        """Reject larger populations unless their effective stress ceiling is frozen."""
        if len(self.requests) > stored_request_limit(settings=self.configuration):
            raise ValueError("Request population exceeds its frozen capacity profile")
        return self


class ArtifactMetadata(BaseModel):
    """Describe an artifact's exact immutable byte length and SHA-256."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    size_bytes: Annotated[int, Field(ge=0, le=256 * 1024 * 1024)]
    sha256: Digest


def verify_snapshot_identity(snapshot: RunSnapshot) -> None:
    """Verify normalized structural identity without depending on engine execution."""
    expected = hashlib.sha256(
        data=canonical_bytes(value=snapshot.model_dump(mode="json", exclude={"snapshot_digest"}))
    ).hexdigest()
    if (
        snapshot.snapshot_digest != expected
        or len(snapshot.requests) > stored_request_limit(settings=snapshot.configuration)
        or snapshot.dataset_digest != input_digest(records=snapshot.requests)
        or snapshot.server_snapshot_digest != input_digest(records=snapshot.servers)
        or snapshot.servers != tuple(sorted(snapshot.servers, key=lambda server: server.id))
        or snapshot.requests
        != tuple(sorted(snapshot.requests, key=lambda request: (request.t, request.request_id)))
        or len({server.id for server in snapshot.servers}) != len(snapshot.servers)
        or len({request.request_id for request in snapshot.requests}) != len(snapshot.requests)
    ):
        raise InputError(field="snapshot", reason="identity_or_normalization_mismatch")


class BundleManifest(BaseModel):
    """Bind canonical artifact identities to independent and supplied-validator provenance."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    snapshot_digest: Digest
    bundle_digest: Digest
    terminal_tick: Annotated[int, Field(ge=0, le=1_000_000)]
    event_count: Annotated[int, Field(ge=1, le=3 * STRESS_REQUESTS)]
    decision_count: Annotated[int, Field(ge=0, le=1_000_000)]
    independent_replay: Literal["passed"]
    original_validator: Literal["passed", "not_applicable"]
    original_validator_digest: Digest
    artifacts: Annotated[dict[str, ArtifactMetadata], Field(min_length=1, max_length=16)]
