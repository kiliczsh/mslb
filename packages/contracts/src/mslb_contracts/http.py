"""REST request/response contracts; exact semantic settings remain shared."""

from typing import Annotated, Self
from uuid import UUID

from pydantic import ConfigDict, Field, model_validator

from mslb_contracts.inputs import PositiveValue
from mslb_contracts.read_base import JobStatus, ReadModel
from mslb_contracts.run_views import RunView
from mslb_contracts.runs import BundleManifest, Digest, RunSettings


class ServerPatch(ReadModel):
    """Change at least one capacity while retaining the managed server identity."""

    model_config = ConfigDict(json_schema_extra={"examples": [{"mem_mb": 128}]})
    cpu_units_per_tick: PositiveValue | None = None
    mem_mb: PositiveValue | None = None
    rate_limit_per_sec: PositiveValue | None = None

    @model_validator(mode="after")
    def nonempty(self) -> Self:
        if not self.model_fields_set or any(
            getattr(self, key) is None for key in self.model_fields_set
        ):
            raise ValueError("at_least_one_non_null_capacity_required")
        return self


class SimulationCreate(RunSettings):
    """Submit a registered dataset with validated deterministic execution settings."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "dataset_id": "00000000-0000-0000-0000-000000000001",
                    "mode": "concurrent",
                    "strategy": "round_robin",
                    "autoscaling": {"enabled": False},
                    "max_simulation_ticks": 100000,
                }
            ]
        }
    )
    # FastAPI decodes the JSON object before Pydantic; UUID strings are the wire form.
    dataset_id: Annotated[UUID, Field(strict=False)]


class SimulationAccepted(ReadModel):
    """Acknowledge durable queued work with its frozen snapshot identity."""

    run_id: UUID
    status: JobStatus
    snapshot_digest: Digest


class RunDetail(RunView):
    """Expose frozen run policy, durable progress and published validation results."""

    mode: str
    strategy: str
    validation: BundleManifest | None


class StrategyView(ReadModel):
    """Describe one supported deterministic scheduling strategy."""

    id: str
    display_name: str
    description: str


class CompareRequest(ReadModel):
    """Select a bounded set of completed logical runs for compatibility-checked comparison."""

    run_ids: Annotated[
        tuple[Annotated[UUID, Field(strict=False)], ...],
        Field(min_length=2, max_length=8, strict=False),
    ]


class FieldError(ReadModel):
    """Describe a bounded validation field/reason without echoing raw input."""

    field: Annotated[str, Field(max_length=256)]
    reason: Annotated[str, Field(max_length=128)]


class ErrorInfo(ReadModel):
    """Expose a stable safe error code, message and bounded field details."""

    code: str
    message: str
    details: Annotated[tuple[FieldError, ...], Field(max_length=32)] = ()


class ErrorEnvelope(ReadModel):
    """Bind a safe operation failure to the existing request identifier."""

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "error": {
                        "code": "CONFLICT",
                        "message": "Server version changed",
                        "details": [],
                    },
                    "request_id": "example-request-id",
                }
            ]
        }
    )
    error: ErrorInfo
    request_id: str
