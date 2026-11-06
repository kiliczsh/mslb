"""Real-time traffic admission/outcomes, separate from deterministic logical events."""

from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from mslb_contracts.docker_runs import DockerModel
from mslb_contracts.traffic import WorkCompleted

type StopReason = Literal["CANCELLED", "FENCE_LOST", "RESOURCE_PRESSURE", "RUN_DEADLINE"]
type HttpReason = Literal[
    "CANCELLED",
    "FENCE_LOST",
    "RESOURCE_PRESSURE",
    "RUN_DEADLINE",
    "DRAIN_TIMEOUT",
    "PENDING_TIMEOUT",
    "PENDING_OVERFLOW",
    "UNSCHEDULABLE_MEMORY",
    "HTTP_TIMEOUT",
    "HTTP_STATUS",
    "CONNECTION_FAILED",
    "INVALID_RESPONSE",
    "HTTP_FAILURE",
]


class HttpEvent(DockerModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    mode: Literal["docker"] = "docker"
    sequence: Annotated[int, Field(ge=1, le=30_000)]
    kind: Literal["arrived", "admitted", "completed", "failed", "dropped", "unexecuted"]
    elapsed_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    request_id: Annotated[str, Field(min_length=1, max_length=256)]
    backend_id: Annotated[str, Field(min_length=1, max_length=128)] | None = None
    reason: HttpReason | None = None
    result: WorkCompleted | None = None

    @model_validator(mode="after")
    def outcome(self) -> Self:
        if self.kind == "completed":
            if (
                self.result is None
                or self.result.request_id != self.request_id
                or self.result.backend_id != self.backend_id
                or self.reason is not None
            ):
                raise ValueError("Completed HTTP outcome requires its actual matching response")
        elif self.result is not None:
            raise ValueError("Only completed HTTP work has a response")
        if (self.kind in {"failed", "dropped", "unexecuted"}) != (self.reason is not None):
            raise ValueError("Terminal HTTP refusal/failure requires an explicit reason")
        if self.kind in {"admitted", "completed", "failed"} and self.backend_id is None:
            raise ValueError("Admitted HTTP work requires a backend identity")
        return self


class TrafficSummary(DockerModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    mode: Literal["docker"] = "docker"
    status: Literal["completed", "cancelled", "failed"]
    stop_reason: StopReason | None
    total: Annotated[int, Field(ge=1, le=10_000)]
    completed: Annotated[int, Field(ge=0, le=10_000)]
    failed: Annotated[int, Field(ge=0, le=10_000)]
    dropped: Annotated[int, Field(ge=0, le=10_000)]
    unexecuted: Annotated[int, Field(ge=0, le=10_000)]
    elapsed_seconds: Annotated[float, Field(ge=0, allow_inf_nan=False)]
    peak_pending: Annotated[int, Field(ge=0, le=256)]
    peak_inflight: Annotated[int, Field(ge=0, le=16)]

    @model_validator(mode="after")
    def accounting(self) -> Self:
        if self.completed + self.failed + self.dropped + self.unexecuted != self.total:
            raise ValueError("Every HTTP request requires exactly one terminal outcome")
        if (
            self.status == "cancelled"
            and self.stop_reason != "CANCELLED"
            or self.status == "failed"
            and self.stop_reason not in {"FENCE_LOST", "RESOURCE_PRESSURE"}
            or self.status == "completed"
            and self.stop_reason not in {None, "RUN_DEADLINE"}
        ):
            raise ValueError("HTTP run status and stop reason conflict")
        return self
