"""Immutable engine inputs, events, and bounded interval observations."""

import json
import unicodedata
from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Literal, Self

type Mode = Literal["concurrent", "sequential"]
type Strategy = Literal["first_fit", "round_robin", "least_connections", "most_available_memory"]
type EventKind = Literal[
    "REQUEST_FINISHED", "REQUEST_ARRIVED", "REQUEST_DROPPED", "REQUEST_STARTED"
]

EVENT_PRIORITY = {
    "REQUEST_FINISHED": 0,
    "REQUEST_ARRIVED": 1,
    "REQUEST_DROPPED": 2,
    "REQUEST_STARTED": 3,
}
STRATEGIES = ("first_fit", "round_robin", "least_connections", "most_available_memory")


class SimulationError(ValueError):
    """Typed deterministic failure; partial output is never a final artifact."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _integer(value: int, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise SimulationError("VALIDATION_ERROR", "Integer is outside the supported bounds")


def _identifier(value: str) -> None:
    if not isinstance(value, str) or not 1 <= len(value.encode("utf-8")) <= 128:
        raise SimulationError("VALIDATION_ERROR", "Invalid identifier")
    if unicodedata.normalize("NFC", value.strip()) != value:
        raise SimulationError("VALIDATION_ERROR", "Input identifiers must already be normalized")


@dataclass(frozen=True, slots=True)
class Server:
    id: str
    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int

    def __post_init__(self) -> None:
        _identifier(self.id)
        for value in (self.cpu_units_per_tick, self.mem_mb, self.rate_limit_per_sec):
            _integer(value, 1, 1_000_000_000)


@dataclass(frozen=True, slots=True)
class Request:
    t: int
    request_id: str
    work_units: int
    mem_mb: int

    def __post_init__(self) -> None:
        _identifier(self.request_id)
        _integer(self.t, 0, 999_999)
        _integer(self.work_units, 1, 1_000_000_000)
        _integer(self.mem_mb, 1, 1_000_000_000)


@dataclass(frozen=True, slots=True)
class ServerTemplate:
    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int

    def __post_init__(self) -> None:
        for value in (self.cpu_units_per_tick, self.mem_mb, self.rate_limit_per_sec):
            _integer(value, 1, 1_000_000_000)


@dataclass(frozen=True, slots=True)
class AutoscalingPolicy:
    enabled: bool = False
    window_ticks: int = 5
    scale_out_cpu_threshold: Fraction = Fraction(4, 5)
    scale_in_cpu_threshold: Fraction = Fraction(1, 5)
    scale_out_queue_threshold: int = 1
    scale_out_drop_rate_threshold: Fraction = Fraction(1, 10)
    cooldown_ticks: int = 3
    min_servers: int | None = None
    max_servers: int | None = None
    scale_out_step: int = 1
    scale_in_step: int = 1
    server_template: ServerTemplate | None = None

    def __post_init__(self) -> None:
        if type(self.enabled) is not bool:
            raise SimulationError("VALIDATION_ERROR", "Scaling enabled must be boolean")
        _integer(self.window_ticks, 1, 10_000)
        _integer(self.cooldown_ticks, 0, 1_000_000)
        _integer(self.scale_out_queue_threshold, 0, 10_000)
        _integer(self.scale_out_step, 1, 64)
        _integer(self.scale_in_step, 1, 64)
        for bound in (self.min_servers, self.max_servers):
            if bound is not None:
                _integer(bound, 1, 64)
        for threshold in (
            self.scale_out_cpu_threshold,
            self.scale_in_cpu_threshold,
            self.scale_out_drop_rate_threshold,
        ):
            if (
                not isinstance(threshold, Fraction)
                or not 0 <= threshold <= 1
                or max(threshold.numerator.bit_length(), threshold.denominator.bit_length()) > 4096
            ):
                raise SimulationError(
                    "VALIDATION_ERROR", "Scaling thresholds must be bounded exact ratios"
                )
        if self.scale_in_cpu_threshold >= self.scale_out_cpu_threshold:
            raise SimulationError(
                "VALIDATION_ERROR", "Scale-in CPU threshold must be below scale-out"
            )
        if self.enabled and self.server_template is None:
            raise SimulationError("VALIDATION_ERROR", "Enabled scaling requires a frozen template")

    def resolved(self, initial_count: int) -> Self:
        """Expand input-dependent defaults; this contains no runtime decisions."""
        minimum = self.min_servers if self.min_servers is not None else initial_count
        maximum = self.max_servers if self.max_servers is not None else min(64, initial_count + 3)
        if not 1 <= minimum <= initial_count <= maximum <= 64:
            raise SimulationError(
                "VALIDATION_ERROR", "Scaling bounds conflict with the initial cluster"
            )
        return replace(self, min_servers=minimum, max_servers=maximum)


@dataclass(frozen=True, slots=True)
class ScaleDecision:
    t: int
    action: str
    trigger: tuple[str, ...]
    applied_at: int | None
    server_ids_added: tuple[str, ...]
    server_ids_removed: tuple[str, ...]
    reason: str | None

    def to_data(self) -> dict[str, object]:
        return {
            "t": self.t,
            "action": self.action,
            "trigger": list(self.trigger),
            "applied_at": self.applied_at,
            "server_ids_added": list(self.server_ids_added),
            "server_ids_removed": list(self.server_ids_removed),
            "reason": self.reason,
        }

    def canonical_line(self) -> str:
        return json.dumps(self.to_data(), ensure_ascii=False, separators=(",", ":")) + "\n"


@dataclass(frozen=True, slots=True)
class RunConfig:
    mode: Mode = "sequential"
    strategy: Strategy = "first_fit"
    queue_timeout_ticks: int | None = None
    max_queue_size: int | None = None
    max_simulation_ticks: int = 100_000
    max_samples: int = 1_000_000
    max_fraction_bits: int = 4096
    max_metric_series: int = 10_000
    autoscaling: AutoscalingPolicy = AutoscalingPolicy()

    def __post_init__(self) -> None:
        if self.mode not in ("concurrent", "sequential") or self.strategy not in STRATEGIES:
            raise SimulationError("VALIDATION_ERROR", "Unknown logical mode or strategy")
        if self.queue_timeout_ticks is not None:
            _integer(self.queue_timeout_ticks, 1, 1_000_000)
        if self.max_queue_size is not None:
            _integer(self.max_queue_size, 0, 10_000)
        _integer(self.max_simulation_ticks, 1, 1_000_000)
        _integer(self.max_samples, 1, 1_000_000)
        _integer(self.max_fraction_bits, 1, 4096)
        _integer(self.max_metric_series, 1, 10_000)


@dataclass(frozen=True, slots=True)
class Event:
    t: int
    event: EventKind
    request_id: str
    server_id: str | None = None
    reason: str | None = None

    def canonical_line(self) -> str:
        """Serialize evaluator-facing keys only, including a final LF."""
        value: dict[str, int | str] = {
            "t": self.t,
            "event": self.event,
            "request_id": self.request_id,
        }
        if self.server_id is not None:
            value["server_id"] = self.server_id
        return json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n"


@dataclass(frozen=True, slots=True)
class ServerSample:
    server_id: str
    capacity: int
    memory_capacity: int
    active_ids: tuple[str, ...]
    reserved_mem_mb: int
    allocated_cpu: Fraction
    consumed_cpu: Fraction
    starts: int


@dataclass(frozen=True, slots=True)
class TickFrame:
    t: int
    duration_ticks: int
    events: tuple[Event, ...]
    pending_ids: tuple[str, ...]
    samples: tuple[ServerSample, ...]
    terminal: bool = False
    membership: tuple[Server, ...] = ()
    scale_decision: ScaleDecision | None = None
