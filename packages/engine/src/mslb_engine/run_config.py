"""Frozen logical execution and simulated scaling policies."""

from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Literal, Self

from mslb_engine.errors import SimulationError
from mslb_engine.validation import _integer

type Mode = Literal["concurrent", "sequential"]


type Strategy = Literal["first_fit", "round_robin", "least_connections", "most_available_memory"]


STRATEGIES = ("first_fit", "round_robin", "least_connections", "most_available_memory")


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
