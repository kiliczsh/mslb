"""Frozen logical execution and simulated scaling policies."""

from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Literal, Self

from mslb_engine.errors import SimulationError
from mslb_engine.validation import _integer, _request_limit

type Mode = Literal["concurrent", "sequential"]


type Strategy = Literal["first_fit", "round_robin", "least_connections", "most_available_memory"]


STRATEGIES = ("first_fit", "round_robin", "least_connections", "most_available_memory")


@dataclass(frozen=True, slots=True)
class ServerTemplate:
    """Freeze capacities used to create deterministic simulated servers."""

    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int

    def __post_init__(self) -> None:
        for value in (self.cpu_units_per_tick, self.mem_mb, self.rate_limit_per_sec):
            _integer(value=value, minimum=1, maximum=1_000_000_000)


@dataclass(frozen=True, slots=True)
class AutoscalingPolicy:
    """Bound exact simulated scaling thresholds, windows and membership changes."""

    enabled: bool = False
    window_ticks: int = 5
    scale_out_cpu_threshold: Fraction = Fraction(numerator=4, denominator=5)
    scale_in_cpu_threshold: Fraction = Fraction(numerator=1, denominator=5)
    scale_out_queue_threshold: int = 1
    scale_out_drop_rate_threshold: Fraction = Fraction(numerator=1, denominator=10)
    cooldown_ticks: int = 3
    min_servers: int | None = None
    max_servers: int | None = None
    scale_out_step: int = 1
    scale_in_step: int = 1
    server_template: ServerTemplate | None = None

    def __post_init__(self) -> None:
        """Validate exact thresholds, finite bounds and enabled-template requirements."""
        if type(self.enabled) is not bool:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Scaling enabled must be boolean"
            )
        _integer(value=self.window_ticks, minimum=1, maximum=10_000)
        _integer(value=self.cooldown_ticks, minimum=0, maximum=1_000_000)
        _integer(value=self.scale_out_queue_threshold, minimum=0, maximum=10_000)
        _integer(value=self.scale_out_step, minimum=1, maximum=64)
        _integer(value=self.scale_in_step, minimum=1, maximum=64)
        for bound in (self.min_servers, self.max_servers):
            if bound is not None:
                _integer(value=bound, minimum=1, maximum=64)
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
                    code="VALIDATION_ERROR",
                    message="Scaling thresholds must be bounded exact ratios",
                )
        if self.scale_in_cpu_threshold >= self.scale_out_cpu_threshold:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Scale-in CPU threshold must be below scale-out"
            )
        if self.enabled and self.server_template is None:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Enabled scaling requires a frozen template"
            )

    def resolved(self, initial_count: int) -> Self:
        """Expand input-dependent defaults; this contains no runtime decisions."""
        minimum = self.min_servers if self.min_servers is not None else initial_count
        maximum = self.max_servers if self.max_servers is not None else min(64, initial_count + 3)
        if not 1 <= minimum <= initial_count <= maximum <= 64:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Scaling bounds conflict with the initial cluster"
            )
        return replace(self, min_servers=minimum, max_servers=maximum)


@dataclass(frozen=True, slots=True)
class RunConfig:
    """Freeze deterministic execution policy and supported arithmetic/sample limits."""

    mode: Mode = "sequential"
    strategy: Strategy = "first_fit"
    queue_timeout_ticks: int | None = None
    max_queue_size: int | None = None
    max_simulation_ticks: int = 100_000
    max_samples: int = 1_000_000
    max_fraction_bits: int = 4096
    max_metric_series: int = 10_000
    autoscaling: AutoscalingPolicy = AutoscalingPolicy()
    max_requests: int = 10_000

    def __post_init__(self) -> None:
        _request_limit(value=self.max_requests)
        if self.mode not in ("concurrent", "sequential") or self.strategy not in STRATEGIES:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Unknown logical mode or strategy"
            )
        if self.queue_timeout_ticks is not None:
            _integer(value=self.queue_timeout_ticks, minimum=1, maximum=1_000_000)
        if self.max_queue_size is not None:
            _integer(value=self.max_queue_size, minimum=0, maximum=10_000)
        _integer(value=self.max_simulation_ticks, minimum=1, maximum=1_000_000)
        _integer(value=self.max_samples, minimum=1, maximum=1_000_000)
        _integer(value=self.max_fraction_bits, minimum=1, maximum=4096)
        _integer(value=self.max_metric_series, minimum=1, maximum=10_000)
