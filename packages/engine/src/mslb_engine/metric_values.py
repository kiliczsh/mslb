"""Exact bounded metric values, distributions and per-server totals."""

from collections.abc import Sequence
from dataclasses import dataclass, field
from fractions import Fraction

from mslb_engine.errors import SimulationError

type JsonValue = None | bool | int | str | list[JsonValue] | dict[str, JsonValue]


def _bounded(value: Fraction) -> Fraction:
    if max(value.numerator.bit_length(), value.denominator.bit_length()) > 4096:
        raise SimulationError(
            code="RESOURCE_LIMIT_EXCEEDED", message="Metric arithmetic exceeds 4096 bits"
        )
    return value


def exact_value(value: Fraction | None) -> JsonValue:
    """Encode authoritative rational values with lossless decimal components."""
    if value is None:
        return None
    _bounded(value=value)
    if value.denominator == 1 and abs(value.numerator) <= 2**53 - 1:
        return value.numerator
    return {"numerator": str(object=value.numerator), "denominator": str(object=value.denominator)}


@dataclass(frozen=True, slots=True)
class Distribution:
    """Report exact latency statistics without floating-point rounding."""

    count: int
    mean: Fraction | None
    minimum: int | None
    maximum: int | None
    p50: int | None
    p95: int | None

    def to_data(self) -> dict[str, JsonValue]:
        return {
            "count": self.count,
            "mean": exact_value(value=self.mean),
            "min": self.minimum,
            "max": self.maximum,
            "p50": self.p50,
            "p95": self.p95,
        }


def distribution(values: Sequence[int], *, max_values: int = 10_000) -> Distribution:
    """Nearest-rank percentiles over at most the admitted request population."""
    if type(max_values) is not int or max_values not in {10_000, 50_000}:
        raise SimulationError(code="VALIDATION_ERROR", message="Unsupported latency sample bound")
    if len(values) > max_values:
        raise SimulationError(code="RESOURCE_LIMIT_EXCEEDED", message="Too many latency samples")
    if any(type(value) is not int or value < 0 for value in values):
        raise SimulationError(
            code="VALIDATION_ERROR", message="Latency samples must be nonnegative ticks"
        )
    if not values:
        return Distribution(count=0, mean=None, minimum=None, maximum=None, p50=None, p95=None)
    ordered = sorted(values)
    count = len(ordered)
    return Distribution(
        count=count,
        mean=Fraction(numerator=sum(ordered), denominator=count),
        minimum=ordered[0],
        maximum=ordered[-1],
        p50=ordered[((50 * count + 99) // 100) - 1],
        p95=ordered[((95 * count + 99) // 100) - 1],
    )


@dataclass(slots=True)
class ServerTotals:
    """Accumulate exact per-server resource integrals and request outcomes."""

    cpu_units_per_tick: int = 0
    mem_mb: int = 0
    waits: list[int] = field(default_factory=list)
    responses: list[int] = field(default_factory=list)
    executions: list[int] = field(default_factory=list)
    started: int = 0
    finished: int = 0
    cpu_allocated: Fraction = field(default_factory=Fraction)
    cpu_consumed: Fraction = field(default_factory=Fraction)
    cpu_capacity_ticks: int = 0
    memory_reserved_mb_ticks: int = 0
    memory_capacity_mb_ticks: int = 0

    def to_data(self, *, max_requests: int = 10_000) -> dict[str, JsonValue]:
        """Encode exact integrals and latency summaries into canonical metric values."""
        return {
            "cpu_units_per_tick": self.cpu_units_per_tick,
            "mem_mb": self.mem_mb,
            "started": self.started,
            "finished": self.finished,
            "waiting": distribution(values=self.waits, max_values=max_requests).to_data(),
            "response": distribution(values=self.responses, max_values=max_requests).to_data(),
            "execution": distribution(values=self.executions, max_values=max_requests).to_data(),
            "cpu_allocated": exact_value(value=self.cpu_allocated),
            "cpu_consumed": exact_value(value=self.cpu_consumed),
            "cpu_capacity_ticks": exact_value(value=Fraction(numerator=self.cpu_capacity_ticks)),
            "cpu_utilization": exact_value(
                value=self.cpu_consumed / self.cpu_capacity_ticks
                if self.cpu_capacity_ticks
                else Fraction()
            ),
            "memory_reserved_mb_ticks": exact_value(
                value=Fraction(numerator=self.memory_reserved_mb_ticks)
            ),
            "memory_capacity_mb_ticks": exact_value(
                value=Fraction(numerator=self.memory_capacity_mb_ticks)
            ),
            "memory_utilization": exact_value(
                value=Fraction(
                    numerator=self.memory_reserved_mb_ticks,
                    denominator=self.memory_capacity_mb_ticks,
                )
                if self.memory_capacity_mb_ticks
                else Fraction()
            ),
        }


@dataclass(frozen=True, slots=True)
class ChartSample:
    t: int
    duration_ticks: int
    value: Fraction


@dataclass(frozen=True, slots=True)
class ComparisonIdentity:
    """Identify the frozen dimensions that must match before comparing strategies."""

    dataset_digest: str
    server_snapshot_digest: str
    mode: str
    engine_version: str
    noncompared_settings_digest: str
