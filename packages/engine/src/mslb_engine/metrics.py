"""Exact chart/comparison functions and compatible metric imports."""

from collections.abc import Iterable
from fractions import Fraction

from mslb_engine.errors import SimulationError
from mslb_engine.metric_accumulator import MetricAccumulator as MetricAccumulator
from mslb_engine.metric_values import ChartSample as ChartSample
from mslb_engine.metric_values import ComparisonIdentity as ComparisonIdentity
from mslb_engine.metric_values import Distribution as Distribution
from mslb_engine.metric_values import JsonValue as JsonValue
from mslb_engine.metric_values import ServerTotals as ServerTotals
from mslb_engine.metric_values import _bounded as _bounded
from mslb_engine.metric_values import distribution as distribution
from mslb_engine.metric_values import exact_value as exact_value


def chart_buckets(
    samples: Iterable[ChartSample],
    start: int,
    end: int,
    max_points: int = 2000,
) -> list[dict[str, JsonValue]]:
    """Stream exact time-weighted means/min/max without erasing observed peaks."""
    if not (0 <= start < end <= 1_000_000 and 1 <= max_points <= 2000):
        raise SimulationError("VALIDATION_ERROR", "Invalid chart range or point bound")
    width = (end - start + max_points - 1) // max_points
    buckets: dict[int, tuple[Fraction, int, Fraction, Fraction]] = {}
    previous_end = 0
    for index, sample in enumerate(samples):
        if index >= 1_000_000:
            raise SimulationError("RESOURCE_LIMIT_EXCEEDED", "Too many chart samples")
        if sample.t < previous_end or sample.duration_ticks < 1:
            raise SimulationError("VALIDATION_ERROR", "Chart samples must be ordered and disjoint")
        previous_end = sample.t + sample.duration_ticks
        value = _bounded(sample.value)
        cursor, stop = max(start, sample.t), min(end, previous_end)
        while cursor < stop:
            bucket = (cursor - start) // width
            duration = min(stop, start + (bucket + 1) * width) - cursor
            total, weight, minimum, maximum = buckets.get(bucket, (Fraction(), 0, value, value))
            buckets[bucket] = (
                _bounded(total + value * duration),
                weight + duration,
                min(minimum, value),
                max(maximum, value),
            )
            cursor += duration
    return [
        {
            "t": start + bucket * width,
            "end_tick": min(end, start + (bucket + 1) * width),
            "covered_ticks": weight,
            "min": exact_value(minimum),
            "max": exact_value(maximum),
            "mean": exact_value(total / weight),
        }
        for bucket, (total, weight, minimum, maximum) in sorted(buckets.items())
    ]


def compare_values(
    baseline_identity: ComparisonIdentity,
    candidate_identity: ComparisonIdentity,
    baseline: Fraction,
    candidate: Fraction,
) -> dict[str, JsonValue]:
    """Compare an explicit baseline only for compatible completed-run identities."""
    if baseline_identity != candidate_identity:
        raise SimulationError("CONFLICT", "Run inputs, modes, versions or semantic bounds differ")
    return {
        "baseline": exact_value(baseline),
        "candidate": exact_value(candidate),
        "difference": exact_value(candidate - baseline),
        "percentage_difference": exact_value((candidate - baseline) * 100 / baseline)
        if baseline
        else None,
    }
