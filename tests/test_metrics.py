from dataclasses import replace
from fractions import Fraction

import pytest
from mslb_engine.metrics import (
    ChartSample,
    ComparisonIdentity,
    MetricAccumulator,
    chart_buckets,
    compare_values,
    distribution,
    exact_value,
)
from mslb_engine.models import Request, RunConfig, Server, SimulationError
from mslb_engine.simulator import Simulation


def test_nearest_rank_percentiles_and_empty_latency() -> None:
    values = distribution([1, 2, 3, 4, 5])
    assert values.mean == 3 and values.p50 == 3 and values.p95 == 5
    assert distribution([]).mean is None
    assert distribution([7]).p95 == 7


def test_counts_latency_consumption_and_capacity_match_hand_calculation() -> None:
    requests = (Request(0, "a", 1, 10), Request(0, "b", 10, 20))
    metrics = MetricAccumulator(requests, (Server("s", 10, 100, 2),))
    for frame in Simulation((Server("s", 10, 100, 2),), requests, RunConfig()).iter_ticks():
        metrics.observe(frame)
    report = metrics.summary()
    assert report["arrived"] == report["finished"] == 2
    assert report["makespan"] == 2 and report["completed_throughput"] == 1
    assert report["response"] == {
        "count": 2,
        "mean": {"numerator": "3", "denominator": "2"},
        "min": 1,
        "max": 2,
        "p50": 1,
        "p95": 2,
    }
    cluster = report["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_consumed"] == 11 and cluster["cpu_allocated"] == 20
    assert cluster["cpu_utilization"] == {"numerator": "11", "denominator": "20"}
    assert cluster["memory_reserved_mb_ticks"] == 50
    assert cluster["memory_utilization"] == {"numerator": "1", "denominator": "4"}


def test_idle_time_counts_in_integrated_capacity_but_not_makespan() -> None:
    requests = (Request(9, "a", 10, 1),)
    metrics = MetricAccumulator(requests, (Server("s", 10, 100, 2),))
    for frame in Simulation((Server("s", 10, 100, 1),), requests, RunConfig()).iter_ticks():
        metrics.observe(frame)
    report = metrics.summary()
    assert report["makespan"] == 1
    cluster = report["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_capacity_ticks"] == 100
    assert cluster["cpu_utilization"] == {"numerator": "1", "denominator": "10"}


def test_heterogeneous_cluster_utilization_is_capacity_weighted() -> None:
    requests = (Request(0, "a", 10, 1),)
    metrics = MetricAccumulator(requests, (Server("a", 10, 100, 1), Server("b", 90, 100, 1)))
    for frame in Simulation(
        (Server("a", 10, 100, 1), Server("b", 90, 100, 1)), requests, RunConfig()
    ).iter_ticks():
        metrics.observe(frame)
    cluster = metrics.summary()["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_utilization"] == {"numerator": "1", "denominator": "10"}


def test_all_dropped_is_successful_with_null_latencies_and_zero_throughput() -> None:
    requests = (Request(0, "a", 10, 1000),)
    metrics = MetricAccumulator(requests, (Server("s", 10, 100, 2),))
    for frame in Simulation((Server("s", 10, 100, 1),), requests, RunConfig()).iter_ticks():
        metrics.observe(frame)
    report = metrics.summary()
    assert report["completed_throughput"] == 0 and report["drop_fraction"] == 1
    assert report["waiting"] == {
        "count": 0,
        "mean": None,
        "min": None,
        "max": None,
        "p50": None,
        "p95": None,
    }
    servers = report["servers"]
    assert isinstance(servers, dict)
    server = servers["s"]
    assert isinstance(server, dict)
    assert server["started"] == server["finished"] == 0
    assert server["mem_mb"] == 100


def test_incomplete_or_repeated_metric_boundaries_fail() -> None:
    requests = (Request(0, "a", 10, 1),)
    metrics = MetricAccumulator(requests, (Server("s", 10, 100, 2),))
    with pytest.raises(SimulationError, match="terminal"):
        metrics.summary()
    frame = next(Simulation((Server("s", 10, 100, 1),), requests, RunConfig()).iter_ticks())
    metrics.observe(frame)
    with pytest.raises(SimulationError, match="contiguous"):
        metrics.observe(frame)


def test_chart_buckets_preserve_peaks_and_time_weighted_means() -> None:
    points = chart_buckets(
        iter([ChartSample(0, 9, Fraction()), ChartSample(9, 1, Fraction(1))]), 0, 10, 1
    )
    assert points == [
        {
            "t": 0,
            "end_tick": 10,
            "covered_ticks": 10,
            "min": 0,
            "max": 1,
            "mean": {"numerator": "1", "denominator": "10"},
        }
    ]
    assert len(chart_buckets([ChartSample(0, 1_000_000, Fraction())], 0, 1_000_000)) == 2000


def test_chart_does_not_invent_samples_for_membership_gaps() -> None:
    assert chart_buckets([ChartSample(5, 1, Fraction(1))], 0, 10, 10) == [
        {"t": 5, "end_tick": 6, "covered_ticks": 1, "min": 1, "max": 1, "mean": 1}
    ]
    with pytest.raises(SimulationError):
        chart_buckets([ChartSample(1, 3, Fraction()), ChartSample(2, 1, Fraction())], 0, 5)


def test_comparison_requires_compatible_identities_and_defines_zero_baseline() -> None:
    identity = ComparisonIdentity("dataset", "servers", "concurrent", "engine", "bounds")
    assert (
        compare_values(identity, identity, Fraction(2), Fraction(3))["percentage_difference"] == 50
    )
    assert (
        compare_values(identity, identity, Fraction(), Fraction(3))["percentage_difference"] is None
    )
    with pytest.raises(SimulationError) as failure:
        compare_values(
            identity, replace(identity, dataset_digest="different"), Fraction(2), Fraction(3)
        )
    assert failure.value.code == "CONFLICT"


def test_integrated_quantities_above_javascript_safe_integer_remain_exact() -> None:
    large = Fraction(64 * 1_000_000_000 * 999_999)
    assert exact_value(large) == {"numerator": str(large.numerator), "denominator": "1"}


def test_latency_population_and_chart_query_limits_are_enforced() -> None:
    with pytest.raises(SimulationError):
        distribution([0] * 10001)
    for start, end, points in [(0, 0, 1), (-1, 2, 1), (0, 2, 2001), (0, 2, 0)]:
        with pytest.raises(SimulationError):
            chart_buckets([], start, end, points)


def test_queue_waiting_and_per_server_distribution_include_only_starts() -> None:
    requests = (Request(0, "a", 20, 100), Request(0, "b", 10, 1), Request(0, "c", 1, 1000))
    servers = (Server("s", 10, 100, 1),)
    metrics = MetricAccumulator(requests, servers)
    for frame in Simulation(servers, requests, RunConfig()).iter_ticks():
        metrics.observe(frame)
    report = metrics.summary()
    assert report["waiting"] == {"count": 2, "mean": 1, "min": 0, "max": 2, "p50": 0, "p95": 2}
    assert report["queue_depth_peak"] == 1 and report["queue_depth_integral"] == 2
    assert report["dropped"] == 1
    servers_data = report["servers"]
    assert isinstance(servers_data, dict)
    server = servers_data["s"]
    assert isinstance(server, dict)
    assert server["waiting"] == report["waiting"]
