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
    values = distribution(values=[1, 2, 3, 4, 5])
    assert values.mean == 3 and values.p50 == 3 and values.p95 == 5
    assert distribution(values=[]).mean is None
    assert distribution(values=[7]).p95 == 7


def test_counts_latency_consumption_and_capacity_match_hand_calculation() -> None:
    requests = (
        Request(t=0, request_id="a", work_units=1, mem_mb=10),
        Request(t=0, request_id="b", work_units=10, mem_mb=20),
    )
    metrics = MetricAccumulator(
        requests=requests,
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=2),),
    )
    for frame in Simulation(
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=2),),
        requests=requests,
        config=RunConfig(mode="concurrent"),
    ).iter_ticks():
        metrics.observe(frame=frame)
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
    requests = (Request(t=9, request_id="a", work_units=10, mem_mb=1),)
    metrics = MetricAccumulator(
        requests=requests,
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=2),),
    )
    for frame in Simulation(
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=1),),
        requests=requests,
        config=RunConfig(),
    ).iter_ticks():
        metrics.observe(frame=frame)
    report = metrics.summary()
    assert report["makespan"] == 1
    cluster = report["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_capacity_ticks"] == 100
    assert cluster["cpu_utilization"] == {"numerator": "1", "denominator": "10"}


def test_heterogeneous_cluster_utilization_is_capacity_weighted() -> None:
    requests = (Request(t=0, request_id="a", work_units=10, mem_mb=1),)
    metrics = MetricAccumulator(
        requests=requests,
        servers=(
            Server(id="a", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=1),
            Server(id="b", cpu_units_per_tick=90, mem_mb=100, rate_limit_per_sec=1),
        ),
    )
    for frame in Simulation(
        servers=(
            Server(id="a", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=1),
            Server(id="b", cpu_units_per_tick=90, mem_mb=100, rate_limit_per_sec=1),
        ),
        requests=requests,
        config=RunConfig(),
    ).iter_ticks():
        metrics.observe(frame=frame)
    cluster = metrics.summary()["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_utilization"] == {"numerator": "1", "denominator": "10"}


def test_all_dropped_is_successful_with_null_latencies_and_zero_throughput() -> None:
    requests = (Request(t=0, request_id="a", work_units=10, mem_mb=1000),)
    metrics = MetricAccumulator(
        requests=requests,
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=2),),
    )
    for frame in Simulation(
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=1),),
        requests=requests,
        config=RunConfig(),
    ).iter_ticks():
        metrics.observe(frame=frame)
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
    requests = (Request(t=0, request_id="a", work_units=10, mem_mb=1),)
    metrics = MetricAccumulator(
        requests=requests,
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=2),),
    )
    with pytest.raises(expected_exception=SimulationError, match="terminal"):
        metrics.summary()
    frame = next(
        Simulation(
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=1),),
            requests=requests,
            config=RunConfig(),
        ).iter_ticks()
    )
    metrics.observe(frame=frame)
    with pytest.raises(expected_exception=SimulationError, match="contiguous"):
        metrics.observe(frame=frame)


def test_chart_buckets_preserve_peaks_and_time_weighted_means() -> None:
    points = chart_buckets(
        samples=iter(
            [
                ChartSample(t=0, duration_ticks=9, value=Fraction()),
                ChartSample(t=9, duration_ticks=1, value=Fraction(numerator=1)),
            ]
        ),
        start=0,
        end=10,
        max_points=1,
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
    assert (
        len(
            chart_buckets(
                samples=[ChartSample(t=0, duration_ticks=1_000_000, value=Fraction())],
                start=0,
                end=1_000_000,
            )
        )
        == 2000
    )


def test_chart_does_not_invent_samples_for_membership_gaps() -> None:
    assert chart_buckets(
        samples=[ChartSample(t=5, duration_ticks=1, value=Fraction(numerator=1))],
        start=0,
        end=10,
        max_points=10,
    ) == [{"t": 5, "end_tick": 6, "covered_ticks": 1, "min": 1, "max": 1, "mean": 1}]
    with pytest.raises(expected_exception=SimulationError):
        chart_buckets(
            samples=[
                ChartSample(t=1, duration_ticks=3, value=Fraction()),
                ChartSample(t=2, duration_ticks=1, value=Fraction()),
            ],
            start=0,
            end=5,
        )


def test_comparison_requires_compatible_identities_and_defines_zero_baseline() -> None:
    identity = ComparisonIdentity(
        dataset_digest="dataset",
        server_snapshot_digest="servers",
        mode="concurrent",
        engine_version="engine",
        noncompared_settings_digest="bounds",
    )
    assert (
        compare_values(
            baseline_identity=identity,
            candidate_identity=identity,
            baseline=Fraction(numerator=2),
            candidate=Fraction(numerator=3),
        )["percentage_difference"]
        == 50
    )
    assert (
        compare_values(
            baseline_identity=identity,
            candidate_identity=identity,
            baseline=Fraction(),
            candidate=Fraction(numerator=3),
        )["percentage_difference"]
        is None
    )
    with pytest.raises(expected_exception=SimulationError) as failure:
        compare_values(
            baseline_identity=identity,
            candidate_identity=replace(identity, dataset_digest="different"),
            baseline=Fraction(numerator=2),
            candidate=Fraction(numerator=3),
        )
    assert failure.value.code == "CONFLICT"


def test_integrated_quantities_above_javascript_safe_integer_remain_exact() -> None:
    large = Fraction(numerator=64 * 1_000_000_000 * 999_999)
    assert exact_value(value=large) == {
        "numerator": str(object=large.numerator),
        "denominator": "1",
    }


def test_latency_population_and_chart_query_limits_are_enforced() -> None:
    with pytest.raises(expected_exception=SimulationError):
        distribution(values=[0] * 10001)
    for start, end, points in [(0, 0, 1), (-1, 2, 1), (0, 2, 2001), (0, 2, 0)]:
        with pytest.raises(expected_exception=SimulationError):
            chart_buckets(samples=[], start=start, end=end, max_points=points)


def test_queue_waiting_and_per_server_distribution_include_only_starts() -> None:
    requests = (
        Request(t=0, request_id="a", work_units=20, mem_mb=100),
        Request(t=0, request_id="b", work_units=10, mem_mb=1),
        Request(t=0, request_id="c", work_units=1, mem_mb=1000),
    )
    servers = (Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=1),)
    metrics = MetricAccumulator(requests=requests, servers=servers)
    for frame in Simulation(servers=servers, requests=requests, config=RunConfig()).iter_ticks():
        metrics.observe(frame=frame)
    report = metrics.summary()
    assert report["waiting"] == {"count": 2, "mean": 1, "min": 0, "max": 2, "p50": 0, "p95": 2}
    assert report["queue_depth_peak"] == 1 and report["queue_depth_integral"] == 2
    assert report["dropped"] == 1
    servers_data = report["servers"]
    assert isinstance(servers_data, dict)
    server = servers_data["s"]
    assert isinstance(server, dict)
    assert server["waiting"] == report["waiting"]
