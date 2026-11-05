import io
import json
from dataclasses import replace
from fractions import Fraction

import pytest
from mslb_engine.autoscaling import Autoscaler
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.models import (
    AutoscalingPolicy,
    Mode,
    Request,
    RunConfig,
    Server,
    ServerTemplate,
    SimulationError,
    Strategy,
    TickFrame,
)
from mslb_engine.replay import ReplayError, validate_trace
from mslb_engine.simulator import Simulation


def execute(
    requests: tuple[Request, ...], policy: AutoscalingPolicy
) -> tuple[list[TickFrame], RunConfig]:
    simulation = Simulation((Server("s1", 10, 1, 1),), requests, RunConfig(autoscaling=policy))
    frames = list(simulation.iter_ticks())
    trace = io.BytesIO(
        "".join(event.canonical_line() for frame in frames for event in frame.events).encode()
    )
    sidecar = io.BytesIO(
        "".join(
            frame.scale_decision.canonical_line()
            for frame in frames
            if frame.scale_decision is not None
        ).encode()
    )
    summary = validate_trace(
        trace, simulation.initial_servers, requests, simulation.config, sidecar
    )
    assert summary.finished + summary.dropped == len(requests)
    return frames, simulation.config


def test_queue_scaling_activates_next_tick_and_metrics_integrate_membership() -> None:
    requests = tuple(Request(0, str(index), 10, 1) for index in range(3))
    policy = AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 1, 1))
    frames, config = execute(requests, policy)
    decision = frames[0].scale_decision
    assert decision is not None and decision.trigger == ("queue_depth",)
    assert decision.applied_at == 1 and decision.server_ids_added == ("autoscale-000001",)
    assert len(frames[0].membership) == 1 and len(frames[1].membership) == 2
    assert frames[-1].terminal and frames[-1].scale_decision is None
    metrics = MetricAccumulator(requests, (Server("s1", 10, 1, 1),), config.max_metric_series)
    for frame in frames:
        metrics.observe(frame)
    cluster = metrics.summary()["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_consumed"] == cluster["cpu_capacity_ticks"] == 30


def test_cpu_and_queue_triggers_are_ordered_and_current_cpu_is_not_used() -> None:
    frames, _ = execute(
        (Request(0, "a", 30, 1), Request(1, "b", 10, 1)),
        AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 1, 1)),
    )
    assert frames[0].scale_decision is not None and frames[0].scale_decision.action == "none"
    assert frames[1].scale_decision is not None
    assert frames[1].scale_decision.trigger == ("cpu_mean", "queue_depth")


def test_drop_trigger_uses_completed_window_and_recent_drop_blocks_scale_in() -> None:
    frames, _ = execute(
        (Request(0, "drop", 1, 100), Request(8, "later", 1, 1)),
        AutoscalingPolicy(
            enabled=True,
            window_ticks=3,
            cooldown_ticks=0,
            scale_out_cpu_threshold=Fraction(1),
            scale_out_drop_rate_threshold=Fraction(1),
            max_servers=2,
            server_template=ServerTemplate(10, 1, 1),
        ),
    )
    decisions = [frame.scale_decision for frame in frames if frame.scale_decision is not None]
    assert decisions[0].action == "none"
    assert decisions[1].t == 1 and decisions[1].trigger == ("drop_rate_mean",)
    scale_in = [decision for decision in decisions if decision.action == "scale_in"]
    assert scale_in[0].t == 4 and scale_in[0].applied_at == 5
    assert len(frames[-1].membership) == 1 and frames[-1].membership[0].id == "s1"


def test_future_template_fit_is_queued_and_max_bound_can_make_it_infeasible() -> None:
    policy = AutoscalingPolicy(
        enabled=True, max_servers=2, server_template=ServerTemplate(10, 2, 1)
    )
    frames, _ = execute((Request(0, "large", 10, 2),), policy)
    assert not any(event.event == "REQUEST_DROPPED" for frame in frames for event in frame.events)
    starts = [
        event for frame in frames for event in frame.events if event.event == "REQUEST_STARTED"
    ]
    assert starts[0].t == 1 and starts[0].server_id == "autoscale-000001"
    frames, _ = execute((Request(0, "large", 10, 2),), replace(policy, max_servers=1))
    assert len(frames) == 1 and frames[0].terminal
    assert frames[0].events[-1].reason == "UNSCHEDULABLE_MEMORY"


def test_steps_clamp_bounds_and_only_idle_dynamic_servers_are_removed() -> None:
    frames, _ = execute(
        (Request(0, "large", 50, 2), Request(20, "later", 1, 1)),
        AutoscalingPolicy(
            enabled=True,
            window_ticks=1,
            cooldown_ticks=0,
            max_servers=3,
            scale_out_step=5,
            scale_in_step=5,
            scale_out_cpu_threshold=Fraction(1),
            scale_in_cpu_threshold=Fraction(9, 10),
            server_template=ServerTemplate(10, 2, 1),
        ),
    )
    decision = frames[0].scale_decision
    assert decision is not None
    assert decision.server_ids_added == ("autoscale-000001", "autoscale-000002")
    removals = [
        frame.scale_decision
        for frame in frames
        if frame.scale_decision is not None and frame.scale_decision.action == "scale_in"
    ]
    assert removals[0].server_ids_removed == ("autoscale-000002",)
    assert all("s1" not in change.server_ids_removed for change in removals)
    assert len(frames[-1].membership) == 1


def test_cooldown_suppresses_records_until_exact_boundary() -> None:
    frames, _ = execute(
        tuple(Request(0, str(index), 100, 1) for index in range(5)),
        AutoscalingPolicy(
            enabled=True, max_servers=4, cooldown_ticks=3, server_template=ServerTemplate(10, 1, 1)
        ),
    )
    assert frames[0].scale_decision is not None and frames[0].scale_decision.action == "scale_out"
    assert all(frame.scale_decision is None for frame in frames[1:4])
    assert frames[4].scale_decision is not None


def test_final_boundary_applies_pending_change_without_new_decision() -> None:
    requests = (Request(0, "a", 10, 1), Request(0, "b", 1, 100))
    frames, _ = execute(
        requests,
        AutoscalingPolicy(
            enabled=True,
            scale_out_drop_rate_threshold=Fraction(0),
            server_template=ServerTemplate(10, 1, 1),
        ),
    )
    assert frames[0].scale_decision is not None and frames[0].scale_decision.action == "scale_out"
    assert frames[-1].t == 1 and frames[-1].terminal
    assert len(frames[-1].membership) == 2 and frames[-1].scale_decision is None
    metrics = MetricAccumulator(requests, (Server("s1", 10, 1, 1),))
    for frame in frames:
        metrics.observe(frame)
    cluster = metrics.summary()["cluster"]
    assert isinstance(cluster, dict)
    assert cluster["cpu_units_per_tick"] == 20 and cluster["cpu_capacity_ticks"] == 10


def test_all_dropped_stops_even_with_zero_drop_threshold() -> None:
    frames, _ = execute(
        (Request(0, "a", 1, 1000),),
        AutoscalingPolicy(
            enabled=True,
            scale_out_drop_rate_threshold=Fraction(0),
            server_template=ServerTemplate(10, 1, 1),
        ),
    )
    assert len(frames) == 1 and frames[0].scale_decision is None


def test_sidecar_tampering_missing_or_trailing_data_is_rejected() -> None:
    requests = (Request(0, "large", 10, 2),)
    frames, config = execute(
        requests, AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 2, 1))
    )
    trace = "".join(event.canonical_line() for frame in frames for event in frame.events).encode()
    sidecar = "".join(
        frame.scale_decision.canonical_line()
        for frame in frames
        if frame.scale_decision is not None
    ).encode()
    for corrupted in (
        b"",
        sidecar.replace(b'"applied_at":1', b'"applied_at":2'),
        sidecar + b"{}\n",
    ):
        with pytest.raises(ReplayError):
            validate_trace(
                io.BytesIO(trace),
                (Server("s1", 10, 1, 1),),
                requests,
                config,
                io.BytesIO(corrupted),
            )
    with pytest.raises(ReplayError, match="sidecar"):
        validate_trace(io.BytesIO(trace), (Server("s1", 10, 1, 1),), requests, config)
    data = json.loads(sidecar.splitlines()[0])
    assert data["trigger"] == ["queue_depth"]


def test_policy_validation_and_reserved_identity_collisions() -> None:
    with pytest.raises(SimulationError, match="template"):
        AutoscalingPolicy(enabled=True)
    with pytest.raises(SimulationError, match="threshold"):
        AutoscalingPolicy(scale_in_cpu_threshold=Fraction(9, 10))
    with pytest.raises(SimulationError):
        AutoscalingPolicy(window_ticks=0)
    policy = AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 1, 1))
    with pytest.raises(SimulationError, match="bounds"):
        replace(policy, max_servers=1).resolved(2)
    with pytest.raises(SimulationError, match="namespace"):
        Simulation(
            (Server("autoscale-000001", 10, 1, 1),),
            (Request(0, "a", 1, 1),),
            RunConfig(autoscaling=policy),
        )


def test_metric_history_growth_has_a_frozen_controlled_bound() -> None:
    requests = (Request(0, "large", 10, 2),)
    frames, _ = execute(
        requests, AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 2, 1))
    )
    metrics = MetricAccumulator(requests, (Server("s1", 10, 1, 1),), max_series=1)
    metrics.observe(frames[0])
    with pytest.raises(SimulationError) as failure:
        metrics.observe(frames[1])
    assert failure.value.code == "RESOURCE_LIMIT_EXCEEDED"


def test_cpu_trigger_without_queue_and_max_bound_no_action() -> None:
    frames, _ = execute(
        (Request(0, "a", 50, 1),),
        AutoscalingPolicy(
            enabled=True,
            window_ticks=1,
            cooldown_ticks=0,
            max_servers=2,
            server_template=ServerTemplate(10, 1, 1),
        ),
    )
    assert frames[1].scale_decision is not None
    assert frames[1].scale_decision.trigger == ("cpu_mean",)
    assert frames[2].scale_decision is not None
    assert frames[2].scale_decision.action == "none" and frames[2].scale_decision.reason == "bounds"


@pytest.mark.parametrize("mode", ["concurrent", "sequential_compat"])
@pytest.mark.parametrize(
    "strategy", ["first_fit", "round_robin", "least_connections", "most_available_memory"]
)
def test_dynamic_workloads_replay_under_every_mode_and_strategy(
    mode: Mode, strategy: Strategy
) -> None:
    requests = tuple(Request(index // 3, str(index), index + 1, 1) for index in range(12))
    config = RunConfig(
        mode=mode,
        strategy=strategy,
        autoscaling=AutoscalingPolicy(
            enabled=True,
            window_ticks=2,
            cooldown_ticks=1,
            server_template=ServerTemplate(7, 3, 2),
        ),
    )
    simulation = Simulation((Server("s1", 10, 2, 2),), requests, config)
    frames = list(simulation.iter_ticks())
    trace = "".join(event.canonical_line() for frame in frames for event in frame.events).encode()
    sidecar = "".join(
        frame.scale_decision.canonical_line()
        for frame in frames
        if frame.scale_decision is not None
    ).encode()
    result = validate_trace(
        io.BytesIO(trace), simulation.initial_servers, requests, config, io.BytesIO(sidecar)
    )
    assert result.finished == 12


def test_replay_policy_does_not_call_runtime_autoscaler(monkeypatch: pytest.MonkeyPatch) -> None:
    requests = (Request(0, "large", 10, 2),)
    frames, config = execute(
        requests, AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 2, 1))
    )
    trace = "".join(event.canonical_line() for frame in frames for event in frame.events).encode()
    sidecar = "".join(
        frame.scale_decision.canonical_line()
        for frame in frames
        if frame.scale_decision is not None
    ).encode()

    def forbidden(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Runtime policy must not be replay's oracle")

    monkeypatch.setattr(Autoscaler, "decide", forbidden)
    assert (
        validate_trace(
            io.BytesIO(trace), (Server("s1", 10, 1, 1),), requests, config, io.BytesIO(sidecar)
        ).finished
        == 1
    )


def test_frozen_server_history_limit_fails_before_unbounded_series_growth() -> None:
    policy = AutoscalingPolicy(enabled=True, server_template=ServerTemplate(10, 2, 1))
    simulation = Simulation(
        (Server("s1", 10, 1, 1),),
        (Request(0, "large", 10, 2),),
        RunConfig(autoscaling=policy, max_metric_series=1),
    )
    with pytest.raises(SimulationError) as failure:
        list(simulation.iter_ticks())
    assert failure.value.code == "RESOURCE_LIMIT_EXCEEDED"


def test_queued_timeout_drop_to_arrival_ratio_can_exceed_one() -> None:
    requests = (Request(0, "a", 100, 1), Request(0, "b", 1, 1), Request(0, "c", 1, 1))
    config = RunConfig(
        queue_timeout_ticks=2,
        autoscaling=AutoscalingPolicy(
            enabled=True,
            max_servers=2,
            scale_out_queue_threshold=10000,
            scale_out_cpu_threshold=Fraction(1),
            server_template=ServerTemplate(10, 1, 1),
        ),
    )
    simulation = Simulation((Server("s1", 10, 1, 1),), requests, config)
    frames = []
    for frame in simulation.iter_ticks():
        frames.append(frame)
        if frame.t == 2:
            assert simulation.autoscaler.history[-1].drop_ratio == 2
    trace = "".join(event.canonical_line() for frame in frames for event in frame.events).encode()
    sidecar = "".join(
        frame.scale_decision.canonical_line()
        for frame in frames
        if frame.scale_decision is not None
    ).encode()
    assert (
        validate_trace(
            io.BytesIO(trace), simulation.initial_servers, requests, config, io.BytesIO(sidecar)
        ).dropped
        == 2
    )
