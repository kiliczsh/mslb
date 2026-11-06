import io
import json
import os
import subprocess
import sys
from fractions import Fraction
from pathlib import Path

import pytest
from mslb_contracts.inputs import read_requests, read_servers
from mslb_engine.models import (
    AutoscalingPolicy,
    Event,
    Mode,
    Request,
    RunConfig,
    Server,
    ServerTemplate,
    SimulationError,
    Strategy,
)
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation
from pydantic import TypeAdapter

from scripts.generate_example import generate


def events_for(requests: tuple[Request, ...], config: RunConfig | None = None) -> list[Event]:
    simulation = Simulation(
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=10),),
        requests=requests,
        config=config or RunConfig(mode="concurrent"),
    )
    return [event for frame in simulation.iter_ticks() for event in frame.events]


def finish_ticks(events: list[Event]) -> dict[str, int]:
    return {event.request_id: event.t for event in events if event.event == "REQUEST_FINISHED"}


def test_equal_fractional_sharing_and_consumption() -> None:
    requests = tuple(
        Request(t=0, request_id=str(index), work_units=10, mem_mb=1) for index in range(3)
    )
    frames = list(
        Simulation(
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=10),),
            requests=requests,
            config=RunConfig(mode="concurrent"),
        ).iter_ticks()
    )
    assert finish_ticks(events=[event for frame in frames for event in frame.events]) == {
        "0": 3,
        "1": 3,
        "2": 3,
    }
    assert frames[0].samples[0].consumed_cpu == 10
    assert frames[-1].terminal and frames[-1].duration_ticks == 0
    assert frames[-1].samples == ()


def test_unused_cpu_is_not_redistributed_and_shares_change_next_interval() -> None:
    frames = list(
        Simulation(
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=10),),
            requests=(
                Request(t=0, request_id="a", work_units=1, mem_mb=1),
                Request(t=0, request_id="b", work_units=10, mem_mb=1),
            ),
            config=RunConfig(mode="concurrent"),
        ).iter_ticks()
    )
    assert frames[0].samples[0].consumed_cpu == 6
    assert frames[0].samples[0].allocated_cpu == 10
    assert finish_ticks(events=[e for f in frames for e in f.events]) == {"a": 1, "b": 2}


def test_midflight_arrival_changes_subsequent_shares() -> None:
    events = events_for(
        requests=(
            Request(t=0, request_id="a", work_units=20, mem_mb=1),
            Request(t=1, request_id="b", work_units=10, mem_mb=1),
        )
    )
    assert finish_ticks(events=events) == {"a": 3, "b": 3}


def test_sequential_boundary_reuse_and_canonical_order() -> None:
    events = events_for(
        requests=(
            Request(t=0, request_id="a", work_units=10, mem_mb=1),
            Request(t=1, request_id="b", work_units=10, mem_mb=1),
        ),
        config=RunConfig(mode="sequential"),
    )
    assert [(e.event, e.request_id) for e in events if e.t == 1] == [
        ("REQUEST_FINISHED", "a"),
        ("REQUEST_ARRIVED", "b"),
        ("REQUEST_STARTED", "b"),
    ]
    assert finish_ticks(events=events) == {"a": 1, "b": 2}


def test_queue_timeout_wins_same_boundary_release() -> None:
    events = events_for(
        requests=(
            Request(t=0, request_id="a", work_units=20, mem_mb=100),
            Request(t=0, request_id="b", work_units=1, mem_mb=1),
        ),
        config=RunConfig(queue_timeout_ticks=2),
    )
    assert [(e.t, e.reason) for e in events if e.event == "REQUEST_DROPPED"] == [
        (2, "QUEUE_TIMEOUT")
    ]


def test_queue_zero_allows_immediate_starts_then_drops_residue() -> None:
    events = events_for(
        requests=(
            Request(t=0, request_id="a", work_units=10, mem_mb=100),
            Request(t=0, request_id="b", work_units=1, mem_mb=1),
        ),
        config=RunConfig(max_queue_size=0),
    )
    assert [(e.request_id, e.reason) for e in events if e.event == "REQUEST_DROPPED"] == [
        ("b", "QUEUE_CAPACITY"),
    ]
    assert finish_ticks(events=events) == {"a": 1}


def test_pending_bypass_keeps_blocked_survivor_order() -> None:
    events = events_for(
        requests=(
            Request(t=0, request_id="a", work_units=20, mem_mb=60),
            Request(t=0, request_id="b", work_units=1, mem_mb=60),
            Request(t=0, request_id="c", work_units=1, mem_mb=40),
        )
    )
    assert [(e.request_id, e.t) for e in events if e.event == "REQUEST_STARTED"] == [
        ("a", 0),
        ("c", 0),
        ("b", 3),
    ]


def test_all_dropped_at_zero_executes_no_interval() -> None:
    frames = list(
        Simulation(
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=1, rate_limit_per_sec=1),),
            requests=(Request(t=0, request_id="r", work_units=10, mem_mb=2),),
            config=RunConfig(),
        ).iter_ticks()
    )
    assert len(frames) == 1 and frames[0].terminal
    assert [e.event for e in frames[0].events] == ["REQUEST_ARRIVED", "REQUEST_DROPPED"]
    assert "reason" not in frames[0].events[1].canonical_line()


def test_idle_gap_is_bounded_but_counts_toward_horizon() -> None:
    frames = list(
        Simulation(
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=1, rate_limit_per_sec=1),),
            requests=(Request(t=999, request_id="r", work_units=1, mem_mb=1),),
            config=RunConfig(max_simulation_ticks=1000),
        ).iter_ticks()
    )
    assert [frame.t for frame in frames] == [0, 999, 1000]
    assert frames[0].duration_ticks == 999
    assert frames[-1].terminal
    with pytest.raises(SimulationError, match="horizon"):
        events_for(
            requests=(Request(t=0, request_id="a", work_units=100, mem_mb=1),),
            config=RunConfig(max_simulation_ticks=1),
        )


@pytest.mark.parametrize(
    ("strategy", "assignments", "finishes"),
    [
        pytest.param("first_fit", ("a", "a", "b", "b"), (5, 2, 1, 2), id="first_fit"),
        pytest.param("round_robin", ("a", "b", "c", "a"), (5, 1, 1, 3), id="round_robin"),
        pytest.param(
            "least_connections", ("a", "b", "c", "b"), (4, 1, 1, 2), id="least_connections"
        ),
        pytest.param(
            "most_available_memory", ("b", "c", "c", "b"), (5, 2, 2, 3), id="most_available_memory"
        ),
    ],
)
def test_scheduling_strategies_make_distinct_hand_calculated_choices(
    strategy: Strategy, assignments: tuple[str, ...], finishes: tuple[int, ...]
) -> None:
    # At t=0, LC/RR spread over a,b,c; MAM sees free capacities 100,200,150.
    # At t=1, LC must forget the completed b/c requests and choose b, while
    # RR wraps its c anchor to a. These expectations do not query _select.
    requests = tuple(
        Request(t=tick, request_id=rid, work_units=work, mem_mb=memory)
        for tick, rid, work, memory in (
            (0, "r1", 40, 80),
            (0, "r2", 10, 20),
            (0, "r3", 10, 60),
            (1, "r4", 10, 10),
        )
    )
    simulation = Simulation(
        servers=tuple(
            Server(id=sid, cpu_units_per_tick=10, mem_mb=memory, rate_limit_per_sec=10)
            for sid, memory in (("c", 150), ("b", 200), ("a", 100))
        ),
        requests=requests,
        config=RunConfig(mode="concurrent", strategy=strategy),
    )
    events = [event for frame in simulation.iter_ticks() for event in frame.events]
    assert [
        (event.request_id, event.server_id, event.t)
        for event in events
        if event.event == "REQUEST_STARTED"
    ] == [
        (request.request_id, server, request.t)
        for request, server in zip(requests, assignments, strict=True)
    ]
    assert finish_ticks(events=events) == dict(
        zip((request.request_id for request in requests), finishes, strict=True)
    )


def test_most_available_memory_uses_reservations_release_and_lowest_id_ties() -> None:
    simulation = Simulation(
        servers=tuple(
            Server(id=sid, cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=4)
            for sid in ("s2", "s1")
        ),
        requests=tuple(
            Request(t=tick, request_id=rid, work_units=work, mem_mb=memory)
            for tick, rid, work, memory in (
                (0, "a", 10, 60),
                (0, "b", 30, 40),
                (0, "c", 20, 50),
                (1, "d", 1, 50),
            )
        ),
        config=RunConfig(mode="concurrent", strategy="most_available_memory"),
    )
    events = [event for frame in simulation.iter_ticks() for event in frame.events]
    assert [
        (event.request_id, event.server_id, event.t)
        for event in events
        if event.event == "REQUEST_STARTED"
    ] == [("a", "s1", 0), ("b", "s2", 0), ("c", "s2", 0), ("d", "s1", 1)]


def test_round_robin_keeps_anchor_on_failed_selection_addition_and_anchor_removal() -> None:
    # At t=1 the blocked d must leave anchor=a. At t=2 adding a member must
    # preserve it, so d chooses autoscale-000001 rather than free a. After
    # that anchor is removed at t=7, late chooses z, the next ID after it.
    simulation = Simulation(
        servers=tuple(
            Server(id=sid, cpu_units_per_tick=10, mem_mb=1, rate_limit_per_sec=1)
            for sid in ("z", "a")
        ),
        requests=tuple(
            Request(t=tick, request_id=rid, work_units=work, mem_mb=1)
            for tick, rid, work in (
                (0, "a", 10),
                (0, "b", 50),
                (1, "c", 10),
                (1, "d", 10),
                (9, "late", 1),
            )
        ),
        config=RunConfig(
            mode="concurrent",
            strategy="round_robin",
            autoscaling=AutoscalingPolicy(
                enabled=True,
                window_ticks=1,
                cooldown_ticks=0,
                min_servers=2,
                max_servers=3,
                scale_out_cpu_threshold=Fraction(numerator=1),
                server_template=ServerTemplate(
                    cpu_units_per_tick=10, mem_mb=1, rate_limit_per_sec=1
                ),
            ),
        ),
    )
    frames = []
    for frame in simulation.iter_ticks():
        frames.append(frame)
        if frame.t == 1:
            assert frame.pending_ids == ("d",)
            assert simulation.anchor == "a", "An unsuccessful selection advanced the cursor"
        if frame.t == 7:
            assert tuple(server.id for server in frame.membership) == ("a", "z")
            assert simulation.anchor == "autoscale-000001", "Removing the anchor reset the cursor"
    assert [
        (event.request_id, event.server_id, event.t)
        for frame in frames
        for event in frame.events
        if event.event == "REQUEST_STARTED"
    ] == [
        ("a", "a", 0),
        ("b", "z", 0),
        ("c", "a", 1),
        ("d", "autoscale-000001", 2),
        ("late", "z", 9),
    ]
    assert [
        (frame.t, frame.scale_decision.action, frame.scale_decision.applied_at)
        for frame in frames
        if frame.scale_decision is not None and frame.scale_decision.action != "none"
    ] == [(1, "scale_out", 2), (6, "scale_in", 7)]


def test_limits_fail_instead_of_fabricating_success() -> None:
    with pytest.raises(SimulationError) as samples:
        list(
            Simulation(
                servers=(Server(id="s", cpu_units_per_tick=1, mem_mb=10, rate_limit_per_sec=10),),
                requests=(Request(t=0, request_id="a", work_units=10, mem_mb=1),),
                config=RunConfig(max_samples=1),
            ).iter_ticks()
        )
    assert samples.value.code == "RESOURCE_LIMIT_EXCEEDED"
    with pytest.raises(SimulationError) as arithmetic:
        events_for(
            requests=tuple(
                Request(t=0, request_id=str(i), work_units=10, mem_mb=1) for i in range(3)
            ),
            config=RunConfig(mode="concurrent", max_fraction_bits=1),
        )
    assert arithmetic.value.code == "RESOURCE_LIMIT_EXCEEDED"


def test_supported_expanded_golden_lifecycles() -> None:
    for folder in sorted(Path("/fixtures/expanded/scenarios").iterdir()):
        if folder.name in {"20_sustained_12k", "21_stress_50k"}:
            continue
        with (folder / "servers.json").open(mode="rb") as source:
            servers = tuple(
                Server(
                    id=s.id,
                    cpu_units_per_tick=s.cpu_units_per_tick,
                    mem_mb=s.mem_mb,
                    rate_limit_per_sec=s.rate_limit_per_sec,
                )
                for s in read_servers(stream=source)
            )
        with (folder / "requests.csv").open(mode="rb") as source:
            requests = tuple(
                Request(t=r.t, request_id=r.request_id, work_units=r.work_units, mem_mb=r.mem_mb)
                for r in read_requests(stream=source)
            )
        config = json.loads(s=(folder / "config.json").read_text())
        expected = json.loads(s=(folder / "expected.json").read_text())
        simulation = Simulation(
            servers=servers,
            requests=requests,
            config=RunConfig(
                mode=config["execution_mode"],
                strategy=config["scheduling_strategy"],
                queue_timeout_ticks=config["queue_timeout_ticks"] or None,
            ),
        )
        events = [e for frame in simulation.iter_ticks() for e in frame.events]
        replay = validate_trace(
            stream=io.BytesIO(
                initial_bytes="".join(event.canonical_line() for event in events).encode()
            ),
            servers=servers,
            requests=requests,
            config=simulation.config,
        )
        counts = expected["terminal_counts"]
        assert replay.finished == counts["completed"] and replay.dropped == counts["dropped"]
        assert sum(e.event == "REQUEST_ARRIVED" for e in events) == counts["arrived"], folder.name
        assert len(finish_ticks(events=events)) == counts["completed"], folder.name
        assert sum(e.event == "REQUEST_DROPPED" for e in events) == counts["dropped"], folder.name
        for rid, lifecycle in expected.get("exact_lifecycle", {}).items():
            for key, kind in (
                ("start", "REQUEST_STARTED"),
                ("finish", "REQUEST_FINISHED"),
                ("drop", "REQUEST_DROPPED"),
            ):
                if key in lifecycle:
                    assert [e.t for e in events if e.request_id == rid and e.event == kind] == [
                        lifecycle[key]
                    ], (folder.name, rid, key)


def test_generated_sequential_trace_passes_unchanged_original_validator(tmp_path: Path) -> None:
    output = tmp_path / "run.jsonl"
    root = Path("/fixtures/core")
    generate(
        servers_path=root / "servers.json",
        requests_path=root / "requests.csv",
        output=output,
        config=RunConfig(mode="sequential"),
    )
    subprocess.run(  # noqa: S603 -- fixed interpreter and trusted fixture paths
        args=[
            sys.executable,
            str(root / "validate_run.py"),
            "--servers",
            str(root / "servers.json"),
            "--requests",
            str(root / "requests.csv"),
            "--run",
            str(output),
        ],
        check=True,
        capture_output=True,
    )


def test_failed_run_does_not_replace_previous_output(tmp_path: Path) -> None:
    output = tmp_path / "run.jsonl"
    output.write_text(data="previous completed artifact\n")
    with pytest.raises(SimulationError):
        generate(
            servers_path=Path("/fixtures/core/servers.json"),
            requests_path=Path("/fixtures/core/requests.csv"),
            output=output,
            config=RunConfig(max_simulation_ticks=3),
        )
    assert output.read_text() == "previous completed artifact\n"
    assert list(tmp_path.iterdir()) == [output]


@pytest.mark.parametrize("mode", ["sequential", "concurrent"])
@pytest.mark.parametrize(
    "strategy", ["first_fit", "round_robin", "least_connections", "most_available_memory"]
)
def test_shuffled_fixture_has_identical_canonical_trace(mode: Mode, strategy: Strategy) -> None:
    traces = []
    for name in ("07_lexical_ties", "08_shuffled_input"):
        root = Path("/fixtures/expanded/scenarios") / name
        with (root / "servers.json").open(mode="rb") as stream:
            servers = tuple(
                Server(
                    id=server.id,
                    cpu_units_per_tick=server.cpu_units_per_tick,
                    mem_mb=server.mem_mb,
                    rate_limit_per_sec=server.rate_limit_per_sec,
                )
                for server in read_servers(stream=stream)
            )
        with (root / "requests.csv").open(mode="rb") as stream:
            requests = tuple(
                Request(
                    t=request.t,
                    request_id=request.request_id,
                    work_units=request.work_units,
                    mem_mb=request.mem_mb,
                )
                for request in read_requests(stream=stream)
            )
        traces.append(
            "".join(
                event.canonical_line()
                for frame in Simulation(
                    servers=servers,
                    requests=requests,
                    config=RunConfig(mode=mode, strategy=strategy),
                ).iter_ticks()
                for event in frame.events
            ).encode(encoding="utf-8")
        )
    assert traces[0] == traces[1], f"07/08 canonical trace differs for {mode}/{strategy}"


def process_outputs(*, seed: str, reverse_inputs: bool) -> dict[str, dict[str, str]]:
    result = subprocess.run(  # noqa: S603 -- fixed local test module, bounded synthetic inputs
        args=[
            sys.executable,
            "-m",
            "tests.engine_repro",
            *(["--reverse-inputs"] if reverse_inputs else []),
        ],
        check=True,
        capture_output=True,
        timeout=20,
        env={**os.environ, "PYTHONHASHSEED": seed},
    )
    assert not result.stderr
    assert len(result.stdout) <= 512 * 1024, "Reproducibility helper exceeded its output budget"
    return TypeAdapter(dict[str, dict[str, str]]).validate_json(result.stdout)


@pytest.fixture(scope="module")
def reference_process_outputs() -> dict[str, dict[str, str]]:
    return process_outputs(seed="0", reverse_inputs=False)


@pytest.mark.parametrize("seed", ["0", "1", "19", "8675309"])
@pytest.mark.parametrize("reverse_inputs", [False, True], ids=["original", "reordered"])
def test_cross_process_matrix_preserves_traces_exact_metrics_and_scaling_decisions(
    seed: str, reverse_inputs: bool, reference_process_outputs: dict[str, dict[str, str]]
) -> None:
    actual = process_outputs(seed=seed, reverse_inputs=reverse_inputs)
    expected_cases = {
        f"{mode}/{strategy}/{workload}"
        for mode in ("sequential", "concurrent")
        for strategy in ("first_fit", "round_robin", "least_connections", "most_available_memory")
        for workload in ("static", "queue_growth", "future_template")
    }
    assert set(actual) == set(reference_process_outputs) == expected_cases
    for case in sorted(expected_cases):
        assert set(actual[case]) == {"trace", "metrics", "decisions"}
        for output in ("trace", "metrics", "decisions"):
            assert actual[case][output] == reference_process_outputs[case][output], (
                f"{case}: {output} differs for seed={seed}, reordered={reverse_inputs}"
            )
        if case.endswith("/static"):
            assert actual[case]["decisions"] == ""
        else:
            actions = {
                json.loads(s=line)["action"] for line in actual[case]["decisions"].splitlines()
            }
            assert {"scale_out", "scale_in"} <= actions, f"{case} did not exercise growth/removal"
