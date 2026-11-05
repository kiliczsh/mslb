import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from mslb_contracts.inputs import read_requests, read_servers
from mslb_engine.models import Event, Request, RunConfig, Server, SimulationError, Strategy
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation

from scripts.generate_example import generate


def events_for(requests: tuple[Request, ...], config: RunConfig | None = None) -> list[Event]:
    simulation = Simulation((Server("s", 10, 100, 10),), requests, config or RunConfig())
    return [event for frame in simulation.iter_ticks() for event in frame.events]


def finish_ticks(events: list[Event]) -> dict[str, int]:
    return {event.request_id: event.t for event in events if event.event == "REQUEST_FINISHED"}


def test_equal_fractional_sharing_and_consumption() -> None:
    requests = tuple(Request(0, str(index), 10, 1) for index in range(3))
    frames = list(Simulation((Server("s", 10, 100, 10),), requests, RunConfig()).iter_ticks())
    assert finish_ticks([event for frame in frames for event in frame.events]) == {
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
            (Server("s", 10, 100, 10),),
            (Request(0, "a", 1, 1), Request(0, "b", 10, 1)),
            RunConfig(),
        ).iter_ticks()
    )
    assert frames[0].samples[0].consumed_cpu == 6
    assert frames[0].samples[0].allocated_cpu == 10
    assert finish_ticks([e for f in frames for e in f.events]) == {"a": 1, "b": 2}


def test_midflight_arrival_changes_subsequent_shares() -> None:
    events = events_for((Request(0, "a", 20, 1), Request(1, "b", 10, 1)))
    assert finish_ticks(events) == {"a": 3, "b": 3}


def test_sequential_boundary_reuse_and_canonical_order() -> None:
    events = events_for(
        (Request(0, "a", 10, 1), Request(1, "b", 10, 1)), RunConfig(mode="sequential_compat")
    )
    assert [(e.event, e.request_id) for e in events if e.t == 1] == [
        ("REQUEST_FINISHED", "a"),
        ("REQUEST_ARRIVED", "b"),
        ("REQUEST_STARTED", "b"),
    ]
    assert finish_ticks(events) == {"a": 1, "b": 2}


def test_queue_timeout_wins_same_boundary_release() -> None:
    events = events_for(
        (Request(0, "a", 20, 100), Request(0, "b", 1, 1)), RunConfig(queue_timeout_ticks=2)
    )
    assert [(e.t, e.reason) for e in events if e.event == "REQUEST_DROPPED"] == [
        (2, "QUEUE_TIMEOUT")
    ]


def test_queue_zero_allows_immediate_starts_then_drops_residue() -> None:
    events = events_for(
        (Request(0, "a", 10, 100), Request(0, "b", 1, 1)), RunConfig(max_queue_size=0)
    )
    assert [(e.request_id, e.reason) for e in events if e.event == "REQUEST_DROPPED"] == [
        ("b", "QUEUE_CAPACITY"),
    ]
    assert finish_ticks(events) == {"a": 1}


def test_pending_bypass_keeps_blocked_survivor_order() -> None:
    events = events_for((Request(0, "a", 20, 60), Request(0, "b", 1, 60), Request(0, "c", 1, 40)))
    assert [(e.request_id, e.t) for e in events if e.event == "REQUEST_STARTED"] == [
        ("a", 0),
        ("c", 0),
        ("b", 3),
    ]


def test_all_dropped_at_zero_executes_no_interval() -> None:
    frames = list(
        Simulation((Server("s", 10, 1, 1),), (Request(0, "r", 10, 2),), RunConfig()).iter_ticks()
    )
    assert len(frames) == 1 and frames[0].terminal
    assert [e.event for e in frames[0].events] == ["REQUEST_ARRIVED", "REQUEST_DROPPED"]
    assert "reason" not in frames[0].events[1].canonical_line()


def test_idle_gap_is_bounded_but_counts_toward_horizon() -> None:
    frames = list(
        Simulation(
            (Server("s", 10, 1, 1),),
            (Request(999, "r", 1, 1),),
            RunConfig(max_simulation_ticks=1000),
        ).iter_ticks()
    )
    assert [frame.t for frame in frames] == [0, 999, 1000]
    assert frames[0].duration_ticks == 999
    assert frames[-1].terminal
    with pytest.raises(SimulationError, match="horizon"):
        events_for((Request(0, "a", 100, 1),), RunConfig(max_simulation_ticks=1))


@pytest.mark.parametrize(
    "strategy", ["first_fit", "round_robin", "least_connections", "most_available_memory"]
)
def test_four_strategies_choose_eligible_servers_with_stable_ties(strategy: Strategy) -> None:
    servers = (Server("a", 10, 100, 1), Server("b", 10, 100, 2))
    events = [
        e
        for f in Simulation(
            servers,
            tuple(Request(0, str(i), 10, 10) for i in range(4)),
            RunConfig(strategy=strategy),
        ).iter_ticks()
        for e in f.events
    ]
    assert [(e.request_id, e.server_id, e.t) for e in events if e.event == "REQUEST_STARTED"][
        :3
    ] == [
        ("0", "a", 0),
        ("1", "b", 0),
        ("2", "b", 0),
    ]
    assert len(finish_ticks(events)) == 4


def test_limits_fail_instead_of_fabricating_success() -> None:
    with pytest.raises(SimulationError) as samples:
        list(
            Simulation(
                (Server("s", 1, 10, 10),), (Request(0, "a", 10, 1),), RunConfig(max_samples=1)
            ).iter_ticks()
        )
    assert samples.value.code == "RESOURCE_LIMIT_EXCEEDED"
    with pytest.raises(SimulationError) as arithmetic:
        events_for(
            tuple(Request(0, str(i), 10, 1) for i in range(3)), RunConfig(max_fraction_bits=1)
        )
    assert arithmetic.value.code == "RESOURCE_LIMIT_EXCEEDED"


def test_supported_expanded_golden_lifecycles() -> None:
    for folder in sorted(Path("/fixtures/expanded/scenarios").iterdir()):
        if folder.name in {"20_sustained_12k", "21_stress_50k"}:
            continue
        with (folder / "servers.json").open("rb") as source:
            servers = tuple(
                Server(s.id, s.cpu_units_per_tick, s.mem_mb, s.rate_limit_per_sec)
                for s in read_servers(source)
            )
        with (folder / "requests.csv").open("rb") as source:
            requests = tuple(
                Request(r.t, r.request_id, r.work_units, r.mem_mb) for r in read_requests(source)
            )
        config = json.loads((folder / "config.json").read_text())
        expected = json.loads((folder / "expected.json").read_text())
        simulation = Simulation(
            servers,
            requests,
            RunConfig(
                strategy=config["scheduling_strategy"],
                queue_timeout_ticks=config["queue_timeout_ticks"] or None,
            ),
        )
        events = [e for frame in simulation.iter_ticks() for e in frame.events]
        replay = validate_trace(
            io.BytesIO("".join(event.canonical_line() for event in events).encode()),
            servers,
            requests,
            simulation.config,
        )
        counts = expected["terminal_counts"]
        assert replay.finished == counts["completed"] and replay.dropped == counts["dropped"]
        assert sum(e.event == "REQUEST_ARRIVED" for e in events) == counts["arrived"], folder.name
        assert len(finish_ticks(events)) == counts["completed"], folder.name
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
        root / "servers.json", root / "requests.csv", output, RunConfig(mode="sequential_compat")
    )
    subprocess.run(  # noqa: S603 -- fixed interpreter and trusted fixture paths
        [
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
    output.write_text("previous completed artifact\n")
    with pytest.raises(SimulationError):
        generate(
            Path("/fixtures/core/servers.json"),
            Path("/fixtures/core/requests.csv"),
            output,
            RunConfig(max_simulation_ticks=3),
        )
    assert output.read_text() == "previous completed artifact\n"
    assert list(tmp_path.iterdir()) == [output]


def test_cross_process_hash_seeds_produce_identical_bytes(tmp_path: Path) -> None:
    outputs = []
    for seed in map(str, range(100)):
        output = tmp_path / f"run-{seed}.jsonl"
        subprocess.run(  # noqa: S603 -- fixed local CLI and bounded test-owned paths
            [
                sys.executable,
                "scripts/generate_example.py",
                "--servers",
                "/fixtures/core/servers.json",
                "--requests",
                "/fixtures/core/requests.csv",
                "--output",
                str(output),
            ],
            check=True,
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
        )
        outputs.append(output.read_bytes())
    assert all(output == outputs[0] for output in outputs)
