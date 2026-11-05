import io
import json
import random
import subprocess
import sys
from pathlib import Path

import pytest
from mslb_engine.models import Mode, Request, RunConfig, Server, Strategy
from mslb_engine.replay import ReplayError, validate_trace
from mslb_engine.simulator import Simulation

from scripts.generate_example import generate


def trace(*records: tuple[int, str, str, str | None]) -> io.BytesIO:
    lines = []
    for tick, kind, rid, sid in records:
        record: dict[str, int | str] = {"t": tick, "event": kind, "request_id": rid}
        if sid is not None:
            record["server_id"] = sid
        lines.append(json.dumps(record, separators=(",", ":")) + "\n")
    return io.BytesIO("".join(lines).encode())


def test_hand_calculated_fractional_trace_without_engine_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_engine(self: Simulation) -> None:
        raise RuntimeError("Replay must not execute the simulator")

    monkeypatch.setattr(Simulation, "iter_ticks", forbidden_engine)
    value = trace(
        (0, "REQUEST_ARRIVED", "a", None),
        (0, "REQUEST_ARRIVED", "b", None),
        (0, "REQUEST_STARTED", "a", "s"),
        (0, "REQUEST_STARTED", "b", "s"),
        (1, "REQUEST_FINISHED", "a", "s"),
        (2, "REQUEST_FINISHED", "b", "s"),
    )
    result = validate_trace(
        value,
        (Server("s", 10, 10, 2),),
        (Request(0, "a", 1, 1), Request(0, "b", 10, 1)),
        RunConfig(),
    )
    assert result.finished == 2 and result.terminal_tick == 2


@pytest.mark.parametrize("mode", ["concurrent", "sequential_compat"])
@pytest.mark.parametrize(
    "strategy", ["first_fit", "round_robin", "least_connections", "most_available_memory"]
)
def test_bounded_random_workloads_replay_in_both_modes(mode: Mode, strategy: Strategy) -> None:
    random_source = random.Random(20261105)  # noqa: S311 -- reproducible synthetic workloads, no secrets
    servers = (Server("a", 7, 32, 2), Server("b", 11, 16, 1))
    for queue_size in (None, 0, 3):
        requests = tuple(
            Request(
                random_source.randrange(8),
                str(index),
                random_source.randrange(1, 30),
                random_source.randrange(1, 40),
            )
            for index in range(60)
        )
        config = RunConfig(
            mode=mode, strategy=strategy, queue_timeout_ticks=4, max_queue_size=queue_size
        )
        events = (
            event
            for frame in Simulation(servers, requests, config).iter_ticks()
            for event in frame.events
        )
        result = validate_trace(
            io.BytesIO("".join(e.canonical_line() for e in events).encode()),
            servers,
            requests,
            config,
        )
        assert result.finished + result.dropped == len(requests)


@pytest.mark.parametrize("completion", [None, 0, 2])
def test_missing_early_late_completions_fail(completion: int | None) -> None:
    records = [(0, "REQUEST_ARRIVED", "r", None), (0, "REQUEST_STARTED", "r", "s")]
    if completion is not None:
        records.append((completion, "REQUEST_FINISHED", "r", "s"))
    with pytest.raises(ReplayError):
        validate_trace(
            trace(*records), (Server("s", 10, 10, 1),), (Request(0, "r", 10, 1),), RunConfig()
        )


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"\n",
        b"not-json\n",
        b"[]\n",
        b'{"t":true,"event":"REQUEST_ARRIVED","request_id":"r"}\n',
        b'{"t":0,"event":"UNKNOWN","request_id":"r"}\n',
        b'{"event":"REQUEST_ARRIVED","t":0,"request_id":"r"}\n',
        b'{"t":0,"event":"REQUEST_ARRIVED","request_id":"r","extra":1}\n',
        b'{"t":0,"event":"REQUEST_ARRIVED","request_id":"r"}',
        b"x" * 10000,
    ],
)
def test_noncanonical_or_malformed_traces_are_rejected(raw: bytes) -> None:
    with pytest.raises(ReplayError):
        validate_trace(
            io.BytesIO(raw), (Server("s", 10, 10, 1),), (Request(0, "r", 10, 1),), RunConfig()
        )


@pytest.mark.parametrize(
    "config,mem,rate",
    [
        (RunConfig(), 6, 2),
        (RunConfig(), 1, 1),
        (RunConfig(mode="sequential_compat"), 1, 2),
    ],
)
def test_overlap_resource_and_start_rate_violations(config: RunConfig, mem: int, rate: int) -> None:
    value = trace(
        (0, "REQUEST_ARRIVED", "a", None),
        (0, "REQUEST_ARRIVED", "b", None),
        (0, "REQUEST_STARTED", "a", "s"),
        (0, "REQUEST_STARTED", "b", "s"),
        (1, "REQUEST_FINISHED", "a", "s"),
        (1, "REQUEST_FINISHED", "b", "s"),
    )
    with pytest.raises(ReplayError, match="Memory, rate, or sequential"):
        validate_trace(
            value,
            (Server("s", 10, 10, rate),),
            (Request(0, "a", 1, mem), Request(0, "b", 1, mem)),
            config,
        )


def test_illegal_drop_and_non_work_conserving_trace_fail() -> None:
    servers, requests = (Server("s", 10, 10, 1),), (Request(0, "r", 1, 1),)
    with pytest.raises(ReplayError):
        validate_trace(
            trace((0, "REQUEST_ARRIVED", "r", None), (0, "REQUEST_DROPPED", "r", None)),
            servers,
            requests,
            RunConfig(),
        )


def test_duplicate_and_wrong_server_events_fail() -> None:
    for finish_server in ("wrong", "s"):
        records = [
            (0, "REQUEST_ARRIVED", "r", None),
            (0, "REQUEST_STARTED", "r", "s"),
            (1, "REQUEST_FINISHED", "r", finish_server),
        ]
        if finish_server == "s":
            records.append((1, "REQUEST_FINISHED", "r", finish_server))
        value = trace(*records)
        with pytest.raises(ReplayError):
            validate_trace(value, (Server("s", 10, 10, 1),), (Request(0, "r", 1, 1),), RunConfig())


@pytest.mark.parametrize("missing", [False, True])
def test_cli_returns_structured_failure_without_traceback(tmp_path: Path, missing: bool) -> None:
    output = tmp_path / "bad.jsonl"
    if not missing:
        output.write_text("{}\n")
    result = subprocess.run(  # noqa: S603 -- fixed interpreter and trusted local CLI
        [
            sys.executable,
            "scripts/validate_concurrent.py",
            "--servers",
            "/fixtures/core/servers.json",
            "--requests",
            "/fixtures/core/requests.csv",
            "--run",
            str(output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert json.loads(result.stderr)["error"]["code"] == "VALIDATION_FAILED"
    assert "Traceback" not in result.stderr


def test_independent_validation_failure_preserves_previous_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mslb_engine.models import Event

    original = Event.canonical_line

    def corrupt(self: Event) -> str:
        line = original(self)
        return line.replace('"server_id":"s1"', '"server_id":"unknown"')

    monkeypatch.setattr(Event, "canonical_line", corrupt)
    output = tmp_path / "run.jsonl"
    output.write_text("previous verified trace\n")
    with pytest.raises(ReplayError):
        generate(
            Path("/fixtures/core/servers.json"),
            Path("/fixtures/core/requests.csv"),
            output,
            RunConfig(),
        )
    assert output.read_text() == "previous verified trace\n"
    assert list(tmp_path.iterdir()) == [output]
