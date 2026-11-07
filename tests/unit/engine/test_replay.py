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
        lines.append(json.dumps(obj=record, separators=(",", ":")) + "\n")
    return io.BytesIO(initial_bytes="".join(lines).encode())


def test_hand_calculated_fractional_trace_without_engine_oracle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden_engine(self: Simulation) -> None:
        raise RuntimeError("Replay must not execute the simulator")

    monkeypatch.setattr(target=Simulation, name="iter_ticks", value=forbidden_engine)
    value = trace(
        (0, "REQUEST_ARRIVED", "a", None),
        (0, "REQUEST_ARRIVED", "b", None),
        (0, "REQUEST_STARTED", "a", "s"),
        (0, "REQUEST_STARTED", "b", "s"),
        (1, "REQUEST_FINISHED", "a", "s"),
        (2, "REQUEST_FINISHED", "b", "s"),
    )
    result = validate_trace(
        stream=value,
        servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=10, rate_limit_per_sec=2),),
        requests=(
            Request(t=0, request_id="a", work_units=1, mem_mb=1),
            Request(t=0, request_id="b", work_units=10, mem_mb=1),
        ),
        config=RunConfig(mode="concurrent"),
    )
    assert result.finished == 2 and result.terminal_tick == 2


@pytest.mark.parametrize(argnames="mode", argvalues=["concurrent", "sequential"])
@pytest.mark.parametrize(
    argnames="strategy",
    argvalues=["first_fit", "round_robin", "least_connections", "most_available_memory"],
)
def test_bounded_random_workloads_replay_in_both_modes(mode: Mode, strategy: Strategy) -> None:
    random_source = random.Random(x=20261105)  # noqa: S311 -- reproducible synthetic workloads, no secrets
    servers = (
        Server(id="a", cpu_units_per_tick=7, mem_mb=32, rate_limit_per_sec=2),
        Server(id="b", cpu_units_per_tick=11, mem_mb=16, rate_limit_per_sec=1),
    )
    for queue_size in (None, 0, 3):
        requests = tuple(
            Request(
                t=random_source.randrange(start=8),
                request_id=str(object=index),
                work_units=random_source.randrange(start=1, stop=30),
                mem_mb=random_source.randrange(start=1, stop=40),
            )
            for index in range(60)
        )
        config = RunConfig(
            mode=mode, strategy=strategy, queue_timeout_ticks=4, max_queue_size=queue_size
        )
        events = (
            event
            for frame in Simulation(servers=servers, requests=requests, config=config).iter_ticks()
            for event in frame.events
        )
        result = validate_trace(
            stream=io.BytesIO(initial_bytes="".join(e.canonical_line() for e in events).encode()),
            servers=servers,
            requests=requests,
            config=config,
        )
        assert result.finished + result.dropped == len(requests)


@pytest.mark.parametrize(argnames="completion", argvalues=[None, 0, 2])
def test_missing_early_late_completions_fail(completion: int | None) -> None:
    records = [(0, "REQUEST_ARRIVED", "r", None), (0, "REQUEST_STARTED", "r", "s")]
    if completion is not None:
        records.append((completion, "REQUEST_FINISHED", "r", "s"))
    with pytest.raises(expected_exception=ReplayError):
        validate_trace(
            stream=trace(*records),
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=10, rate_limit_per_sec=1),),
            requests=(Request(t=0, request_id="r", work_units=10, mem_mb=1),),
            config=RunConfig(),
        )


@pytest.mark.parametrize(
    argnames="raw",
    argvalues=[
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
    with pytest.raises(expected_exception=ReplayError):
        validate_trace(
            stream=io.BytesIO(initial_bytes=raw),
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=10, rate_limit_per_sec=1),),
            requests=(Request(t=0, request_id="r", work_units=10, mem_mb=1),),
            config=RunConfig(),
        )


@pytest.mark.parametrize(
    argnames="config,mem,rate",
    argvalues=[
        (RunConfig(mode="concurrent"), 6, 2),
        (RunConfig(mode="concurrent"), 1, 1),
        (RunConfig(mode="sequential"), 1, 2),
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
    with pytest.raises(expected_exception=ReplayError, match="Memory, rate, or sequential"):
        validate_trace(
            stream=value,
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=10, rate_limit_per_sec=rate),),
            requests=(
                Request(t=0, request_id="a", work_units=1, mem_mb=mem),
                Request(t=0, request_id="b", work_units=1, mem_mb=mem),
            ),
            config=config,
        )


def test_illegal_drop_and_non_work_conserving_trace_fail() -> None:
    servers, requests = (
        (Server(id="s", cpu_units_per_tick=10, mem_mb=10, rate_limit_per_sec=1),),
        (Request(t=0, request_id="r", work_units=1, mem_mb=1),),
    )
    with pytest.raises(expected_exception=ReplayError):
        validate_trace(
            stream=trace((0, "REQUEST_ARRIVED", "r", None), (0, "REQUEST_DROPPED", "r", None)),
            servers=servers,
            requests=requests,
            config=RunConfig(),
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
        with pytest.raises(expected_exception=ReplayError):
            validate_trace(
                stream=value,
                servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=10, rate_limit_per_sec=1),),
                requests=(Request(t=0, request_id="r", work_units=1, mem_mb=1),),
                config=RunConfig(),
            )


@pytest.mark.parametrize(argnames="missing", argvalues=[False, True])
def test_cli_returns_structured_failure_without_traceback(tmp_path: Path, missing: bool) -> None:
    output = tmp_path / "bad.jsonl"
    if not missing:
        output.write_text(data="{}\n")
    result = subprocess.run(  # noqa: S603 -- fixed interpreter and trusted local CLI
        args=[
            sys.executable,
            "scripts/validate_concurrent.py",
            "--servers",
            "/fixtures/core/servers.json",
            "--requests",
            "/fixtures/core/requests.csv",
            "--run",
            str(object=output),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2
    assert json.loads(s=result.stderr)["error"]["code"] == "VALIDATION_FAILED"
    assert "Traceback" not in result.stderr


def test_independent_validation_failure_preserves_previous_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from mslb_engine.models import Event

    original = Event.canonical_line

    def corrupt(self: Event) -> str:
        line = original(self=self)
        return line.replace('"server_id":"s1"', '"server_id":"unknown"')

    monkeypatch.setattr(target=Event, name="canonical_line", value=corrupt)
    output = tmp_path / "run.jsonl"
    output.write_text(data="previous verified trace\n")
    with pytest.raises(expected_exception=ReplayError):
        generate(
            servers_path=Path("/fixtures/core/servers.json"),
            requests_path=Path("/fixtures/core/requests.csv"),
            output=output,
            config=RunConfig(),
        )
    assert output.read_text() == "previous verified trace\n"
    assert list(tmp_path.iterdir()) == [output]
