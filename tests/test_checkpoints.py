import hashlib
import io
import json
import subprocess
import sys
from pathlib import Path
from typing import BinaryIO

import pytest
from mslb_application.artifacts import timeline_record
from mslb_application.checkpoints import (
    PREFIX_NAMES,
    capture_terminal_seal,
    checkpoint_due,
    read_checkpoint,
    read_terminal_seal,
    write_checkpoint,
)
from mslb_application.errors import OperationError
from mslb_application.snapshots import canonical_bytes, domain_config, domain_inputs, make_snapshot
from mslb_contracts.checkpoints import IntervalCheckpoint
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import RequestInput, ServerInput
from mslb_contracts.runs import LegacyRunSettings, RunSettings, RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.models import STRATEGIES, SimulationError, TickFrame
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation


def saved_run(
    mode: str = "concurrent", strategy: str = "first_fit", scaling: bool = True
) -> RunSnapshot:
    settings_type = LegacyRunSettings if mode == "sequential_compat" else RunSettings
    settings = settings_type.model_validate_json(
        json_data=json.dumps(
            obj={
                "mode": mode,
                "strategy": strategy,
                "queue_timeout_ticks": 7,
                "max_queue_size": 3,
                "autoscaling": {
                    "enabled": scaling,
                    "window_ticks": 3,
                    "max_servers": 4,
                    "server_template": {
                        "cpu_units_per_tick": 2,
                        "mem_mb": 10,
                        "rate_limit_per_sec": 1,
                    },
                },
            }
        )
    )
    return make_snapshot(
        servers=(
            ServerInput(id="s1", cpu_units_per_tick=1, mem_mb=4, rate_limit_per_sec=1),
            ServerInput(id="s2", cpu_units_per_tick=2, mem_mb=3, rate_limit_per_sec=1),
        ),
        requests=tuple(
            RequestInput(t=t, request_id=rid, work_units=work, mem_mb=mem)
            for t, rid, work, mem in (
                (0, "r0", 7, 3),
                (0, "r1", 5, 2),
                (0, "r2", 5, 2),
                (1, "r3", 5, 1),
                (4, "r4", 1, 8),
                (40, "late", 3, 1),
            )
        ),
        settings=settings,
    )


def execution(snapshot: RunSnapshot) -> tuple[Simulation, MetricAccumulator, dict[str, BinaryIO]]:
    servers, requests = domain_inputs(snapshot=snapshot)
    return (
        Simulation(
            servers=servers,
            requests=requests,
            config=domain_config(settings=snapshot.configuration),
        ),
        MetricAccumulator(requests=requests, servers=servers),
        {name: io.BytesIO() for name in PREFIX_NAMES},
    )


def append(frame: TickFrame, metrics: MetricAccumulator, streams: dict[str, BinaryIO]) -> None:
    metrics.observe(frame=frame)
    for event in frame.events:
        streams["run.jsonl"].write(event.canonical_line().encode())
        if event.reason is not None:
            streams["drop_reasons.jsonl"].write(
                canonical_bytes(
                    value={"t": event.t, "request_id": event.request_id, "reason": event.reason}
                )
            )
    streams["timeline.jsonl"].write(canonical_bytes(value=timeline_record(frame=frame)))
    if frame.scale_decision is not None:
        streams["autoscaling_decisions.jsonl"].write(frame.scale_decision.canonical_line().encode())


def contents(streams: dict[str, BinaryIO]) -> dict[str, bytes]:
    result = {}
    for name, stream in streams.items():
        stream.seek(0)
        result[name] = stream.read()
        stream.seek(0, io.SEEK_END)
    return result


@pytest.mark.parametrize(
    argnames="mode", argvalues=["concurrent", "sequential", "sequential_compat"]
)
@pytest.mark.parametrize(argnames="strategy", argvalues=STRATEGIES)
@pytest.mark.parametrize(argnames="scaling", argvalues=[False, True])
def test_restore_at_every_completed_interval_matches_uninterrupted_bytes_and_metrics(
    tmp_path: Path, mode: str, strategy: str, scaling: bool
) -> None:
    saved = saved_run(mode=mode, strategy=strategy, scaling=scaling)
    simulation, metrics, streams = execution(snapshot=saved)
    reference = list(simulation.iter_ticks())
    for frame in reference:
        append(frame=frame, metrics=metrics, streams=streams)
    expected_bytes, expected_metrics = contents(streams=streams), metrics.summary()
    simulation, metrics, streams = execution(snapshot=saved)
    for index, frame in enumerate(iterable=simulation.iter_ticks()):
        append(frame=frame, metrics=metrics, streams=streams)
        if frame.terminal:
            break
        artifact = write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
        )
        # A crash may leave uncommitted bytes beyond each durable offset.
        cloned: dict[str, BinaryIO] = {
            name: io.BytesIO(initial_bytes=data + b"uncommitted corrupt suffix")
            for name, data in contents(streams=streams).items()
        }
        with artifact.path.open(mode="rb") as source:
            restored = read_checkpoint(
                stream=source, expected_digest=artifact.sha256, snapshot=saved, prefixes=cloned
            )
        for prefix in restored.prefixes:
            assert cloned[prefix.name].tell() == prefix.size_bytes
            cloned[prefix.name].truncate()
        continuation = list(restored.simulation.iter_ticks())
        assert continuation == reference[index + 1 :]
        for next_frame in continuation:
            append(frame=next_frame, metrics=restored.metrics, streams=cloned)
        assert contents(streams=cloned) == expected_bytes
        assert restored.metrics.summary() == expected_metrics
    servers, requests = domain_inputs(snapshot=saved)
    summary = validate_trace(
        stream=io.BytesIO(initial_bytes=expected_bytes["run.jsonl"]),
        servers=servers,
        requests=requests,
        config=domain_config(settings=saved.configuration),
        scale_stream=io.BytesIO(initial_bytes=expected_bytes["autoscaling_decisions.jsonl"]),
    )
    assert summary.finished + summary.dropped == len(requests)


def test_pending_completion_and_pending_scale_activation_are_preserved(tmp_path: Path) -> None:
    saved = saved_run()
    simulation, metrics, streams = execution(snapshot=saved)
    iterator = simulation.iter_ticks()
    first = next(iterator)
    append(frame=first, metrics=metrics, streams=streams)
    assert simulation.autoscaler.pending is not None
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    with artifact.path.open(mode="rb") as source:
        restored = read_checkpoint(
            stream=source, expected_digest=artifact.sha256, snapshot=saved, prefixes=streams
        )
    assert (
        write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=restored.simulation,
            metrics=restored.metrics,
            prefixes=streams,
        )
        == artifact
    )
    assert next(restored.simulation.iter_ticks()) == next(iterator)
    # The exhaustive boundary test above also resumes immediately before each
    # zero-remaining active request emits its next-boundary completion.


def test_initial_terminal_and_failed_boundaries_are_not_resumable_checkpoints(
    tmp_path: Path,
) -> None:
    saved = saved_run(scaling=False)
    simulation, metrics, streams = execution(snapshot=saved)
    with pytest.raises(expected_exception=OperationError, match="completed"):
        write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
        )
    for frame in simulation.iter_ticks():
        append(frame=frame, metrics=metrics, streams=streams)
    with pytest.raises(expected_exception=OperationError, match="nonterminal"):
        write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
        )
    failed = make_snapshot(
        servers=saved.servers,
        requests=(saved.requests[0],),
        settings=RunSettings(max_simulation_ticks=1),
    )
    simulation, metrics, streams = execution(snapshot=failed)
    iterator = simulation.iter_ticks()
    append(frame=next(iterator), metrics=metrics, streams=streams)
    with pytest.raises(expected_exception=SimulationError):
        next(iterator)
    with pytest.raises(expected_exception=OperationError, match="completed"):
        write_checkpoint(
            directory=tmp_path,
            snapshot=failed,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
        )
    assert list(tmp_path.iterdir()) == []


def test_all_dropped_tick_zero_requires_a_terminal_seal_instead_of_checkpoint(
    tmp_path: Path,
) -> None:
    saved = make_snapshot(
        servers=(ServerInput(id="small", cpu_units_per_tick=1, mem_mb=1, rate_limit_per_sec=1),),
        requests=(RequestInput(t=0, request_id="large", work_units=1, mem_mb=2),),
        settings=RunSettings(),
    )
    simulation, metrics, streams = execution(snapshot=saved)
    frames = list(simulation.iter_ticks())
    assert len(frames) == 1 and frames[0].terminal and frames[0].t == 0
    append(frame=frames[0], metrics=metrics, streams=streams)
    assert simulation.executed_intervals == 0 and not checkpoint_due(simulation=simulation)
    with pytest.raises(expected_exception=OperationError):
        write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
        )
    assert not list(tmp_path.iterdir())


def test_checkpoint_cadence_counts_executed_intervals_and_new_events(tmp_path: Path) -> None:
    server = ServerInput(id="cpu", cpu_units_per_tick=1, mem_mb=1000, rate_limit_per_sec=1000)
    saved = make_snapshot(
        servers=(server,),
        requests=(RequestInput(t=9999, request_id="late", work_units=1000, mem_mb=1),),
        settings=RunSettings(),
    )
    simulation, metrics, streams = execution(snapshot=saved)
    iterator = simulation.iter_ticks()
    append(frame=next(iterator), metrics=metrics, streams=streams)
    assert simulation.tick == 9999 and simulation.executed_intervals == 1
    assert not checkpoint_due(simulation=simulation)
    for _ in range(99):
        append(frame=next(iterator), metrics=metrics, streams=streams)
    assert checkpoint_due(simulation=simulation)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    assert not checkpoint_due(simulation=simulation, previous=artifact)
    burst = make_snapshot(
        servers=(server,),
        requests=tuple(
            RequestInput(t=0, request_id=f"burst-{n}", work_units=2, mem_mb=1) for n in range(500)
        ),
        settings=RunSettings(mode="concurrent"),
    )
    simulation, metrics, streams = execution(snapshot=burst)
    append(frame=next(simulation.iter_ticks()), metrics=metrics, streams=streams)
    assert simulation.executed_intervals == 1 and simulation.event_count == 1000
    assert checkpoint_due(simulation=simulation)


def test_budget_failure_preserves_previous_checkpoint_and_leaves_no_staging_file(
    tmp_path: Path,
) -> None:
    saved = saved_run()
    simulation, metrics, streams = execution(snapshot=saved)
    append(frame=next(simulation.iter_ticks()), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    before = artifact.path.read_bytes()
    assert (
        write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
        )
        == artifact
    )
    with pytest.raises(expected_exception=OperationError, match="budget") as failure:
        write_checkpoint(
            directory=tmp_path,
            snapshot=saved,
            simulation=simulation,
            metrics=metrics,
            prefixes=streams,
            max_bytes=1,
        )
    assert failure.value.code == "RESOURCE_LIMIT_EXCEEDED"
    assert list(tmp_path.iterdir()) == [artifact.path] and artifact.path.read_bytes() == before


def test_prefix_truncation_corruption_missing_names_and_wrong_checkpoint_hash_fail(
    tmp_path: Path,
) -> None:
    saved = saved_run()
    simulation, metrics, streams = execution(snapshot=saved)
    append(frame=next(simulation.iter_ticks()), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    original = contents(streams=streams)
    for corrupt in ("truncated", "changed", "missing", "checkpoint"):
        cloned: dict[str, BinaryIO] = {
            name: io.BytesIO(initial_bytes=data) for name, data in original.items()
        }
        if corrupt == "truncated":
            cloned["run.jsonl"] = io.BytesIO(initial_bytes=original["run.jsonl"][:-1])
        elif corrupt == "changed":
            cloned["run.jsonl"] = io.BytesIO(initial_bytes=b"X" + original["run.jsonl"][1:])
        elif corrupt == "missing":
            cloned.pop("timeline.jsonl")
        with (
            artifact.path.open(mode="rb") as source,
            pytest.raises(expected_exception=OperationError),
        ):
            read_checkpoint(
                stream=source,
                expected_digest="0" * 64 if corrupt == "checkpoint" else artifact.sha256,
                snapshot=saved,
                prefixes=cloned,
            )


@pytest.mark.parametrize(
    argnames="corruption",
    argvalues=[
        "version",
        "boolean",
        "float",
        "identity",
        "cursor",
        "queue",
        "reservation",
        "work",
        "sample_limit",
        "original_capacity",
        "history",
        "counter",
        "activation",
        "sequence",
        "integral",
    ],
)
def test_malformed_or_inconsistent_state_is_rejected_before_execution(
    tmp_path: Path, corruption: str
) -> None:
    saved = saved_run()
    simulation, metrics, streams = execution(snapshot=saved)
    append(frame=next(simulation.iter_ticks()), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    state = json.loads(s=artifact.path.read_bytes())
    if corruption == "version":
        state["schema_version"] = 2
    elif corruption == "boolean":
        state["input_cursor"] = True
    elif corruption == "float":
        state["next_tick"] = 1.0
    elif corruption == "identity":
        state["snapshot_digest"] = "0" * 64
    elif corruption == "cursor":
        state["input_cursor"] = 0
    elif corruption == "queue":
        state["pending_ids"] = []
    elif corruption == "reservation":
        state["servers"][0]["reserved_mem_mb"] = 0
    elif corruption == "work":
        state["servers"][0]["active"][0]["remaining"]["numerator"] = "-1"
    elif corruption == "sample_limit":
        state["recorded_samples"] = 1_000_001
    elif corruption == "original_capacity":
        state["servers"][0]["server"]["mem_mb"] = 9
    elif corruption == "history":
        state["autoscaler"]["history"][0]["ending_tick"] = 0
    elif corruption == "counter":
        state["autoscaler"]["counter"] = 0
    elif corruption == "activation":
        state["autoscaler"]["pending"]["applied_at"] = 2
    elif corruption == "sequence":
        state["event_count"] += 1
    else:
        state["metrics"]["servers"][0]["cpu_consumed"]["numerator"] = "0"
    payload = canonical_bytes(value=state)
    with pytest.raises(expected_exception=OperationError):
        read_checkpoint(
            stream=io.BytesIO(initial_bytes=payload),
            expected_digest=hashlib.sha256(data=payload).hexdigest(),
            snapshot=saved,
            prefixes=streams,
        )


def test_actual_high_precision_remaining_work_round_trips_without_float_conversion(
    tmp_path: Path,
) -> None:
    saved = make_snapshot(
        servers=(
            ServerInput(id="cpu", cpu_units_per_tick=1, mem_mb=1000, rate_limit_per_sec=1000),
        ),
        requests=(
            RequestInput(t=0, request_id="short", work_units=5, mem_mb=1),
            *(
                RequestInput(t=t, request_id=f"long-{t:03d}", work_units=1000, mem_mb=1)
                for t in range(83)
            ),
        ),
        settings=RunSettings(mode="concurrent"),
    )
    simulation, metrics, streams = execution(snapshot=saved)
    iterator = simulation.iter_ticks()
    for _ in range(83):
        append(frame=next(iterator), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    assert max(value.numerator.bit_length() for value in simulation.active["cpu"].values()) > 100
    with artifact.path.open(mode="rb") as source:
        restored = read_checkpoint(
            stream=source, expected_digest=artifact.sha256, snapshot=saved, prefixes=streams
        )
    assert restored.simulation.active == simulation.active
    assert next(restored.simulation.iter_ticks()) == next(iterator)
    state = IntervalCheckpoint.model_validate_json(json_data=artifact.path.read_bytes())
    assert all(
        isinstance(work.remaining.numerator, str)
        for server in state.servers
        for work in server.active
    )


def test_restore_in_a_fresh_process_matches_the_original_continuation(tmp_path: Path) -> None:
    saved = saved_run(strategy="round_robin")
    simulation, metrics, streams = execution(snapshot=saved)
    iterator = simulation.iter_ticks()
    for _ in range(4):
        append(frame=next(iterator), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    (tmp_path / "snapshot.json").write_bytes(
        data=canonical_bytes(value=saved.model_dump(mode="json"))
    )
    for name, data in contents(streams=streams).items():
        (tmp_path / name).write_bytes(data=data)
    for frame in iterator:
        append(frame=frame, metrics=metrics, streams=streams)
    code = """
import sys
from contextlib import ExitStack
from pathlib import Path
from mslb_application.checkpoints import PREFIX_NAMES, read_checkpoint
from mslb_application.snapshots import read_snapshot, canonical_bytes
root = Path(sys.argv[1])
with ExitStack() as stack:
    saved = read_snapshot(stack.enter_context((root / "snapshot.json").open("rb")))
    prefixes = {name: stack.enter_context((root / name).open("r+b")) for name in PREFIX_NAMES}
    source = stack.enter_context(Path(sys.argv[2]).open("rb"))
    restored = read_checkpoint(source, sys.argv[3], saved, prefixes)
    for frame in restored.simulation.iter_ticks():
        restored.metrics.observe(frame)
        for event in frame.events:
            prefixes["run.jsonl"].write(event.canonical_line().encode())
        if frame.scale_decision is not None:
            prefixes["autoscaling_decisions.jsonl"].write(frame.scale_decision.canonical_line().encode())
    (root / "metrics.json").write_bytes(canonical_bytes(restored.metrics.summary()))
"""
    subprocess.run(  # noqa: S603 -- fixed restore program and bounded test-owned paths
        args=[
            sys.executable,
            "-c",
            code,
            str(object=tmp_path),
            str(object=artifact.path),
            artifact.sha256,
        ],
        check=True,
        capture_output=True,
    )
    expected = contents(streams=streams)
    assert (tmp_path / "run.jsonl").read_bytes() == expected["run.jsonl"]
    assert (tmp_path / "autoscaling_decisions.jsonl").read_bytes() == expected[
        "autoscaling_decisions.jsonl"
    ]
    assert json.loads(s=(tmp_path / "metrics.json").read_bytes()) == metrics.summary()


@pytest.mark.parametrize(argnames="drop_at_zero", argvalues=[False, True])
def test_terminal_seal_round_trip_does_not_require_or_replay_an_interval(
    drop_at_zero: bool,
) -> None:
    saved = make_snapshot(
        servers=(ServerInput(id="s", cpu_units_per_tick=2, mem_mb=1, rate_limit_per_sec=1),),
        requests=(
            RequestInput(t=0, request_id="r", work_units=2, mem_mb=2 if drop_at_zero else 1),
        ),
        settings=RunSettings(),
    )
    simulation, metrics, streams = execution(snapshot=saved)
    for frame in simulation.iter_ticks():
        append(frame=frame, metrics=metrics, streams=streams)
    report_bytes = canonical_bytes(value=metrics.summary())
    report = StoredFile(
        key="reports/metrics.json",
        sha256=hashlib.sha256(data=report_bytes).hexdigest(),
        size_bytes=len(report_bytes),
    )
    seal = capture_terminal_seal(
        snapshot=saved, simulation=simulation, metrics=metrics, streams=streams, report=report
    )
    payload = canonical_bytes(value=seal.model_dump(mode="json"))
    assert (
        read_terminal_seal(
            stream=io.BytesIO(initial_bytes=payload),
            expected_digest=hashlib.sha256(data=payload).hexdigest(),
            snapshot=saved,
        )
        == seal
    )
    if drop_at_zero:
        assert seal.terminal_tick == seal.executed_intervals == 0
    with pytest.raises(expected_exception=OperationError):
        read_terminal_seal(
            stream=io.BytesIO(initial_bytes=payload), expected_digest="0" * 64, snapshot=saved
        )
    raw = seal.model_dump(mode="json")
    raw["schema_version"] = 2
    changed = canonical_bytes(value=raw)
    with pytest.raises(expected_exception=OperationError):
        read_terminal_seal(
            stream=io.BytesIO(initial_bytes=changed),
            expected_digest=hashlib.sha256(data=changed).hexdigest(),
            snapshot=saved,
        )
