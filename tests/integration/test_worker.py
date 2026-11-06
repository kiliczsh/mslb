import asyncio
import io
import json
import os
import selectors
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from threading import Event
from uuid import UUID

import pytest
from mslb_application import journal
from mslb_application.artifacts import execute_bundle, verify_bundle
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.submissions import Submissions
from mslb_application.worker import Worker
from mslb_contracts.runs import BundleManifest, RunSettings, RunSnapshot
from mslb_engine.models import STRATEGIES, TickFrame
from mslb_engine.simulator import Simulation
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database
from mslb_persistence.durability import Durability
from mslb_persistence.jobs import Jobs
from mslb_persistence.records import JobRecord, RunRecord

from tests.integration.helpers import fixture_inputs, scratch


async def submission(
    database: Database, root: Path, source: bytes | None = None, settings: RunSettings | None = None
) -> tuple[ManagedFiles, ManagedFiles, RunRecord]:
    servers, _, _ = await asyncio.to_thread(fixture_inputs)
    catalog = Catalog(database)
    await catalog.seed_once(servers)
    inputs = await asyncio.to_thread(ManagedFiles, root / "inputs", StorageLimits(min_free_bytes=0))
    outputs = await asyncio.to_thread(
        ManagedFiles, root / "outputs", StorageLimits(min_free_bytes=0)
    )
    service = Submissions(catalog, inputs)
    payload = (
        source
        if source is not None
        else await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
    )
    dataset = await service.upload(io.BytesIO(payload), "requests.csv")
    run = await service.submit(dataset.id, settings or RunSettings())
    readonly = await asyncio.to_thread(
        ManagedFiles, inputs.root, StorageLimits(min_free_bytes=0), True
    )
    return readonly, outputs, run


@pytest.mark.parametrize("mode", ["concurrent", "sequential"])
@pytest.mark.parametrize("strategy", STRATEGIES)
@pytest.mark.parametrize("scaling", [False, True])
def test_worker_executes_queued_run_and_matches_independent_cli_bundle(
    tmp_path: Path, mode: str, strategy: str, scaling: bool
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            settings = RunSettings.model_validate_json(
                json.dumps(
                    dict(
                        mode=mode,
                        strategy=strategy,
                        autoscaling=dict(
                            enabled=scaling,
                            max_servers=3,
                            server_template=dict(
                                cpu_units_per_tick=10, mem_mb=1024, rate_limit_per_sec=2
                            ),
                        ),
                    )
                )
            )
            source = (
                await asyncio.to_thread(Path("data/examples/scaling-requests.csv").read_bytes)
                if scaling
                else None
            )
            inputs, outputs, run = await submission(database, tmp_path, source, settings)
            phases: list[str] = []
            worker = Worker(
                database, inputs, outputs, observer=lambda phase, lease, tick: phases.append(phase)
            )
            assert await worker.run_once()
            state = await Jobs(database).get(run.id)
            assert state.status == "completed", phases
            pointer = await Durability(database).published(run.id)
            manifest = await asyncio.to_thread(verify_bundle, outputs.path(pointer.key).parent)
            saved = await asyncio.to_thread(
                inputs.load_snapshot, run.snapshot_key, run.snapshot_digest
            )
            reference = await asyncio.to_thread(execute_bundle, saved, tmp_path / "reference")
            assert manifest == reference
            assert phases == ["claimed", "terminal_committed", "candidate_published", "completed"]
            assert not await worker.run_once()

    asyncio.run(scenario())


def test_worker_renews_during_validation_and_observer_failure_does_not_change_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = journal.validate_artifact_files

    def delayed(
        snapshot: RunSnapshot,
        directory: Path,
        terminal_tick: int,
        event_count: int,
        decision_count: int,
    ) -> BundleManifest:
        time.sleep(1.5)
        return original(snapshot, directory, terminal_tick, event_count, decision_count)

    # A real blocking validation phase runs in the execution thread while the
    # async supervisor renews a shorter-than-validation test lease.
    monkeypatch.setattr(journal, "validate_artifact_files", delayed)

    async def scenario() -> None:
        async with scratch() as database:
            inputs, outputs, run = await submission(database, tmp_path)

            def unavailable(phase: str, lease: object, tick: int) -> None:
                raise OSError("operational observer unavailable")

            worker = Worker(
                database,
                inputs,
                outputs,
                lease_seconds=2,
                renewal_seconds=0.3,
                observer=unavailable,
            )
            assert await worker.run_once()
            assert (await Jobs(database).get(run.id)).status == "completed"
            assert worker.dropped_notifications > 0

    asyncio.run(scenario())


def test_cancellation_during_validation_never_publishes_completed_pointer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = Event()
    original = journal.validate_artifact_files

    def delayed(
        snapshot: RunSnapshot,
        directory: Path,
        terminal_tick: int,
        event_count: int,
        decision_count: int,
    ) -> BundleManifest:
        entered.set()
        time.sleep(1)
        return original(snapshot, directory, terminal_tick, event_count, decision_count)

    monkeypatch.setattr(journal, "validate_artifact_files", delayed)

    async def scenario() -> None:
        async with scratch() as database:
            inputs, outputs, run = await submission(database, tmp_path)
            task = asyncio.create_task(Worker(database, inputs, outputs).run_once())
            assert await asyncio.to_thread(entered.wait, 5)
            await Jobs(database).cancel(run.id)
            await task
            assert (await Jobs(database).get(run.id)).status == "cancelled"
            with pytest.raises(ValueError):
                await Durability(database).published(run.id)

    asyncio.run(scenario())


def test_deadline_stops_execution_while_a_database_poll_is_still_cancelling(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release_validation, stopped = Event(), Event()
    original_validation = journal.validate_artifact_files

    def held_validation(
        snapshot: RunSnapshot,
        directory: Path,
        terminal_tick: int,
        event_count: int,
        decision_count: int,
    ) -> BundleManifest:
        assert release_validation.wait(5)
        return original_validation(snapshot, directory, terminal_tick, event_count, decision_count)

    monkeypatch.setattr(journal, "validate_artifact_files", held_validation)

    async def scenario() -> None:
        async with scratch() as database:
            inputs, outputs, run = await submission(database, tmp_path)
            cleanup_started, release_cleanup = asyncio.Event(), asyncio.Event()

            def observe(phase: str, lease: object, tick: int) -> None:
                if phase == "stopped_lease_lost":
                    stopped.set()

            worker = Worker(
                database, inputs, outputs, lease_seconds=2, renewal_seconds=0.3, observer=observe
            )
            original_get = worker.jobs.get

            async def blocked_get(run_id: UUID) -> JobRecord:
                try:
                    await asyncio.sleep(10)
                except asyncio.CancelledError:
                    cleanup_started.set()
                    await release_cleanup.wait()
                return await original_get(run_id)

            monkeypatch.setattr(worker.jobs, "get", blocked_get)
            task = asyncio.create_task(worker.run_once())
            try:
                await asyncio.wait_for(cleanup_started.wait(), timeout=4)
                release_validation.set()
                assert await asyncio.to_thread(stopped.wait, 3)
            finally:
                release_validation.set()
                release_cleanup.set()
                await task
            assert (await Jobs(database).get(run.id)).status == "running"
            with pytest.raises(ValueError):
                await Durability(database).published(run.id)

    asyncio.run(scenario())


def wait_phase(process: subprocess.Popen[bytes], phase: str) -> None:
    assert process.stdout is not None
    buffer = b""
    deadline = time.monotonic() + 15
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        while time.monotonic() < deadline:
            if not selector.select(timeout=0.25):
                continue
            data = os.read(process.stdout.fileno(), 4096)
            if not data:
                raise AssertionError("Worker exited before the requested durable phase")
            buffer += data
            assert len(buffer) <= 64 * 1024
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                if json.loads(line).get("event") == phase:
                    return
    raise AssertionError("Worker did not reach the requested durable phase")


def spawn_worker(
    code: str, schema: str, inputs: Path, outputs: Path, phase: str
) -> subprocess.Popen[bytes]:
    return subprocess.Popen(  # noqa: S603 -- fixed test-owned worker program and paths
        [sys.executable, "-c", code, schema, str(inputs), str(outputs), phase],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_graceful_shutdown_checkpoints_and_new_owner_resumes_identical_bytes(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            source = b"t,request_id,work_units,mem_mb\n0,long,20000,1\n"
            inputs, outputs, run = await submission(database, tmp_path, source)
            shutdown = Event()

            def observe(phase: str, lease: object, tick: int) -> None:
                if phase == "checkpoint_committed":
                    shutdown.set()

            first = Worker(database, inputs, outputs, observer=observe)
            await first.run_once(shutdown)
            state = await Jobs(database).get(run.id)
            assert state.status == "queued" and state.attempts == 1
            phases: list[str] = []
            second = Worker(
                database, inputs, outputs, observer=lambda phase, lease, tick: phases.append(phase)
            )
            await second.run_once()
            assert (await Jobs(database).get(run.id)).status == "completed"
            assert "recovered_checkpoint" in phases
            reference = await Durability(database).published(run.id)
            actual = await asyncio.to_thread(verify_bundle, outputs.path(reference.key).parent)
            saved = await asyncio.to_thread(
                inputs.load_snapshot, run.snapshot_key, run.snapshot_digest
            )
            expected = await asyncio.to_thread(execute_bundle, saved, tmp_path / "reference")
            assert actual == expected

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "phase",
    ["checkpoint_committed", "terminal_committed", "candidate_published", "scaling_checkpoint"],
)
def test_killed_worker_recovers_committed_bytes_and_sealed_runs_do_not_execute_again(
    tmp_path: Path, phase: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            target = "checkpoint_committed" if phase == "scaling_checkpoint" else phase
            source = (
                b"t,request_id,work_units,mem_mb\n0,long,20000,1\n"
                if phase == "checkpoint_committed"
                else None
            )
            settings = None
            if phase == "scaling_checkpoint":
                source = b"t,request_id,work_units,mem_mb\n" + b"".join(
                    f"0,r-{n:04d},1,1000\n".encode() for n in range(1000)
                )
                settings = RunSettings.model_validate_json(
                    await asyncio.to_thread(Path("data/examples/scaling-config.json").read_bytes)
                )
            inputs, outputs, run = await submission(database, tmp_path, source, settings)
            saved = await asyncio.to_thread(
                inputs.load_snapshot, run.snapshot_key, run.snapshot_digest
            )
            expected = await asyncio.to_thread(execute_bundle, saved, tmp_path / "reference")
            code = """
import asyncio, json, os, sys, time
from pathlib import Path
from mslb_application.worker import Worker
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_persistence.database import Database
async def main():
    database = Database(os.environ["TEST_DATABASE_URL"], sys.argv[1])
    limits = StorageLimits(min_free_bytes=0)
    def observe(phase, lease, tick):
        print(json.dumps({"event":phase}), flush=True)
        if phase == sys.argv[4]:
            time.sleep(30)
    worker = Worker(database, ManagedFiles(Path(sys.argv[2]), limits, True),
        ManagedFiles(Path(sys.argv[3]), limits), lease_seconds=2,
        renewal_seconds=0.3, observer=observe)
    try: await worker.run_once()
    finally: await database.close()
asyncio.run(main())
"""
            process = await asyncio.to_thread(
                spawn_worker, code, database.schema, inputs.root, outputs.root, target
            )
            try:
                await asyncio.to_thread(wait_phase, process, target)
                assert process.poll() is None
                process.kill()
                await asyncio.to_thread(process.wait, 5)
                assert process.returncode == -9
            finally:
                if process.poll() is None:
                    process.kill()
                await asyncio.to_thread(process.communicate, timeout=5)
            with pytest.raises(ValueError):
                await Durability(database).published(run.id)
            await asyncio.sleep(2.2)
            if target != "checkpoint_committed":

                def forbidden(self: Simulation) -> Iterator[TickFrame]:
                    raise AssertionError("A sealed run executed its simulation again")

                monkeypatch.setattr(Simulation, "iter_ticks", forbidden)
            phases: list[str] = []
            resumed = Worker(
                database, inputs, outputs, observer=lambda event, lease, tick: phases.append(event)
            )
            assert await resumed.run_once()
            assert (await Jobs(database).get(run.id)).status == "completed", phases
            expected_phase = (
                "recovered_checkpoint" if target == "checkpoint_committed" else "recovered_terminal"
            )
            assert expected_phase in phases
            reference = await Durability(database).published(run.id)
            actual = await asyncio.to_thread(verify_bundle, outputs.path(reference.key).parent)
            assert actual == expected

    asyncio.run(scenario())
