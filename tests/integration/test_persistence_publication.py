"""Real PostgreSQL publication behavior in disposable schemas."""

import asyncio
from dataclasses import replace
from pathlib import Path

import pytest
from mslb_application.artifacts import verify_bundle
from mslb_application.snapshots import make_snapshot
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import RequestInput
from mslb_contracts.runs import BundleManifest, RunSettings
from mslb_persistence.database import StoreError
from mslb_persistence.durability import Durability
from mslb_persistence.models import (
    Attempt,
    CommittedChunk,
    IndexedEvent,
    OutputProgress,
    PublishedArtifact,
    SavedCheckpoint,
)
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from tests.integration.helpers import scratch
from tests.integration.persistence_helpers import OutputFixture, expire, prepare


def test_fenced_progress_keeps_two_checkpoints_and_requires_terminal_seal_for_completion(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            run = await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="producer")
            assert lease is not None
            flow = await asyncio.to_thread(
                OutputFixture, prepared=prepared, lease=lease, root=tmp_path
            )
            durable = Durability(database=database)
            committed = 0
            retired: list[StoredFile] = []
            candidate: StoredFile | None = None
            manifest: BundleManifest | None = None
            while not await asyncio.to_thread(flow.step):
                data = await asyncio.to_thread(flow.describe)
                retired.extend(await durable.commit(lease=lease, data=data))
                flow.acknowledge(commit=data)
                committed += 1
                recovery = await durable.recovery(lease=lease)
                assert (
                    recovery is not None
                    and recovery.kind == "checkpoint"
                    and recovery.next_tick == flow.simulation.tick
                )
                with pytest.raises(expected_exception=StoreError):
                    await durable.published(run_id=run.id)
                if candidate is None:
                    candidate, manifest = await asyncio.to_thread(flow.candidate)
                    with pytest.raises(expected_exception=StoreError, match="terminal seal"):
                        await durable.complete(lease=lease, reference=candidate, manifest=manifest)
            data = await asyncio.to_thread(flow.describe)
            await durable.commit(lease=lease, data=data)
            assert (await prepared.jobs.get(run_id=run.id)).status == "running"
            assert committed > 2 and len(retired) == committed - 2
            async with database.sessions() as session:
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(SavedCheckpoint)
                    )
                    == 2
                )
            assert candidate is not None and manifest is not None
            with pytest.raises(expected_exception=StoreError, match="byte identity"):
                await durable.complete(
                    lease=lease,
                    reference=candidate.model_copy(update={"sha256": "0" * 64}),
                    manifest=manifest,
                )
            async with database.sessions() as session:
                indexed = await session.get(entity=IndexedEvent, ident=(run.id, 1))
                assert indexed is not None
                original_id = indexed.request_id
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=IndexedEvent)
                    .where(IndexedEvent.run_id == run.id, IndexedEvent.sequence == 1)
                    .values(request_id="tampered-index")
                )
            with pytest.raises(expected_exception=StoreError, match="index"):
                await durable.complete(lease=lease, reference=candidate, manifest=manifest)
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=IndexedEvent)
                    .where(IndexedEvent.run_id == run.id, IndexedEvent.sequence == 1)
                    .values(request_id=original_id)
                )
            assert (
                await durable.complete(lease=lease, reference=candidate, manifest=manifest)
            ).status == "completed"
            assert await durable.published(run_id=run.id) == candidate
            async with database.sessions() as session:
                attempt = await session.get(entity=Attempt, ident=(run.id, 1))
                assert attempt is not None and attempt.outcome == "completed"
            with pytest.raises(expected_exception=StoreError):
                await durable.complete(lease=lease, reference=candidate, manifest=manifest)

    asyncio.run(main=scenario())


def test_committed_terminal_seal_survives_expiry_and_only_new_owner_can_publish(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            run = await prepared.enqueue()
            old = await prepared.jobs.claim(owner="old")
            assert old is not None
            flow = await asyncio.to_thread(
                OutputFixture, prepared=prepared, lease=old, root=tmp_path
            )
            while not await asyncio.to_thread(flow.step):
                pass
            durable = Durability(database=database)
            data = await asyncio.to_thread(flow.describe)
            await durable.commit(lease=old, data=data)
            old_candidate, manifest = await asyncio.to_thread(flow.candidate)
            await expire(database=database, lease=old)
            current = await prepared.jobs.claim(owner="new")
            assert current is not None and current.attempt == 2
            recovered = await durable.recovery(lease=current)
            assert (
                recovered is not None
                and recovered.kind == "terminal"
                and recovered.reference == data.reference
            )
            with pytest.raises(expected_exception=StoreError):
                await durable.complete(lease=old, reference=old_candidate, manifest=manifest)
            with pytest.raises(expected_exception=StoreError):
                await durable.complete(lease=current, reference=old_candidate, manifest=manifest)
            current_ref = StoredFile(
                key=f"runs/{run.id}/attempt-{current.fence}/bundles/{manifest.bundle_digest}/manifest.json",
                sha256=old_candidate.sha256,
                size_bytes=old_candidate.size_bytes,
            )

            def copy_candidate() -> None:
                path = flow.files.path(key=current_ref.key)
                path.parent.mkdir(parents=True)
                for source in flow.files.path(key=old_candidate.key).parent.iterdir():
                    (path.parent / source.name).write_bytes(data=source.read_bytes())
                assert verify_bundle(directory=path.parent) == manifest

            await asyncio.to_thread(copy_candidate)
            assert (
                await durable.complete(lease=current, reference=current_ref, manifest=manifest)
            ).status == "completed"

    asyncio.run(main=scenario())


@pytest.mark.parametrize(
    argnames="failure",
    argvalues=["gap", "namespace", "identity", "outage", "expired_during_commit"],
)
def test_failed_progress_transactions_never_expose_partial_chunks_or_events(
    tmp_path: Path, failure: str
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            await prepared.enqueue()
            lease = await prepared.jobs.claim(
                owner="producer", lease_seconds=1 if failure == "expired_during_commit" else 30
            )
            assert lease is not None
            flow = await asyncio.to_thread(
                OutputFixture, prepared=prepared, lease=lease, root=tmp_path
            )
            await asyncio.to_thread(flow.step)
            data = await asyncio.to_thread(flow.describe)
            if failure == "gap":
                data = data.model_copy(
                    update={
                        "chunks": (
                            data.chunks[0].model_copy(update={"first_sequence": 2}),
                            *data.chunks[1:],
                        )
                    }
                )
            elif failure == "namespace":
                data = data.model_copy(
                    update={
                        "reference": data.reference.model_copy(
                            update={"key": "runs/other/attempt-1/checkpoint.json"}
                        )
                    }
                )
            elif failure == "identity":
                data = data.model_copy(update={"snapshot_digest": "0" * 64})
            else:
                async with database.engine.begin() as connection:
                    action = (
                        "PERFORM pg_sleep(1.1); RETURN NEW;"
                        if failure == "expired_during_commit"
                        else "RAISE EXCEPTION 'injected storage transaction failure';"
                    )
                    await connection.execute(
                        statement=text(
                            text="CREATE FUNCTION test_progress_fault() RETURNS trigger "
                            "LANGUAGE plpgsql AS $$ BEGIN " + action + " END $$"
                        )
                    )
                    await connection.execute(
                        statement=text(
                            text="CREATE TRIGGER test_progress_fault BEFORE INSERT "
                            "ON committed_chunks "
                            "FOR EACH ROW EXECUTE FUNCTION test_progress_fault()"
                        )
                    )
            with pytest.raises(expected_exception=(StoreError, DBAPIError)):
                await Durability(database=database).commit(lease=lease, data=data)
            async with database.sessions() as session:
                for model in (
                    OutputProgress,
                    SavedCheckpoint,
                    CommittedChunk,
                    IndexedEvent,
                    PublishedArtifact,
                ):
                    assert (
                        await session.scalar(statement=select(func.count()).select_from(model)) == 0
                    )

    asyncio.run(main=scenario())


def test_cancellation_after_terminal_seal_prevents_artifact_publication(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            run = await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="producer")
            assert lease is not None
            flow = await asyncio.to_thread(
                OutputFixture, prepared=prepared, lease=lease, root=tmp_path
            )
            while not await asyncio.to_thread(flow.step):
                pass
            durable = Durability(database=database)
            await durable.commit(lease=lease, data=await asyncio.to_thread(flow.describe))
            candidate, manifest = await asyncio.to_thread(flow.candidate)
            await prepared.jobs.cancel(run_id=run.id)
            with pytest.raises(expected_exception=StoreError) as error:
                await durable.complete(lease=lease, reference=candidate, manifest=manifest)
            assert error.value.code == "INTERRUPTED"
            assert (await prepared.jobs.acknowledge_cancel(lease=lease)).status == "cancelled"
            async with database.sessions() as session:
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(PublishedArtifact)
                    )
                    == 0
                )

    asyncio.run(main=scenario())


def test_persisted_event_pagination_handles_more_than_501_events_without_gaps(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            requests = tuple(
                RequestInput(t=0, request_id=f"r-{n:04d}", work_units=10, mem_mb=1)
                for n in range(500)
            )
            snapshot = make_snapshot(
                servers=prepared.snapshot.servers,
                requests=requests,
                settings=RunSettings(mode="concurrent"),
            )
            dataset = await prepared.catalog.register_dataset(
                data=DatasetRegistration(
                    digest=snapshot.dataset_digest,
                    source_filename="burst.csv",
                    request_count=500,
                    earliest_arrival=0,
                    latest_arrival=0,
                    size_bytes=8000,
                )
            )
            prepared = replace(prepared, snapshot=snapshot, dataset=dataset)
            run = await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="producer")
            assert lease is not None
            flow = await asyncio.to_thread(
                OutputFixture, prepared=prepared, lease=lease, root=tmp_path
            )
            await asyncio.to_thread(flow.step)
            durable = Durability(database=database)
            await durable.commit(lease=lease, data=await asyncio.to_thread(flow.describe))
            first = await durable.events(run_id=run.id, limit=500, start_tick=0, end_tick=0)
            second = await durable.events(
                run_id=run.id, after_sequence=first[-1].sequence, limit=500
            )
            assert len(first) == 500 and len(second) == 3
            assert [event.sequence for event in (*first, *second)] == list(range(1, 504))
            with pytest.raises(expected_exception=StoreError):
                await durable.events(run_id=run.id, limit=501)

    asyncio.run(main=scenario())


def test_tick_zero_terminal_seal_completes_without_an_interval_checkpoint(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            snapshot = make_snapshot(
                servers=prepared.snapshot.servers,
                requests=(RequestInput(t=0, request_id="too-large", work_units=1, mem_mb=100_000),),
                settings=RunSettings(),
            )
            dataset = await prepared.catalog.register_dataset(
                data=DatasetRegistration(
                    digest=snapshot.dataset_digest,
                    source_filename="dropped.csv",
                    request_count=1,
                    earliest_arrival=0,
                    latest_arrival=0,
                    size_bytes=100,
                )
            )
            prepared = replace(prepared, snapshot=snapshot, dataset=dataset)
            run = await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="producer")
            assert lease is not None
            flow = await asyncio.to_thread(
                OutputFixture, prepared=prepared, lease=lease, root=tmp_path
            )
            assert await asyncio.to_thread(flow.step)
            data = await asyncio.to_thread(flow.describe)
            assert data.kind == "terminal" and data.next_tick == data.executed_intervals == 0
            durable = Durability(database=database)
            await durable.commit(lease=lease, data=data)
            async with database.sessions() as session:
                assert (
                    await session.scalar(
                        statement=select(func.count()).select_from(SavedCheckpoint)
                    )
                    == 0
                )
            reference, manifest = await asyncio.to_thread(flow.candidate)
            assert manifest.terminal_tick == 0 and manifest.event_count == 2
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=IndexedEvent)
                    .where(IndexedEvent.run_id == run.id, IndexedEvent.sequence == 2)
                    .values(reason="QUEUE_CAPACITY")
                )
            with pytest.raises(expected_exception=StoreError, match="index"):
                await durable.complete(lease=lease, reference=reference, manifest=manifest)
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=IndexedEvent)
                    .where(IndexedEvent.run_id == run.id, IndexedEvent.sequence == 2)
                    .values(reason="UNSCHEDULABLE_MEMORY")
                )
            assert (
                await durable.complete(lease=lease, reference=reference, manifest=manifest)
            ).status == "completed"
            assert await durable.published(run_id=run.id) == reference

    asyncio.run(main=scenario())
