"""Real PostgreSQL transactions in disposable schemas owned only by this suite."""

import asyncio
import hashlib
import io
import json
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from mslb_application.artifacts import execute_bundle, timeline_record, verify_bundle
from mslb_application.checkpoints import PREFIX_NAMES, capture_terminal_seal, write_checkpoint
from mslb_application.files import FileReference, ManagedFiles, StorageLimits
from mslb_application.snapshots import canonical_bytes, domain_config, domain_inputs, make_snapshot
from mslb_application.submissions import Submissions
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.checkpoints import ArtifactPrefix
from mslb_contracts.durability import OutputChunk, PersistedEvent, ProgressCommit, StoredFile
from mslb_contracts.inputs import InputError, RequestInput, ServerInput
from mslb_contracts.runs import BundleManifest, RunSettings, RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.simulator import Simulation
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database, StoreError
from mslb_persistence.durability import Durability
from mslb_persistence.jobs import Jobs
from mslb_persistence.migrations import configuration, migrate
from mslb_persistence.models import (
    Attempt,
    Base,
    CommittedChunk,
    Dataset,
    IdempotencyRecord,
    IndexedEvent,
    Job,
    Outbox,
    OutputProgress,
    PublishedArtifact,
    Run,
    SavedCheckpoint,
    ServerHistory,
)
from mslb_persistence.records import DatasetRecord, Lease, RunRecord
from sqlalchemy import Connection, func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.helpers import fixture_inputs, scratch


@dataclass(frozen=True, slots=True)
class Prepared:
    catalog: Catalog
    jobs: Jobs
    snapshot: RunSnapshot
    dataset: DatasetRecord
    versions: tuple[tuple[str, int], ...]

    async def enqueue(self, key: str | None = None) -> RunRecord:
        return await self.catalog.enqueue(
            dataset_id=self.dataset.id,
            snapshot=self.snapshot,
            versions=self.versions,
            body_digest=self.snapshot.snapshot_digest,
            key=key,
        )


async def prepare(database: Database) -> Prepared:
    servers, requests, size = await asyncio.to_thread(fixture_inputs)
    catalog, jobs = Catalog(database=database), Jobs(database=database)
    await catalog.seed_once(servers=servers)
    snapshot = make_snapshot(servers=servers, requests=requests, settings=RunSettings())
    dataset = await catalog.register_dataset(
        data=DatasetRegistration(
            digest=snapshot.dataset_digest,
            source_filename="requests.csv",
            request_count=len(requests),
            earliest_arrival=requests[0].t,
            latest_arrival=requests[-1].t,
            size_bytes=size,
        )
    )
    versions = tuple((server.id, server.version) for server in await catalog.list_servers())
    return Prepared(
        catalog=catalog, jobs=jobs, snapshot=snapshot, dataset=dataset, versions=versions
    )


async def expire(database: Database, lease: Lease) -> None:
    # Only this disposable test schema's lease is moved into the past; application
    # ownership checks still use PostgreSQL server time and real SQL predicates.
    async with database.engine.begin() as connection:
        await connection.execute(
            statement=update(table=Job)
            .where(Job.run_id == lease.run_id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )


def test_migrations_are_repeatable_match_models_and_downgrade_cleanly() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            await migrate(database=database)
            async with database.engine.begin() as connection:
                assert (
                    await connection.scalar(statement=text(text="SHOW transaction_isolation"))
                    == "repeatable read"
                )
                consistent = await connection.run_sync(
                    fn=lambda sync: (
                        compare_metadata(
                            context=MigrationContext.configure(connection=sync),
                            metadata=Base.metadata,
                        )
                        == []
                    )
                )
                assert consistent
                assert (
                    await connection.scalar(
                        statement=text(text="SELECT version_num FROM alembic_version")
                    )
                    == "bounded_stress_profile"
                )

                def downgrade(sync: Connection) -> None:
                    config = configuration()
                    config.attributes["connection"] = sync
                    command.downgrade(config=config, revision="base")

                await connection.run_sync(fn=downgrade)
                count = await connection.scalar(
                    statement=text(
                        text="SELECT count(*) FROM information_schema.tables "
                        "WHERE table_schema = current_schema() "
                        "AND table_name <> 'alembic_version'"
                    )
                )
                assert count == 0
            await migrate(database=database)
            assert await database.ready()

    asyncio.run(main=scenario())


def test_server_versions_history_conflicts_and_seed_marker_survive_reconnection() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            original = (await prepared.catalog.list_servers())[0]
            edited = ServerInput(
                id=original.id, cpu_units_per_tick=19, mem_mb=88, rate_limit_per_sec=2
            )
            updated = await prepared.catalog.update_server(
                server=edited, expected_version=original.version
            )
            assert updated.version == original.version + 1
            with pytest.raises(expected_exception=StoreError) as error:
                await prepared.catalog.update_server(
                    server=edited, expected_version=original.version
                )
            assert error.value.code == "CONFLICT"
            with pytest.raises(expected_exception=StoreError):
                await prepared.enqueue()
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(
                    server_id=server.id, expected_version=server.version
                )
            await database.engine.dispose()
            await prepared.catalog.seed_once(servers=prepared.snapshot.servers)
            assert await prepared.catalog.list_servers() == ()
            recreated = await prepared.catalog.create_server(server=edited)
            assert recreated.version == updated.version + 2
            async with database.sessions() as session:
                history = tuple(
                    await session.scalars(
                        statement=select(ServerHistory)
                        .where(ServerHistory.server_id == original.id)
                        .order_by(ServerHistory.version)
                    )
                )
                assert [row.version for row in history] == [1, 2, 3, 4]
                assert [row.deleted for row in history] == [False, False, True, False]

    asyncio.run(main=scenario())


def test_atomic_enqueue_idempotency_and_historical_snapshot_ignore_later_live_deletion() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            first = await prepared.enqueue(key="same-key")
            assert await prepared.enqueue(key="same-key") == first
            assert await prepared.catalog.get_run(run_id=first.id) == first
            with pytest.raises(expected_exception=StoreError, match="different request"):
                await prepared.catalog.enqueue(
                    dataset_id=prepared.dataset.id,
                    snapshot=prepared.snapshot,
                    versions=prepared.versions,
                    body_digest="0" * 64,
                    key="same-key",
                )
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(
                    server_id=server.id, expected_version=server.version
                )
            assert await prepared.enqueue(key="same-key") == first
            assert (await prepared.jobs.get(run_id=first.id)).status == "queued"
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Run)) == 1
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 1
                assert await session.scalar(statement=select(func.count()).select_from(Outbox)) == 1
                run = await session.get(entity=Run, ident=first.id)
                assert run is not None and run.server_versions == dict(prepared.versions)
                assert run.snapshot_digest == prepared.snapshot.snapshot_digest

    asyncio.run(main=scenario())


def test_failed_outbox_insert_rolls_back_run_job_and_idempotency_record() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=text(
                        text="""CREATE FUNCTION reject_test_outbox() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                    RAISE EXCEPTION 'injected outbox failure'; END $$"""
                    )
                )
                await connection.execute(
                    statement=text(
                        text="""CREATE TRIGGER reject_test_outbox BEFORE INSERT ON outbox
                    FOR EACH ROW EXECUTE FUNCTION reject_test_outbox()"""
                    )
                )
            with pytest.raises(expected_exception=DBAPIError):
                await prepared.enqueue(key="rollback")
            async with database.sessions() as session:
                for model in (Run, Job, Outbox, IdempotencyRecord):
                    assert (
                        await session.scalar(statement=select(func.count()).select_from(model)) == 0
                    )

    asyncio.run(main=scenario())


def test_two_workers_racing_cannot_claim_two_jobs_or_share_one_attempt() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            first, second = await prepared.enqueue(), await prepared.enqueue()
            claims = await asyncio.gather(
                prepared.jobs.claim(owner="worker-a"), prepared.jobs.claim(owner="worker-b")
            )
            owners = [lease for lease in claims if lease is not None]
            assert len(owners) == 1 and owners[0].run_id == first.id
            assert await prepared.jobs.claim(owner="worker-c") is None
            assert (await prepared.jobs.get(run_id=second.id)).status == "queued"
            current = owners[0]
            assert (await prepared.jobs.renew(lease=current)).status == "running"
            assert (
                await prepared.jobs.fail(lease=current, error_code="VALIDATION_FAILED")
            ).status == "failed"
            next_lease = await prepared.jobs.claim(owner="worker-b")
            assert next_lease is not None and next_lease.run_id == second.id
            for mutation in (
                prepared.jobs.renew(lease=current),
                prepared.jobs.fail(lease=current, error_code="INTERNAL_ERROR"),
            ):
                with pytest.raises(expected_exception=StoreError) as error:
                    await mutation
                assert error.value.code == "FENCE_LOST"

    asyncio.run(main=scenario())


def test_expiry_requeues_with_new_fence_and_stale_workers_cannot_renew_or_finish() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            run = await prepared.enqueue()
            old = await prepared.jobs.claim(owner="old-worker")
            assert old is not None
            await expire(database=database, lease=old)
            with pytest.raises(expected_exception=StoreError):
                await prepared.jobs.renew(lease=old)
            new = await prepared.jobs.claim(owner="new-worker")
            assert (
                new is not None
                and new.run_id == run.id
                and new.attempt == 2
                and new.fence > old.fence
            )
            with pytest.raises(expected_exception=StoreError) as error:
                await prepared.jobs.fail(lease=old, error_code="INTERNAL_ERROR")
            assert error.value.code == "FENCE_LOST"
            assert (await prepared.jobs.renew(lease=new)).status == "running"
            async with database.sessions() as session:
                previous = await session.get(entity=Attempt, ident=(run.id, 1))
                assert (
                    previous is not None
                    and previous.outcome == "lease_expired"
                    and previous.finished_at is not None
                )

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="expired", argvalues=[False, True])
def test_cancellation_wins_over_failure_and_expired_owner_recovery(expired: bool) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            queued = await prepared.enqueue()
            assert (await prepared.jobs.cancel(run_id=queued.id)).status == "cancelled"
            with pytest.raises(expected_exception=StoreError) as error:
                await prepared.jobs.cancel(run_id=queued.id)
            assert error.value.code == "INVALID_STATE"
            running = await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="worker")
            assert lease is not None and lease.run_id == running.id
            assert (await prepared.jobs.cancel(run_id=running.id)).status == "cancel_requested"
            assert (await prepared.jobs.cancel(run_id=running.id)).status == "cancel_requested"
            if expired:
                await expire(database=database, lease=lease)
                await prepared.jobs.reap_expired()
            else:
                assert (await prepared.jobs.renew(lease=lease)).status == "cancel_requested"
                await prepared.jobs.fail(
                    lease=lease, error_code="INFRASTRUCTURE_FAILURE", retryable=True
                )
            result = await prepared.jobs.get(run_id=running.id)
            assert result.status == "cancelled" and result.error_code == "INTERRUPTED"
            assert await prepared.jobs.claim(owner="next") is None

    asyncio.run(main=scenario())


def test_infrastructure_retries_stop_at_three_attempts_and_pressure_does_not_retry() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            run = await prepared.enqueue()
            for number in range(1, 4):
                lease = await prepared.jobs.claim(owner="worker")
                assert lease is not None and lease.attempt == number
                failed = await prepared.jobs.fail(
                    lease=lease, error_code="INFRASTRUCTURE_FAILURE", retryable=True
                )
                assert failed.status == ("failed" if number == 3 else "queued")
            assert (await prepared.jobs.get(run_id=run.id)).attempts == 3
            assert await prepared.jobs.claim(owner="worker") is None
            await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="worker")
            assert lease is not None
            with pytest.raises(expected_exception=StoreError):
                await prepared.jobs.fail(
                    lease=lease, error_code="RESOURCE_PRESSURE", retryable=True
                )
            assert (
                await prepared.jobs.fail(lease=lease, error_code="RESOURCE_PRESSURE")
            ).status == "failed"

    asyncio.run(main=scenario())


def test_queue_ceiling_idempotency_expiry_and_db_polling_without_redis() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            original = await prepared.enqueue(key="expires")
            for _ in range(19):
                await prepared.enqueue()
            with pytest.raises(expected_exception=StoreError) as error:
                await prepared.enqueue()
            assert error.value.code == "RATE_LIMITED"
            assert await prepared.enqueue(key="expires") == original
            await prepared.jobs.cancel(run_id=original.id)
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=IdempotencyRecord).values(
                        expires_at=func.clock_timestamp() - timedelta(seconds=1)
                    )
                )
            replacement = await prepared.enqueue(key="expires")
            assert replacement.id != original.id
            pending = await prepared.jobs.pending_outbox(limit=500)
            assert len(pending) == 21
            for message, _ in pending:
                await prepared.jobs.mark_outbox_delivered(message_id=message)
            assert await prepared.jobs.pending_outbox() == ()
            assert await prepared.jobs.claim(owner="db-only") is not None
            async with database.engine.begin() as connection:
                await connection.execute(
                    statement=update(table=Outbox).values(
                        created_at=func.clock_timestamp() - timedelta(hours=25), delivered_at=None
                    )
                )
            await prepared.jobs.prune_outbox()
            assert await prepared.jobs.pending_outbox() == ()
            # Hint expiration cannot remove the durable queued/running jobs.
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 21

    asyncio.run(main=scenario())


def test_owner_attempt_and_fence_all_participate_in_mutation_predicates() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            await prepared.enqueue()
            lease = await prepared.jobs.claim(owner="owner")
            assert lease is not None
            for stale in (
                replace(lease, owner="other"),
                replace(lease, attempt=2),
                replace(lease, fence=lease.fence + 1),
                replace(lease, run_id=uuid4()),
            ):
                with pytest.raises(expected_exception=StoreError) as error:
                    await prepared.jobs.renew(lease=stale)
                assert error.value.code == "FENCE_LOST"
            with pytest.raises(expected_exception=StoreError):
                await prepared.jobs.acknowledge_cancel(lease=lease)
            await prepared.jobs.cancel(run_id=lease.run_id)
            assert (await prepared.jobs.acknowledge_cancel(lease=lease)).status == "cancelled"

    asyncio.run(main=scenario())


def test_database_constraints_reject_invalid_lease_shapes() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            run = await prepared.enqueue()
            with pytest.raises(expected_exception=IntegrityError):
                async with database.engine.begin() as connection:
                    await connection.execute(
                        statement=update(table=Job)
                        .where(Job.run_id == run.id)
                        .values(owner="invalid")
                    )

    asyncio.run(main=scenario())


def test_snapshot_integrity_and_dataset_metadata_fail_before_job_creation() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            with pytest.raises(expected_exception=InputError):
                await prepared.catalog.enqueue(
                    dataset_id=prepared.dataset.id,
                    snapshot=prepared.snapshot.model_copy(update={"snapshot_digest": "0" * 64}),
                    versions=prepared.versions,
                    body_digest=prepared.snapshot.snapshot_digest,
                )
            with pytest.raises(expected_exception=StoreError):
                await prepared.catalog.register_dataset(
                    data=DatasetRegistration(
                        digest=prepared.dataset.digest,
                        source_filename="other.csv",
                        request_count=1,
                        earliest_arrival=0,
                        latest_arrival=0,
                        size_bytes=1,
                    )
                )
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Run)) == 0
            with pytest.raises(expected_exception=StoreError):
                await prepared.catalog.get_run(run_id=uuid4())

    asyncio.run(main=scenario())


def test_submission_persists_verified_inputs_before_job_and_frozen_run_is_executable(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            source = b"request_id,t,work_units,mem_mb,extra\r\nr2,1,3,2,x\r\nr1,0,4,1,y\r\n"
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=source), filename="../../source.csv"
            )
            assert dataset.original_digest == hashlib.sha256(data=source).hexdigest()
            assert dataset.source_size_bytes == len(source)
            settings = RunSettings(strategy="round_robin")
            run = await service.submit(
                dataset_id=dataset.id, settings=settings, key="durable-submission"
            )
            saved = await asyncio.to_thread(
                files.load_snapshot, key=run.snapshot_key, expected_digest=run.snapshot_digest
            )
            assert (await prepared.jobs.get(run_id=run.id)).status == "queued"
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(
                    server_id=server.id, expected_version=server.version
                )
            assert (
                await service.submit(
                    dataset_id=dataset.id, settings=settings, key="durable-submission"
                )
                == run
            )
            with pytest.raises(expected_exception=StoreError, match="different request"):
                await service.submit(
                    dataset_id=dataset.id, settings=RunSettings(), key="durable-submission"
                )
            manifest = await asyncio.to_thread(
                execute_bundle, snapshot=saved, destination=tmp_path / "verified-result"
            )
            assert manifest.event_count == 6 and manifest.independent_replay == "passed"
            original = FileReference(
                key=f"uploads/{dataset.original_digest}.csv",
                sha256=str(object=dataset.original_digest),
                size_bytes=len(source),
            )

            def read_original() -> bytes:
                with files.verified_stream(reference=original) as stream:
                    return stream.read()

            assert await asyncio.to_thread(read_original) == source

    asyncio.run(main=scenario())


def test_concurrent_idempotent_submissions_share_one_durable_job(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=b"t,request_id,work_units,mem_mb\n0,r,1,1\n"),
                filename="request.csv",
            )
            runs = await asyncio.gather(
                service.submit(dataset_id=dataset.id, settings=RunSettings(), key="same"),
                service.submit(dataset_id=dataset.id, settings=RunSettings(), key="same"),
            )
            assert runs[0] == runs[1]
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 1
                assert await session.scalar(statement=select(func.count()).select_from(Outbox)) == 1

    asyncio.run(main=scenario())


def test_snapshot_storage_failure_does_not_register_a_job_or_replace_dataset(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=b"t,request_id,work_units,mem_mb\n0,r,1,1\n"),
                filename="request.csv",
            )
            await asyncio.to_thread((tmp_path / "snapshots").chmod, mode=0o500)
            try:
                with pytest.raises(expected_exception=PermissionError):
                    await service.submit(dataset_id=dataset.id, settings=RunSettings())
            finally:
                await asyncio.to_thread((tmp_path / "snapshots").chmod, mode=0o700)
            async with database.sessions() as session:
                assert await session.scalar(statement=select(func.count()).select_from(Job)) == 0
                assert await session.scalar(statement=select(func.count()).select_from(Run)) == 0
            assert await prepared.catalog.get_dataset(dataset_id=dataset.id) == dataset

    asyncio.run(main=scenario())


def test_reimport_supplies_real_source_provenance_for_legacy_metadata(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database=database)
            assert prepared.dataset.original_digest is None
            files = await asyncio.to_thread(
                ManagedFiles, root=tmp_path, limits=StorageLimits(min_free_bytes=0)
            )
            service = Submissions(catalog=prepared.catalog, files=files)
            source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
            current = await service.upload(
                source=io.BytesIO(initial_bytes=source), filename="reimported.csv"
            )
            assert current.id == prepared.dataset.id
            assert current.original_digest == hashlib.sha256(data=source).hexdigest()
            assert current.source_size_bytes == len(source)
            async with database.engine.begin() as connection:
                with pytest.raises(expected_exception=IntegrityError):
                    await connection.execute(
                        statement=update(table=Dataset)
                        .where(Dataset.id == current.id)
                        .values(source_size_bytes=None)
                    )

    asyncio.run(main=scenario())


def stored(reference: FileReference) -> StoredFile:
    return StoredFile(key=reference.key, sha256=reference.sha256, size_bytes=reference.size_bytes)


class OutputFixture:
    """Small real engine/files producer for SQL protocol tests, not a worker."""

    def __init__(self, prepared: Prepared, lease: Lease, root: Path) -> None:
        self.snapshot, self.lease, self.root = prepared.snapshot, lease, root
        self.files = ManagedFiles(root=root / "files", limits=StorageLimits(min_free_bytes=0))
        servers, requests = domain_inputs(snapshot=prepared.snapshot)
        self.simulation = Simulation(
            servers=servers,
            requests=requests,
            config=domain_config(settings=prepared.snapshot.configuration),
        )
        self.metrics = MetricAccumulator(requests=requests, servers=servers)
        self.iterator = self.simulation.iter_ticks()
        self.streams: dict[str, BinaryIO] = {name: io.BytesIO() for name in PREFIX_NAMES}
        self.before = {
            name: ArtifactPrefix(
                name=name, size_bytes=0, records=0, sha256=hashlib.sha256(data=b"").hexdigest()
            )
            for name in PREFIX_NAMES
        }
        self.pending: list[PersistedEvent] = []
        self.prefix = f"runs/{lease.run_id}/attempt-{lease.fence}"

    def step(self) -> bool:
        frame = next(self.iterator)
        self.metrics.observe(frame=frame)
        self.streams["timeline.jsonl"].write(canonical_bytes(value=timeline_record(frame=frame)))
        for event in frame.events:
            self.streams["run.jsonl"].write(event.canonical_line().encode())
            sequence = self.before["run.jsonl"].records + len(self.pending) + 1
            self.pending.append(
                PersistedEvent.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "sequence": sequence,
                            "t": event.t,
                            "event": event.event,
                            "request_id": event.request_id,
                            "server_id": event.server_id,
                            "reason": event.reason,
                        }
                    )
                )
            )
            if event.reason is not None:
                self.streams["drop_reasons.jsonl"].write(
                    canonical_bytes(
                        value={"t": event.t, "request_id": event.request_id, "reason": event.reason}
                    )
                )
        if frame.scale_decision is not None:
            self.streams["autoscaling_decisions.jsonl"].write(
                frame.scale_decision.canonical_line().encode()
            )
        return frame.terminal

    def describe(self) -> ProgressCommit:
        report = None
        if self.simulation.terminal:
            report = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/metrics.json",
                    chunks=(canonical_bytes(value=self.metrics.summary()),),
                    maximum=256 * 1024**2,
                )
            )
            seal = capture_terminal_seal(
                snapshot=self.snapshot,
                simulation=self.simulation,
                metrics=self.metrics,
                streams=self.streams,
                report=report,
            )
            reference = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/terminal.json",
                    chunks=(canonical_bytes(value=seal.model_dump(mode="json")),),
                    maximum=1024**2,
                )
            )
            prefixes = seal.prefixes
        else:
            checkpoint = write_checkpoint(
                directory=self.root / "scratch",
                snapshot=self.snapshot,
                simulation=self.simulation,
                metrics=self.metrics,
                prefixes=self.streams,
            )
            with checkpoint.path.open(mode="rb") as stream:
                reference = stored(
                    reference=self.files.publish(
                        key=f"{self.prefix}/checkpoint-{checkpoint.next_tick}.json",
                        chunks=(stream.read(),),
                        maximum=32 * 1024**2,
                    )
                )
            prefixes = checkpoint.prefixes
        chunks: list[OutputChunk] = []
        for prefix in prefixes:
            before = self.before[prefix.name]
            if prefix.records == before.records:
                continue
            stream = self.streams[prefix.name]
            stream.seek(before.size_bytes)
            suffix = stream.read()
            stream.seek(0, io.SEEK_END)
            chunk = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/chunks/{prefix.name}-{before.records + 1}.jsonl",
                    chunks=(suffix,),
                    maximum=32 * 1024**2,
                )
            )
            ticks: list[int] = []
            for line in suffix.splitlines():
                tick = json.loads(s=line)["t"]
                assert isinstance(tick, int)
                ticks.append(tick)
            chunks.append(
                OutputChunk(
                    name=prefix.name,
                    file=chunk,
                    first_sequence=before.records + 1,
                    last_sequence=prefix.records,
                    byte_offset=before.size_bytes,
                    first_tick=min(ticks),
                    last_tick=max(ticks),
                )
            )
        return ProgressCommit(
            kind="terminal" if self.simulation.terminal else "checkpoint",
            reference=reference,
            snapshot_digest=self.snapshot.snapshot_digest,
            engine_version=self.snapshot.engine_version,
            next_tick=self.simulation.tick,
            executed_intervals=self.simulation.executed_intervals,
            prefixes=prefixes,
            metrics=report,
            chunks=tuple(chunks),
            events=tuple(self.pending),
        )

    def acknowledge(self, commit: ProgressCommit) -> None:
        self.before = {prefix.name: prefix for prefix in commit.prefixes}
        self.pending.clear()

    def candidate(self) -> tuple[StoredFile, BundleManifest]:
        staging = self.files.path(key=f"{self.prefix}/candidate")
        manifest = execute_bundle(snapshot=self.snapshot, destination=staging)
        final = self.files.path(key=f"{self.prefix}/bundles/{manifest.bundle_digest}")
        final.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(target=final)
        payload = (final / "manifest.json").read_bytes()
        return StoredFile(
            key=f"{self.prefix}/bundles/{manifest.bundle_digest}/manifest.json",
            sha256=hashlib.sha256(data=payload).hexdigest(),
            size_bytes=len(payload),
        ), manifest


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
