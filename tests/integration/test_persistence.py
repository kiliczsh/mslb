"""Real PostgreSQL transactions in disposable schemas owned only by this suite."""

import asyncio
import hashlib
import io
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from mslb_application.artifacts import execute_bundle
from mslb_application.files import FileReference, ManagedFiles, StorageLimits
from mslb_application.snapshots import make_snapshot
from mslb_application.submissions import Submissions
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.inputs import InputError, RequestInput, ServerInput, read_requests, read_servers
from mslb_contracts.runs import RunSettings, RunSnapshot
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database, StoreError
from mslb_persistence.jobs import Jobs
from mslb_persistence.migrations import configuration, migrate
from mslb_persistence.models import (
    Attempt,
    Base,
    Dataset,
    IdempotencyRecord,
    Job,
    Outbox,
    Run,
    ServerHistory,
)
from mslb_persistence.records import DatasetRecord, Lease, RunRecord
from sqlalchemy import Connection, func, select, text, update
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.schema import CreateSchema, DropSchema


@asynccontextmanager
async def scratch() -> AsyncGenerator[Database]:
    url = os.environ["TEST_DATABASE_URL"]
    schema = "mslb_test_" + uuid4().hex
    admin, database = Database(url), Database(url, schema)
    async with admin.engine.begin() as connection:
        await connection.execute(CreateSchema(schema))
    try:
        await migrate(database)
        yield database
    finally:
        await database.close()
        async with admin.engine.begin() as connection:
            await connection.execute(DropSchema(schema, cascade=True))
        await admin.close()


@dataclass(frozen=True, slots=True)
class Prepared:
    catalog: Catalog
    jobs: Jobs
    snapshot: RunSnapshot
    dataset: DatasetRecord
    versions: tuple[tuple[str, int], ...]

    async def enqueue(self, key: str | None = None) -> RunRecord:
        return await self.catalog.enqueue(
            self.dataset.id, self.snapshot, self.versions, self.snapshot.snapshot_digest, key
        )


def fixture_inputs() -> tuple[tuple[ServerInput, ...], tuple[RequestInput, ...], int]:
    with Path("/fixtures/core/servers.json").open("rb") as stream:
        servers = read_servers(stream)
    with Path("/fixtures/core/requests.csv").open("rb") as stream:
        requests = read_requests(stream)
    return servers, requests, Path("/fixtures/core/requests.csv").stat().st_size


async def prepare(database: Database) -> Prepared:
    servers, requests, size = await asyncio.to_thread(fixture_inputs)
    catalog, jobs = Catalog(database), Jobs(database)
    await catalog.seed_once(servers)
    snapshot = make_snapshot(servers, requests, RunSettings())
    dataset = await catalog.register_dataset(
        DatasetRegistration(
            digest=snapshot.dataset_digest,
            source_filename="requests.csv",
            request_count=len(requests),
            earliest_arrival=requests[0].t,
            latest_arrival=requests[-1].t,
            size_bytes=size,
        )
    )
    versions = tuple((server.id, server.version) for server in await catalog.list_servers())
    return Prepared(catalog, jobs, snapshot, dataset, versions)


async def expire(database: Database, lease: Lease) -> None:
    # Only this disposable test schema's lease is moved into the past; application
    # ownership checks still use PostgreSQL server time and real SQL predicates.
    async with database.engine.begin() as connection:
        await connection.execute(
            update(Job)
            .where(Job.run_id == lease.run_id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )


def test_migrations_are_repeatable_match_models_and_downgrade_cleanly() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            await migrate(database)
            async with database.engine.begin() as connection:
                assert (
                    await connection.scalar(text("SHOW transaction_isolation")) == "repeatable read"
                )
                consistent = await connection.run_sync(
                    lambda sync: (
                        compare_metadata(MigrationContext.configure(sync), Base.metadata) == []
                    )
                )
                assert consistent
                assert (
                    await connection.scalar(text("SELECT version_num FROM alembic_version"))
                    == "dataset_sources"
                )

                def downgrade(sync: Connection) -> None:
                    config = configuration()
                    config.attributes["connection"] = sync
                    command.downgrade(config, "base")

                await connection.run_sync(downgrade)
                count = await connection.scalar(
                    text(
                        "SELECT count(*) FROM information_schema.tables "
                        "WHERE table_schema = current_schema() "
                        "AND table_name <> 'alembic_version'"
                    )
                )
                assert count == 0
            await migrate(database)
            assert await database.ready()

    asyncio.run(scenario())


def test_server_versions_history_conflicts_and_seed_marker_survive_reconnection() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            original = (await prepared.catalog.list_servers())[0]
            edited = ServerInput(
                id=original.id, cpu_units_per_tick=19, mem_mb=88, rate_limit_per_sec=2
            )
            updated = await prepared.catalog.update_server(edited, original.version)
            assert updated.version == original.version + 1
            with pytest.raises(StoreError) as error:
                await prepared.catalog.update_server(edited, original.version)
            assert error.value.code == "CONFLICT"
            with pytest.raises(StoreError):
                await prepared.enqueue()
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(server.id, server.version)
            await database.engine.dispose()
            await prepared.catalog.seed_once(prepared.snapshot.servers)
            assert await prepared.catalog.list_servers() == ()
            recreated = await prepared.catalog.create_server(edited)
            assert recreated.version == updated.version + 2
            async with database.sessions() as session:
                history = tuple(
                    await session.scalars(
                        select(ServerHistory)
                        .where(ServerHistory.server_id == original.id)
                        .order_by(ServerHistory.version)
                    )
                )
                assert [row.version for row in history] == [1, 2, 3, 4]
                assert [row.deleted for row in history] == [False, False, True, False]

    asyncio.run(scenario())


def test_atomic_enqueue_idempotency_and_historical_snapshot_ignore_later_live_deletion() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            first = await prepared.enqueue("same-key")
            assert await prepared.enqueue("same-key") == first
            assert await prepared.catalog.get_run(first.id) == first
            with pytest.raises(StoreError, match="different request"):
                await prepared.catalog.enqueue(
                    prepared.dataset.id, prepared.snapshot, prepared.versions, "0" * 64, "same-key"
                )
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(server.id, server.version)
            assert await prepared.enqueue("same-key") == first
            assert (await prepared.jobs.get(first.id)).status == "queued"
            async with database.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Run)) == 1
                assert await session.scalar(select(func.count()).select_from(Job)) == 1
                assert await session.scalar(select(func.count()).select_from(Outbox)) == 1
                run = await session.get(Run, first.id)
                assert run is not None and run.server_versions == dict(prepared.versions)
                assert run.snapshot_digest == prepared.snapshot.snapshot_digest

    asyncio.run(scenario())


def test_failed_outbox_insert_rolls_back_run_job_and_idempotency_record() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            async with database.engine.begin() as connection:
                await connection.execute(
                    text("""CREATE FUNCTION reject_test_outbox() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN
                    RAISE EXCEPTION 'injected outbox failure'; END $$""")
                )
                await connection.execute(
                    text("""CREATE TRIGGER reject_test_outbox BEFORE INSERT ON outbox
                    FOR EACH ROW EXECUTE FUNCTION reject_test_outbox()""")
                )
            with pytest.raises(DBAPIError):
                await prepared.enqueue("rollback")
            async with database.sessions() as session:
                for model in (Run, Job, Outbox, IdempotencyRecord):
                    assert await session.scalar(select(func.count()).select_from(model)) == 0

    asyncio.run(scenario())


def test_two_workers_racing_cannot_claim_two_jobs_or_share_one_attempt() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            first, second = await prepared.enqueue(), await prepared.enqueue()
            claims = await asyncio.gather(
                prepared.jobs.claim("worker-a"), prepared.jobs.claim("worker-b")
            )
            owners = [lease for lease in claims if lease is not None]
            assert len(owners) == 1 and owners[0].run_id == first.id
            assert await prepared.jobs.claim("worker-c") is None
            assert (await prepared.jobs.get(second.id)).status == "queued"
            current = owners[0]
            assert (await prepared.jobs.renew(current)).status == "running"
            assert (await prepared.jobs.fail(current, "VALIDATION_FAILED")).status == "failed"
            next_lease = await prepared.jobs.claim("worker-b")
            assert next_lease is not None and next_lease.run_id == second.id
            for mutation in (
                prepared.jobs.renew(current),
                prepared.jobs.fail(current, "INTERNAL_ERROR"),
            ):
                with pytest.raises(StoreError) as error:
                    await mutation
                assert error.value.code == "FENCE_LOST"

    asyncio.run(scenario())


def test_expiry_requeues_with_new_fence_and_stale_workers_cannot_renew_or_finish() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            run = await prepared.enqueue()
            old = await prepared.jobs.claim("old-worker")
            assert old is not None
            await expire(database, old)
            with pytest.raises(StoreError):
                await prepared.jobs.renew(old)
            new = await prepared.jobs.claim("new-worker")
            assert (
                new is not None
                and new.run_id == run.id
                and new.attempt == 2
                and new.fence > old.fence
            )
            with pytest.raises(StoreError) as error:
                await prepared.jobs.fail(old, "INTERNAL_ERROR")
            assert error.value.code == "FENCE_LOST"
            assert (await prepared.jobs.renew(new)).status == "running"
            async with database.sessions() as session:
                previous = await session.get(Attempt, (run.id, 1))
                assert (
                    previous is not None
                    and previous.outcome == "lease_expired"
                    and previous.finished_at is not None
                )

    asyncio.run(scenario())


@pytest.mark.parametrize("expired", [False, True])
def test_cancellation_wins_over_failure_and_expired_owner_recovery(expired: bool) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            queued = await prepared.enqueue()
            assert (await prepared.jobs.cancel(queued.id)).status == "cancelled"
            with pytest.raises(StoreError) as error:
                await prepared.jobs.cancel(queued.id)
            assert error.value.code == "INVALID_STATE"
            running = await prepared.enqueue()
            lease = await prepared.jobs.claim("worker")
            assert lease is not None and lease.run_id == running.id
            assert (await prepared.jobs.cancel(running.id)).status == "cancel_requested"
            assert (await prepared.jobs.cancel(running.id)).status == "cancel_requested"
            if expired:
                await expire(database, lease)
                await prepared.jobs.reap_expired()
            else:
                assert (await prepared.jobs.renew(lease)).status == "cancel_requested"
                await prepared.jobs.fail(lease, "INFRASTRUCTURE_FAILURE", retryable=True)
            result = await prepared.jobs.get(running.id)
            assert result.status == "cancelled" and result.error_code == "INTERRUPTED"
            assert await prepared.jobs.claim("next") is None

    asyncio.run(scenario())


def test_infrastructure_retries_stop_at_three_attempts_and_pressure_does_not_retry() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            run = await prepared.enqueue()
            for number in range(1, 4):
                lease = await prepared.jobs.claim("worker")
                assert lease is not None and lease.attempt == number
                failed = await prepared.jobs.fail(lease, "INFRASTRUCTURE_FAILURE", retryable=True)
                assert failed.status == ("failed" if number == 3 else "queued")
            assert (await prepared.jobs.get(run.id)).attempts == 3
            assert await prepared.jobs.claim("worker") is None
            await prepared.enqueue()
            lease = await prepared.jobs.claim("worker")
            assert lease is not None
            with pytest.raises(StoreError):
                await prepared.jobs.fail(lease, "RESOURCE_PRESSURE", retryable=True)
            assert (await prepared.jobs.fail(lease, "RESOURCE_PRESSURE")).status == "failed"

    asyncio.run(scenario())


def test_queue_ceiling_idempotency_expiry_and_db_polling_without_redis() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            original = await prepared.enqueue("expires")
            for _ in range(19):
                await prepared.enqueue()
            with pytest.raises(StoreError) as error:
                await prepared.enqueue()
            assert error.value.code == "RATE_LIMITED"
            assert await prepared.enqueue("expires") == original
            await prepared.jobs.cancel(original.id)
            async with database.engine.begin() as connection:
                await connection.execute(
                    update(IdempotencyRecord).values(
                        expires_at=func.clock_timestamp() - timedelta(seconds=1)
                    )
                )
            replacement = await prepared.enqueue("expires")
            assert replacement.id != original.id
            pending = await prepared.jobs.pending_outbox(limit=500)
            assert len(pending) == 21
            for message, _ in pending:
                await prepared.jobs.mark_outbox_delivered(message)
            assert await prepared.jobs.pending_outbox() == ()
            assert await prepared.jobs.claim("db-only") is not None
            async with database.engine.begin() as connection:
                await connection.execute(
                    update(Outbox).values(
                        created_at=func.clock_timestamp() - timedelta(hours=25), delivered_at=None
                    )
                )
            await prepared.jobs.prune_outbox()
            assert await prepared.jobs.pending_outbox() == ()
            # Hint expiration cannot remove the durable queued/running jobs.
            async with database.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Job)) == 21

    asyncio.run(scenario())


def test_owner_attempt_and_fence_all_participate_in_mutation_predicates() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            await prepared.enqueue()
            lease = await prepared.jobs.claim("owner")
            assert lease is not None
            for stale in (
                replace(lease, owner="other"),
                replace(lease, attempt=2),
                replace(lease, fence=lease.fence + 1),
                replace(lease, run_id=uuid4()),
            ):
                with pytest.raises(StoreError) as error:
                    await prepared.jobs.renew(stale)
                assert error.value.code == "FENCE_LOST"
            with pytest.raises(StoreError):
                await prepared.jobs.acknowledge_cancel(lease)
            await prepared.jobs.cancel(lease.run_id)
            assert (await prepared.jobs.acknowledge_cancel(lease)).status == "cancelled"

    asyncio.run(scenario())


def test_database_constraints_reject_invalid_lease_shapes() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            run = await prepared.enqueue()
            with pytest.raises(IntegrityError):
                async with database.engine.begin() as connection:
                    await connection.execute(
                        update(Job).where(Job.run_id == run.id).values(owner="invalid")
                    )

    asyncio.run(scenario())


def test_snapshot_integrity_and_dataset_metadata_fail_before_job_creation() -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            with pytest.raises(InputError):
                await prepared.catalog.enqueue(
                    prepared.dataset.id,
                    prepared.snapshot.model_copy(update={"snapshot_digest": "0" * 64}),
                    prepared.versions,
                    prepared.snapshot.snapshot_digest,
                )
            with pytest.raises(StoreError):
                await prepared.catalog.register_dataset(
                    DatasetRegistration(
                        digest=prepared.dataset.digest,
                        source_filename="other.csv",
                        request_count=1,
                        earliest_arrival=0,
                        latest_arrival=0,
                        size_bytes=1,
                    )
                )
            async with database.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Run)) == 0
            with pytest.raises(StoreError):
                await prepared.catalog.get_run(uuid4())

    asyncio.run(scenario())


def test_submission_persists_verified_inputs_before_job_and_frozen_run_is_executable(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            files = await asyncio.to_thread(ManagedFiles, tmp_path, StorageLimits(min_free_bytes=0))
            service = Submissions(prepared.catalog, files)
            source = b"request_id,t,work_units,mem_mb,extra\r\nr2,1,3,2,x\r\nr1,0,4,1,y\r\n"
            dataset = await service.upload(io.BytesIO(source), "../../source.csv")
            assert dataset.original_digest == hashlib.sha256(source).hexdigest()
            assert dataset.source_size_bytes == len(source)
            settings = RunSettings(strategy="round_robin")
            run = await service.submit(dataset.id, settings, "durable-submission")
            saved = await asyncio.to_thread(
                files.load_snapshot, run.snapshot_key, run.snapshot_digest
            )
            assert (await prepared.jobs.get(run.id)).status == "queued"
            for server in await prepared.catalog.list_servers():
                await prepared.catalog.delete_server(server.id, server.version)
            assert await service.submit(dataset.id, settings, "durable-submission") == run
            with pytest.raises(StoreError, match="different request"):
                await service.submit(dataset.id, RunSettings(), "durable-submission")
            manifest = await asyncio.to_thread(execute_bundle, saved, tmp_path / "verified-result")
            assert manifest.event_count == 6 and manifest.independent_replay == "passed"
            original = FileReference(
                f"uploads/{dataset.original_digest}.csv", str(dataset.original_digest), len(source)
            )

            def read_original() -> bytes:
                with files.verified_stream(original) as stream:
                    return stream.read()

            assert await asyncio.to_thread(read_original) == source

    asyncio.run(scenario())


def test_concurrent_idempotent_submissions_share_one_durable_job(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            files = await asyncio.to_thread(ManagedFiles, tmp_path, StorageLimits(min_free_bytes=0))
            service = Submissions(prepared.catalog, files)
            dataset = await service.upload(
                io.BytesIO(b"t,request_id,work_units,mem_mb\n0,r,1,1\n"), "request.csv"
            )
            runs = await asyncio.gather(
                service.submit(dataset.id, RunSettings(), "same"),
                service.submit(dataset.id, RunSettings(), "same"),
            )
            assert runs[0] == runs[1]
            async with database.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Job)) == 1
                assert await session.scalar(select(func.count()).select_from(Outbox)) == 1

    asyncio.run(scenario())


def test_snapshot_storage_failure_does_not_register_a_job_or_replace_dataset(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            files = await asyncio.to_thread(ManagedFiles, tmp_path, StorageLimits(min_free_bytes=0))
            service = Submissions(prepared.catalog, files)
            dataset = await service.upload(
                io.BytesIO(b"t,request_id,work_units,mem_mb\n0,r,1,1\n"), "request.csv"
            )
            await asyncio.to_thread((tmp_path / "snapshots").chmod, 0o500)
            try:
                with pytest.raises(PermissionError):
                    await service.submit(dataset.id, RunSettings())
            finally:
                await asyncio.to_thread((tmp_path / "snapshots").chmod, 0o700)
            async with database.sessions() as session:
                assert await session.scalar(select(func.count()).select_from(Job)) == 0
                assert await session.scalar(select(func.count()).select_from(Run)) == 0
            assert await prepared.catalog.get_dataset(dataset.id) == dataset

    asyncio.run(scenario())


def test_reimport_supplies_real_source_provenance_for_legacy_metadata(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            prepared = await prepare(database)
            assert prepared.dataset.original_digest is None
            files = await asyncio.to_thread(ManagedFiles, tmp_path, StorageLimits(min_free_bytes=0))
            service = Submissions(prepared.catalog, files)
            source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
            current = await service.upload(io.BytesIO(source), "reimported.csv")
            assert current.id == prepared.dataset.id
            assert current.original_digest == hashlib.sha256(source).hexdigest()
            assert current.source_size_bytes == len(source)
            async with database.engine.begin() as connection:
                with pytest.raises(IntegrityError):
                    await connection.execute(
                        update(Dataset)
                        .where(Dataset.id == current.id)
                        .values(source_size_bytes=None)
                    )

    asyncio.run(scenario())
