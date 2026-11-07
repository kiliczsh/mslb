"""Real PostgreSQL catalog behavior in disposable schemas."""

import asyncio

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from mslb_contracts.inputs import ServerInput
from mslb_persistence.database import StoreError
from mslb_persistence.migrations import configuration, migrate
from mslb_persistence.models import (
    Base,
    IdempotencyRecord,
    Job,
    Outbox,
    Run,
    ServerHistory,
)
from sqlalchemy import Connection, func, select, text
from sqlalchemy.exc import DBAPIError

from tests.integration.helpers import scratch
from tests.integration.persistence_helpers import prepare


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
                    == "docker_progress"
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
