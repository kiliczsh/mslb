"""Real PostgreSQL leases behavior in disposable schemas."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest
from mslb_persistence.database import StoreError
from mslb_persistence.models import (
    Attempt,
    IdempotencyRecord,
    Job,
    Outbox,
)
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from tests.integration.helpers import scratch
from tests.integration.persistence_helpers import expire, prepare


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
