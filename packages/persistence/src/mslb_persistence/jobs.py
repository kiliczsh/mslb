"""One global logical execution slot, server-time leases, and fenced transitions."""

from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import and_, delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from mslb_persistence.database import Database
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import Attempt, ExecutionSlot, Job, Outbox
from mslb_persistence.records import JobRecord, Lease


def ownership_predicate(lease: Lease) -> ColumnElement[bool]:
    return and_(
        Job.kind == "logical",
        Job.run_id == lease.run_id,
        Job.attempts == lease.attempt,
        Job.fence == lease.fence,
        Job.owner == lease.owner,
        Job.status.in_(other=("running", "cancel_requested")),
        Job.lease_expires_at > func.clock_timestamp(),
    )


async def _clock(session: AsyncSession) -> datetime:
    now = await session.scalar(statement=select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise StoreError(code="NOT_READY", message="Database clock unavailable")
    return now


async def _slot(session: AsyncSession, skip_locked: bool = False) -> ExecutionSlot | None:
    return await session.scalar(
        statement=select(ExecutionSlot)
        .where(ExecutionSlot.id == 1)
        .with_for_update(skip_locked=skip_locked)
    )


async def _end_attempt(session: AsyncSession, job: Job, outcome: str, now: datetime) -> None:
    if job.attempts:
        attempt = await session.get(entity=Attempt, ident=(job.run_id, job.attempts))
        if attempt is None:
            raise StoreError(code="NOT_READY", message="Current job attempt is missing")
        attempt.finished_at, attempt.outcome = now, outcome


async def _expire(session: AsyncSession, slot: ExecutionSlot) -> None:
    """Resolve expired ownership with cancellation priority and bounded retry attempts."""
    if slot.run_id is None:
        return
    job = await session.scalar(
        statement=select(Job).where(Job.run_id == slot.run_id).with_for_update()
    )
    if job is None:
        raise StoreError(code="NOT_READY", message="Execution slot references missing job")
    now = await _clock(session=session)
    if job.lease_expires_at is not None and job.lease_expires_at > now:
        return
    if job.status not in {"running", "cancel_requested"}:
        raise StoreError(code="NOT_READY", message="Execution slot references inactive job")
    status = (
        "cancelled"
        if job.status == "cancel_requested"
        else "failed"
        if job.attempts >= 3
        else "queued"
    )
    await _end_attempt(session=session, job=job, outcome="lease_expired", now=now)
    result = await session.scalar(
        statement=update(table=Job)
        .where(
            Job.run_id == job.run_id,
            Job.fence == job.fence,
            Job.owner == job.owner,
            Job.attempts == job.attempts,
            Job.status == job.status,
            Job.lease_expires_at <= func.clock_timestamp(),
        )
        .values(
            status=status,
            fence=Job.fence + 1,
            owner=None,
            lease_expires_at=None,
            error_code="INTERRUPTED" if status == "cancelled" else "LEASE_EXPIRED",
            completed_at=now if status in {"failed", "cancelled"} else None,
        )
        .returning(Job)
    )
    if result is None:
        raise StoreError(code="FENCE_LOST", message="Expired job changed during reconciliation")
    slot.run_id = None


class Jobs:
    """Claim and transition durable logical jobs with leases, fencing and cancellation priority."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def get(self, run_id: UUID) -> JobRecord:
        async def operation(session: AsyncSession) -> JobRecord:
            job = await session.get(entity=Job, ident=run_id)
            if job is None:
                raise StoreError(code="NOT_FOUND", message="Run does not exist")
            return JobRecord.from_model(row=job)

        return await self.database.transaction(operation=operation)

    async def claim(self, owner: str, lease_seconds: int = 30) -> Lease | None:
        """Claim one logical job with a global slot, bounded attempts and a fresh fence."""
        if (
            not 1 <= len(owner.encode()) <= 128
            or type(lease_seconds) is not int
            or not 1 <= lease_seconds <= 300
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid worker lease settings")

        async def operation(session: AsyncSession) -> Lease | None:
            slot = await _slot(session=session, skip_locked=True)
            if slot is None:
                return None
            await _expire(session=session, slot=slot)
            if slot.run_id is not None:
                return None
            job = await session.scalar(
                statement=select(Job)
                .where(Job.kind == "logical", Job.status == "queued", Job.attempts < 3)
                .order_by(Job.created_at, Job.run_id)
                .limit(limit=1)
                .with_for_update(skip_locked=True)
            )
            if job is None:
                return None
            now = await _clock(session=session)
            job.status, job.attempts, job.fence = "running", job.attempts + 1, job.fence + 1
            job.owner, job.lease_expires_at, job.error_code = (
                owner,
                now + timedelta(seconds=lease_seconds),
                None,
            )
            slot.run_id = job.run_id
            session.add(
                instance=Attempt(
                    run_id=job.run_id, number=job.attempts, fence=job.fence, owner=owner
                )
            )
            await session.flush()
            expires = job.lease_expires_at
            if expires is None:
                raise StoreError(code="NOT_READY", message="Claim did not establish a lease")
            return Lease(
                run_id=job.run_id,
                attempt=job.attempts,
                fence=job.fence,
                owner=owner,
                expires_at=expires,
            )

        return await self.database.transaction(operation=operation)

    async def renew(self, lease: Lease, lease_seconds: int = 30) -> JobRecord:
        """Renew only the exact current owner before its authoritative lease expires."""
        if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
            raise StoreError(code="VALIDATION_ERROR", message="Invalid lease duration")

        async def operation(session: AsyncSession) -> JobRecord:
            job = await session.scalar(
                statement=update(table=Job)
                .where(ownership_predicate(lease=lease))
                .values(lease_expires_at=func.clock_timestamp() + timedelta(seconds=lease_seconds))
                .returning(Job)
            )
            if job is None:
                raise StoreError(code="FENCE_LOST", message="Worker lease is no longer current")
            return JobRecord.from_model(row=job)

        return await self.database.transaction(operation=operation)

    async def cancel(
        self, run_id: UUID, *, kind: Literal["logical", "docker"] = "logical"
    ) -> JobRecord:
        """Persist cancellation with priority over subsequent completion or expired ownership."""
        if kind not in {"logical", "docker"}:
            raise StoreError(code="VALIDATION_ERROR", message="Invalid run mode")

        async def operation(session: AsyncSession) -> JobRecord:
            job = await session.scalar(
                statement=select(Job)
                .where(Job.run_id == run_id, Job.kind == kind)
                .with_for_update()
            )
            if job is None:
                raise StoreError(code="NOT_FOUND", message="Run does not exist")
            if job.status == "queued":
                job.status, job.fence, job.completed_at = (
                    "cancelled",
                    job.fence + 1,
                    await _clock(session=session),
                )
                job.error_code = "INTERRUPTED"
            elif job.status == "running":
                job.status = "cancel_requested"
            elif job.status != "cancel_requested":
                raise StoreError(code="INVALID_STATE", message="Terminal run cannot be cancelled")
            await session.flush()
            return JobRecord.from_model(row=job)

        return await self.database.transaction(operation=operation)

    async def acknowledge_cancel(self, lease: Lease) -> JobRecord:
        return await self._finish(
            lease=lease, error_code="INTERRUPTED", retryable=False, cancellation_only=True
        )

    async def fail(self, lease: Lease, error_code: str, retryable: bool = False) -> JobRecord:
        """Record a safe failure or bounded retry only while the attempt fence remains valid."""
        allowed = {
            "INFRASTRUCTURE_FAILURE",
            "RESOURCE_PRESSURE",
            "SIMULATION_LIMIT_EXCEEDED",
            "RESOURCE_LIMIT_EXCEEDED",
            "STORAGE_LIMIT_EXCEEDED",
            "VALIDATION_FAILED",
            "INTERNAL_ERROR",
        }
        if error_code not in allowed or (retryable and error_code != "INFRASTRUCTURE_FAILURE"):
            raise StoreError(
                code="VALIDATION_ERROR",
                message="Only infrastructure failures may retry automatically",
            )
        return await self._finish(
            lease=lease, error_code=error_code, retryable=retryable, cancellation_only=False
        )

    async def _finish(
        self, lease: Lease, error_code: str, retryable: bool, cancellation_only: bool
    ) -> JobRecord:
        """Apply a terminal or retry transition only for the current live job owner."""

        async def operation(session: AsyncSession) -> JobRecord:
            slot = await _slot(session=session)
            if slot is None or slot.run_id != lease.run_id:
                raise StoreError(code="FENCE_LOST", message="Execution slot has another owner")
            job = await session.scalar(
                statement=select(Job).where(ownership_predicate(lease=lease)).with_for_update()
            )
            if job is None:
                raise StoreError(code="FENCE_LOST", message="Worker lease is no longer current")
            if cancellation_only and job.status != "cancel_requested":
                raise StoreError(code="INVALID_STATE", message="Cancellation was not requested")
            now = await _clock(session=session)
            status = (
                "cancelled"
                if job.status == "cancel_requested"
                else "queued"
                if retryable and job.attempts < 3
                else "failed"
            )
            code = "INTERRUPTED" if status == "cancelled" else error_code
            updated = await session.scalar(
                statement=update(table=Job)
                .where(ownership_predicate(lease=lease), Job.status == job.status)
                .values(
                    status=status,
                    fence=Job.fence + 1,
                    owner=None,
                    lease_expires_at=None,
                    error_code=code,
                    completed_at=now if status != "queued" else None,
                )
                .returning(Job)
            )
            if updated is None:
                raise StoreError(
                    code="FENCE_LOST", message="Worker lease expired before state commit"
                )
            await _end_attempt(session=session, job=updated, outcome=status, now=now)
            slot.run_id = None
            await session.flush()
            return JobRecord.from_model(row=updated)

        return await self.database.transaction(operation=operation)

    async def reap_expired(self) -> None:
        async def operation(session: AsyncSession) -> None:
            slot = await _slot(session=session, skip_locked=True)
            if slot is not None:
                await _expire(session=session, slot=slot)

        await self.database.transaction(operation=operation)

    async def pending_outbox(self, limit: int = 100) -> tuple[tuple[UUID, UUID], ...]:
        """Read bounded durable wake-up IDs without transferring job authority to Redis."""
        if type(limit) is not int or not 1 <= limit <= 500:
            raise StoreError(code="VALIDATION_ERROR", message="Invalid outbox page size")

        async def operation(session: AsyncSession) -> tuple[tuple[UUID, UUID], ...]:
            rows = await session.scalars(
                statement=select(Outbox)
                .where(Outbox.delivered_at.is_(other=None))
                .order_by(Outbox.created_at, Outbox.id)
                .limit(limit=limit)
            )
            return tuple((row.id, row.run_id) for row in rows)

        return await self.database.transaction(operation=operation)

    async def mark_outbox_delivered(self, message_id: UUID) -> None:
        async def operation(session: AsyncSession) -> None:
            await session.execute(
                statement=update(table=Outbox)
                .where(Outbox.id == message_id, Outbox.delivered_at.is_(other=None))
                .values(delivered_at=func.clock_timestamp())
            )

        await self.database.transaction(operation=operation)

    async def prune_outbox(self) -> None:
        async def operation(session: AsyncSession) -> None:
            # Old hints may expire even when Redis is unavailable. Jobs remain
            # authoritative and discoverable through PostgreSQL polling.
            ids = select(Outbox.id).where(
                Outbox.created_at < func.clock_timestamp() - timedelta(hours=24)
            )
            await session.execute(
                statement=delete(table=Outbox).where(Outbox.id.in_(other=ids.limit(limit=500)))
            )

        await self.database.transaction(operation=operation)
