"""One fenced Docker execution/cleanup slot; interrupted real calls are never replayed."""

from datetime import timedelta
from typing import Literal
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import DockerManifest, DockerSnapshot
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.docker_models import (
    DockerControlSlot,
    DockerPublication,
    DockerResource,
    DockerRunState,
)
from mslb_persistence.docker_records import ControlLease
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import Attempt, Job, Run
from mslb_persistence.jobs import Jobs, _clock, _end_attempt
from mslb_persistence.records import JobRecord

RESOLVED = ("removed", "abandoned", "released")
TERMINAL = ("completed", "failed", "cancelled")
FAILURES = {
    "CONTROLLER_FAILURE",
    "INFRASTRUCTURE_FAILURE",
    "RESOURCE_PRESSURE",
    "STORAGE_LIMIT_EXCEEDED",
    "RESOURCE_LIMIT_EXCEEDED",
    "CONTAINER_CONFLICT",
    "INVALID_BACKEND",
    "INTERRUPTED",
}


def _duration(seconds: int) -> None:
    if type(seconds) is not int or not 1 <= seconds <= 300:
        raise StoreError(code="VALIDATION_ERROR", message="Invalid control lease duration")


async def current(
    session: AsyncSession, lease: ControlLease
) -> tuple[DockerControlSlot, Job, DockerRunState]:
    """Require the exact current control owner, phase and unexpired database lease."""
    slot = await session.scalar(
        statement=select(DockerControlSlot)
        .where(
            DockerControlSlot.id == 1,
            DockerControlSlot.run_id == lease.run_id,
            DockerControlSlot.fence == lease.fence,
            DockerControlSlot.owner == lease.owner,
            DockerControlSlot.phase == lease.phase,
            DockerControlSlot.lease_expires_at > func.clock_timestamp(),
        )
        .with_for_update()
    )
    if slot is None:
        raise StoreError(code="FENCE_LOST", message="Docker control lease is no longer current")
    job = await session.scalar(
        statement=select(Job)
        .where(Job.run_id == lease.run_id, Job.kind == "docker")
        .with_for_update()
    )
    state = await session.get(entity=DockerRunState, ident=lease.run_id)
    if job is None or state is None:
        raise StoreError(code="NOT_READY", message="Docker control references missing state")
    if job.status not in TERMINAL and (job.fence != lease.fence or job.owner != lease.owner):
        raise StoreError(code="FENCE_LOST", message="Docker execution ownership changed")
    return slot, job, state


async def guard(
    session: AsyncSession, lease: ControlLease, release: bool = False
) -> DockerControlSlot:
    """Recheck server-time ownership in the final mutation; roll back earlier changes on expiry."""
    statement = (
        update(table=DockerControlSlot)
        .where(
            DockerControlSlot.id == 1,
            DockerControlSlot.run_id == lease.run_id,
            DockerControlSlot.fence == lease.fence,
            DockerControlSlot.owner == lease.owner,
            DockerControlSlot.lease_expires_at > func.clock_timestamp(),
        )
        .values(revision=DockerControlSlot.revision + 1)
    )
    if release:
        statement = statement.values(
            run_id=None,
            owner=None,
            phase="idle",
            lease_expires_at=None,
            fence=DockerControlSlot.fence + 1,
            cleanup_attempts=0,
        )
    row = await session.scalar(statement=statement.returning(DockerControlSlot))
    if row is None:
        raise StoreError(code="FENCE_LOST", message="Docker control expired before commit")
    return row


def execution(lease: ControlLease, job: Job) -> None:
    if lease.phase != "running" or job.status != "running":
        raise StoreError(
            code="INVALID_STATE", message="Only active execution can increase Docker work"
        )


class DockerControl:
    """Serialize Docker ownership, interruption and bounded cleanup through one durable slot."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def cancel(self, run_id: UUID) -> JobRecord:
        return await Jobs(database=self.database).cancel(run_id=run_id, kind="docker")

    async def claim(self, owner: str, lease_seconds: int = 30) -> ControlLease | None:
        """Obtain exclusive ownership or claim expired work for cleanup, never blind replay."""
        _duration(seconds=lease_seconds)
        if not 1 <= len(owner.encode()) <= 128:
            raise StoreError(code="VALIDATION_ERROR", message="Invalid controller identity")

        async def operation(session: AsyncSession) -> ControlLease | None:
            slot = await session.scalar(
                statement=select(DockerControlSlot)
                .where(DockerControlSlot.id == 1)
                .with_for_update(skip_locked=True)
            )
            if slot is None:
                return None
            now = await _clock(session=session)
            if slot.run_id is not None:
                if slot.lease_expires_at is not None and slot.lease_expires_at > now:
                    return None
                job = await session.scalar(
                    statement=select(Job)
                    .where(Job.run_id == slot.run_id, Job.kind == "docker")
                    .with_for_update()
                )
                state = await session.get(entity=DockerRunState, ident=slot.run_id)
                if job is None or state is None:
                    raise StoreError(
                        code="NOT_READY", message="Docker slot references missing state"
                    )
                if job.status not in TERMINAL:
                    job.status = "cancelled" if job.status == "cancel_requested" else "failed"
                    job.error_code, job.completed_at = "INTERRUPTED", now
                    job.fence, job.owner, job.lease_expires_at = slot.fence + 1, None, None
                    state.desired_status, state.desired_error = job.status, "INTERRUPTED"
                    await _end_attempt(
                        session=session, job=job, outcome="controller_interrupted", now=now
                    )
                if slot.phase != "cleanup":
                    slot.cleanup_attempts = 0
                slot.phase = "cleanup"
                if slot.cleanup_attempts >= 3:
                    # Retain reservations and the expired-cleanup diagnostic.
                    return None
                slot.cleanup_attempts += 1
            else:
                job = await session.scalar(
                    statement=select(Job)
                    .where(Job.kind == "docker", Job.status == "queued", Job.attempts == 0)
                    .order_by(Job.created_at, Job.run_id)
                    .limit(limit=1)
                    .with_for_update(skip_locked=True)
                )
                if job is None:
                    return None
                slot.run_id, slot.phase, slot.cleanup_attempts = job.run_id, "running", 0
                job.status, job.attempts, job.error_code = "running", 1, None
            slot.fence += 1
            slot.owner, slot.lease_expires_at = owner, now + timedelta(seconds=lease_seconds)
            slot.revision += 1
            if slot.phase == "running":
                job.owner, job.fence, job.lease_expires_at = (
                    owner,
                    slot.fence,
                    slot.lease_expires_at,
                )
                session.add(
                    instance=Attempt(run_id=job.run_id, number=1, fence=slot.fence, owner=owner)
                )
            await session.flush()
            expires_at = slot.lease_expires_at
            if (
                slot.run_id is None
                or expires_at is None
                or slot.phase not in {"running", "cleanup"}
            ):
                raise StoreError(
                    code="NOT_READY", message="Claim did not establish Docker ownership"
                )
            return ControlLease(
                run_id=slot.run_id,
                fence=slot.fence,
                owner=owner,
                expires_at=expires_at,
                phase="running" if slot.phase == "running" else "cleanup",
            )

        return await self.database.transaction(operation=operation)

    async def renew(self, lease: ControlLease, lease_seconds: int = 30) -> ControlLease:
        """Renew the exact active controller fence using authoritative database time."""
        _duration(seconds=lease_seconds)

        async def operation(session: AsyncSession) -> ControlLease:
            slot, job, _state = await current(session=session, lease=lease)
            expires = await _clock(session=session) + timedelta(seconds=lease_seconds)
            if job.status not in TERMINAL:
                job.lease_expires_at = expires
            await guard(session=session, lease=lease)
            # The final lease-extension statement still tests the old slot expiry.
            updated = await session.scalar(
                statement=update(table=DockerControlSlot)
                .where(
                    DockerControlSlot.id == 1,
                    DockerControlSlot.run_id == lease.run_id,
                    DockerControlSlot.owner == lease.owner,
                    DockerControlSlot.fence == lease.fence,
                    DockerControlSlot.lease_expires_at > func.clock_timestamp(),
                )
                .values(lease_expires_at=expires)
                .returning(DockerControlSlot.id)
            )
            if updated is None:
                raise StoreError(code="FENCE_LOST", message="Docker lease expired before renewal")
            return ControlLease(
                run_id=lease.run_id,
                fence=slot.fence,
                owner=lease.owner,
                expires_at=expires,
                phase=lease.phase,
            )

        return await self.database.transaction(operation=operation)

    async def begin_cleanup(
        self,
        lease: ControlLease,
        outcome: Literal["completed", "failed", "cancelled"],
        error_code: str | None = None,
    ) -> ControlLease:
        """Persist terminal intent while retaining ownership of unresolved resources."""
        if (
            outcome not in {"completed", "failed", "cancelled"}
            or (outcome == "failed" and error_code not in FAILURES)
            or (outcome != "failed" and error_code is not None)
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid Docker terminal outcome")

        async def operation(session: AsyncSession) -> ControlLease:
            slot, job, state = await current(session=session, lease=lease)
            if lease.phase != "running" or job.status in TERMINAL:
                raise StoreError(code="INVALID_STATE", message="Docker execution is not active")
            if outcome == "cancelled" and job.status != "cancel_requested":
                raise StoreError(code="INVALID_STATE", message="Cancellation was not requested")
            state.desired_status = "cancelled" if job.status == "cancel_requested" else outcome
            state.desired_error = (
                "INTERRUPTED" if state.desired_status == "cancelled" else error_code
            )
            slot.phase, slot.cleanup_attempts = "cleanup", 1
            await guard(session=session, lease=lease)
            return ControlLease(
                run_id=lease.run_id,
                fence=lease.fence,
                owner=lease.owner,
                expires_at=slot.lease_expires_at or lease.expires_at,
                phase="cleanup",
            )

        return await self.database.transaction(operation=operation)

    async def retry_cleanup(self, run_id: UUID) -> None:
        """Explicit operator retry; automatic cleanup never exceeds three ownership attempts."""

        async def operation(session: AsyncSession) -> None:
            slot = await session.scalar(
                statement=select(DockerControlSlot)
                .where(
                    DockerControlSlot.id == 1,
                    DockerControlSlot.run_id == run_id,
                    DockerControlSlot.phase == "cleanup",
                    DockerControlSlot.lease_expires_at <= func.clock_timestamp(),
                )
                .with_for_update()
            )
            job = await session.get(entity=Job, ident=run_id)
            if slot is None or job is None or job.status not in TERMINAL:
                raise StoreError(
                    code="INVALID_STATE", message="Cleanup is not stalled after interruption"
                )
            slot.cleanup_attempts, slot.fence, slot.revision = 0, slot.fence + 1, slot.revision + 1

        await self.database.transaction(operation=operation)

    async def finish_cleanup(
        self, lease: ControlLease, manifest: DockerManifest | None = None
    ) -> JobRecord:
        """Release the slot only after bounded cleanup and definitive resource resolution."""
        if manifest is not None:
            manifest = DockerManifest.model_validate_json(
                json_data=canonical_bytes(value=manifest.model_dump(mode="json"))
            )

        async def operation(session: AsyncSession) -> JobRecord:
            _slot, job, state = await current(session=session, lease=lease)
            if lease.phase != "cleanup":
                raise StoreError(
                    code="INVALID_STATE", message="Cleanup must precede Docker finalization"
                )
            unresolved = await session.scalar(
                statement=select(func.count())
                .select_from(DockerResource)
                .where(
                    DockerResource.run_id == lease.run_id,
                    DockerResource.state.not_in(other=RESOLVED),
                )
            )
            if unresolved is None:
                raise StoreError(code="NOT_READY", message="Resource accounting is unavailable")
            if unresolved > 0:
                raise StoreError(
                    code="INVALID_STATE",
                    message="Unresolved Docker resources still reserve capacity",
                )
            status = (
                job.status
                if job.status in TERMINAL
                else "cancelled"
                if job.status == "cancel_requested"
                else state.desired_status
            )
            if status not in TERMINAL:
                raise StoreError(
                    code="INVALID_STATE", message="Docker outcome has not been selected"
                )
            if status == "completed":
                run = await session.get(entity=Run, ident=lease.run_id)
                snapshot = DockerSnapshot.model_validate_json(
                    json_data=canonical_bytes(value=state.snapshot)
                )
                if (
                    manifest is None
                    or run is None
                    or manifest.run_id != lease.run_id
                    or manifest.publication_fence != lease.fence
                    or manifest.snapshot_digest != run.snapshot_digest
                    or snapshot.digest() != run.snapshot_digest
                    or manifest.total != snapshot.request_count
                ):
                    raise StoreError(
                        code="VALIDATION_FAILED", message="Docker publication identity is invalid"
                    )
                session.add(
                    instance=DockerPublication(
                        run_id=lease.run_id,
                        manifest=manifest.model_dump(mode="json"),
                        digest=manifest.digest(),
                    )
                )
            if job.status not in TERMINAL:
                now = await _clock(session=session)
                job.status, job.completed_at = status, now
                job.error_code = "INTERRUPTED" if status == "cancelled" else state.desired_error
                job.fence, job.owner, job.lease_expires_at = job.fence + 1, None, None
                await _end_attempt(session=session, job=job, outcome=status, now=now)
            await guard(session=session, lease=lease, release=True)
            return JobRecord.from_model(row=job)

        return await self.database.transaction(operation=operation)
