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
        raise StoreError("VALIDATION_ERROR", "Invalid control lease duration")


async def current(
    session: AsyncSession, lease: ControlLease
) -> tuple[DockerControlSlot, Job, DockerRunState]:
    slot = await session.scalar(
        select(DockerControlSlot)
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
        raise StoreError("FENCE_LOST", "Docker control lease is no longer current")
    job = await session.scalar(
        select(Job).where(Job.run_id == lease.run_id, Job.kind == "docker").with_for_update()
    )
    state = await session.get(DockerRunState, lease.run_id)
    if job is None or state is None:
        raise StoreError("NOT_READY", "Docker control references missing state")
    if job.status not in TERMINAL and (job.fence != lease.fence or job.owner != lease.owner):
        raise StoreError("FENCE_LOST", "Docker execution ownership changed")
    return slot, job, state


async def guard(
    session: AsyncSession, lease: ControlLease, release: bool = False
) -> DockerControlSlot:
    """Recheck server-time ownership in the final mutation; roll back earlier changes on expiry."""
    statement = (
        update(DockerControlSlot)
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
    row = await session.scalar(statement.returning(DockerControlSlot))
    if row is None:
        raise StoreError("FENCE_LOST", "Docker control expired before commit")
    return row


def execution(lease: ControlLease, job: Job) -> None:
    if lease.phase != "running" or job.status != "running":
        raise StoreError("INVALID_STATE", "Only active execution can increase Docker work")


class DockerControl:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def cancel(self, run_id: UUID) -> JobRecord:
        return await Jobs(self.database).cancel(run_id, kind="docker")

    async def claim(self, owner: str, lease_seconds: int = 30) -> ControlLease | None:
        _duration(lease_seconds)
        if not 1 <= len(owner.encode()) <= 128:
            raise StoreError("VALIDATION_ERROR", "Invalid controller identity")

        async def operation(session: AsyncSession) -> ControlLease | None:
            slot = await session.scalar(
                select(DockerControlSlot)
                .where(DockerControlSlot.id == 1)
                .with_for_update(skip_locked=True)
            )
            if slot is None:
                return None
            now = await _clock(session)
            if slot.run_id is not None:
                if slot.lease_expires_at is not None and slot.lease_expires_at > now:
                    return None
                job = await session.scalar(
                    select(Job)
                    .where(Job.run_id == slot.run_id, Job.kind == "docker")
                    .with_for_update()
                )
                state = await session.get(DockerRunState, slot.run_id)
                if job is None or state is None:
                    raise StoreError("NOT_READY", "Docker slot references missing state")
                if job.status not in TERMINAL:
                    job.status = "cancelled" if job.status == "cancel_requested" else "failed"
                    job.error_code, job.completed_at = "INTERRUPTED", now
                    job.fence, job.owner, job.lease_expires_at = slot.fence + 1, None, None
                    state.desired_status, state.desired_error = job.status, "INTERRUPTED"
                    await _end_attempt(session, job, "controller_interrupted", now)
                if slot.phase != "cleanup":
                    slot.cleanup_attempts = 0
                slot.phase = "cleanup"
                if slot.cleanup_attempts >= 3:
                    # Retain reservations and the expired-cleanup diagnostic.
                    return None
                slot.cleanup_attempts += 1
            else:
                job = await session.scalar(
                    select(Job)
                    .where(Job.kind == "docker", Job.status == "queued", Job.attempts == 0)
                    .order_by(Job.created_at, Job.run_id)
                    .limit(1)
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
                session.add(Attempt(run_id=job.run_id, number=1, fence=slot.fence, owner=owner))
            await session.flush()
            expires_at = slot.lease_expires_at
            if (
                slot.run_id is None
                or expires_at is None
                or slot.phase not in {"running", "cleanup"}
            ):
                raise StoreError("NOT_READY", "Claim did not establish Docker ownership")
            return ControlLease(
                run_id=slot.run_id,
                fence=slot.fence,
                owner=owner,
                expires_at=expires_at,
                phase="running" if slot.phase == "running" else "cleanup",
            )

        return await self.database.transaction(operation)

    async def renew(self, lease: ControlLease, lease_seconds: int = 30) -> ControlLease:
        _duration(lease_seconds)

        async def operation(session: AsyncSession) -> ControlLease:
            slot, job, _state = await current(session, lease)
            expires = await _clock(session) + timedelta(seconds=lease_seconds)
            if job.status not in TERMINAL:
                job.lease_expires_at = expires
            await guard(session, lease)
            # The final lease-extension statement still tests the old slot expiry.
            updated = await session.scalar(
                update(DockerControlSlot)
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
                raise StoreError("FENCE_LOST", "Docker lease expired before renewal")
            return ControlLease(lease.run_id, slot.fence, lease.owner, expires, lease.phase)

        return await self.database.transaction(operation)

    async def begin_cleanup(
        self,
        lease: ControlLease,
        outcome: Literal["completed", "failed", "cancelled"],
        error_code: str | None = None,
    ) -> ControlLease:
        if (
            outcome not in {"completed", "failed", "cancelled"}
            or (outcome == "failed" and error_code not in FAILURES)
            or (outcome != "failed" and error_code is not None)
        ):
            raise StoreError("VALIDATION_ERROR", "Invalid Docker terminal outcome")

        async def operation(session: AsyncSession) -> ControlLease:
            slot, job, state = await current(session, lease)
            if lease.phase != "running" or job.status in TERMINAL:
                raise StoreError("INVALID_STATE", "Docker execution is not active")
            if outcome == "cancelled" and job.status != "cancel_requested":
                raise StoreError("INVALID_STATE", "Cancellation was not requested")
            state.desired_status = "cancelled" if job.status == "cancel_requested" else outcome
            state.desired_error = (
                "INTERRUPTED" if state.desired_status == "cancelled" else error_code
            )
            slot.phase, slot.cleanup_attempts = "cleanup", 1
            await guard(session, lease)
            return ControlLease(
                lease.run_id,
                lease.fence,
                lease.owner,
                slot.lease_expires_at or lease.expires_at,
                "cleanup",
            )

        return await self.database.transaction(operation)

    async def retry_cleanup(self, run_id: UUID) -> None:
        """Explicit operator retry; automatic cleanup never exceeds three ownership attempts."""

        async def operation(session: AsyncSession) -> None:
            slot = await session.scalar(
                select(DockerControlSlot)
                .where(
                    DockerControlSlot.id == 1,
                    DockerControlSlot.run_id == run_id,
                    DockerControlSlot.phase == "cleanup",
                    DockerControlSlot.lease_expires_at <= func.clock_timestamp(),
                )
                .with_for_update()
            )
            job = await session.get(Job, run_id)
            if slot is None or job is None or job.status not in TERMINAL:
                raise StoreError("INVALID_STATE", "Cleanup is not stalled after interruption")
            slot.cleanup_attempts, slot.fence, slot.revision = 0, slot.fence + 1, slot.revision + 1

        await self.database.transaction(operation)

    async def finish_cleanup(
        self, lease: ControlLease, manifest: DockerManifest | None = None
    ) -> JobRecord:
        if manifest is not None:
            manifest = DockerManifest.model_validate_json(
                canonical_bytes(manifest.model_dump(mode="json"))
            )

        async def operation(session: AsyncSession) -> JobRecord:
            _slot, job, state = await current(session, lease)
            if lease.phase != "cleanup":
                raise StoreError("INVALID_STATE", "Cleanup must precede Docker finalization")
            unresolved = await session.scalar(
                select(func.count())
                .select_from(DockerResource)
                .where(DockerResource.run_id == lease.run_id, DockerResource.state.not_in(RESOLVED))
            )
            if unresolved is None:
                raise StoreError("NOT_READY", "Resource accounting is unavailable")
            if unresolved > 0:
                raise StoreError(
                    "INVALID_STATE", "Unresolved Docker resources still reserve capacity"
                )
            status = (
                job.status
                if job.status in TERMINAL
                else "cancelled"
                if job.status == "cancel_requested"
                else state.desired_status
            )
            if status not in TERMINAL:
                raise StoreError("INVALID_STATE", "Docker outcome has not been selected")
            if status == "completed":
                run = await session.get(Run, lease.run_id)
                snapshot = DockerSnapshot.model_validate_json(canonical_bytes(state.snapshot))
                if (
                    manifest is None
                    or run is None
                    or manifest.run_id != lease.run_id
                    or manifest.publication_fence != lease.fence
                    or manifest.snapshot_digest != run.snapshot_digest
                    or snapshot.digest() != run.snapshot_digest
                    or manifest.total != snapshot.request_count
                ):
                    raise StoreError("VALIDATION_FAILED", "Docker publication identity is invalid")
                session.add(
                    DockerPublication(
                        run_id=lease.run_id,
                        manifest=manifest.model_dump(mode="json"),
                        digest=manifest.digest(),
                    )
                )
            if job.status not in TERMINAL:
                now = await _clock(session)
                job.status, job.completed_at = status, now
                job.error_code = "INTERRUPTED" if status == "cancelled" else state.desired_error
                job.fence, job.owner, job.lease_expires_at = job.fence + 1, None, None
                await _end_attempt(session, job, status, now)
            await guard(session, lease, release=True)
            return JobRecord.from_model(job)

        return await self.database.transaction(operation)
