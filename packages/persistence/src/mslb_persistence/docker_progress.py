"""Fenced latest-frame writes; observation data never authorizes execution or publication."""

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import DockerProgress
from mslb_contracts.docker_runs import DockerSnapshot
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.docker_control import RESOLVED, current, execution, guard
from mslb_persistence.docker_models import DockerResource
from mslb_persistence.docker_records import ControlLease
from mslb_persistence.errors import StoreError
from mslb_persistence.transaction_clock import database_now


class DockerProgressStore:
    """Replace at most one16KiB frame under the current live controller fence."""

    def __init__(self, *, database: Database) -> None:
        self.database = database

    async def write(self, *, lease: ControlLease, frame: DockerProgress) -> None:
        """Check immutable scope, actual owned identities and authoritative freshness."""
        raw = canonical_bytes(value=frame.model_dump(mode="json"))
        if len(raw) > 16 * 1024:
            raise StoreError(code="RESOURCE_LIMIT_EXCEEDED", message="Live frame exceeds bounds")
        frame = DockerProgress.model_validate_json(json_data=raw)

        async def operation(session: AsyncSession) -> None:
            _slot, job, state = await current(session=session, lease=lease)
            execution(lease=lease, job=job)
            snapshot = DockerSnapshot.model_validate_json(
                json_data=canonical_bytes(value=state.snapshot)
            )
            if (
                frame.run_id != lease.run_id
                or frame.fence != lease.fence
                or frame.snapshot_digest != snapshot.digest()
                or frame.pending > snapshot.configuration.max_pending
                or frame.inflight > snapshot.configuration.global_inflight
            ):
                raise StoreError(code="VALIDATION_FAILED", message="Live frame scope differs")
            now = await database_now(session=session)
            if not 0 <= (now - frame.observed_at).total_seconds() <= 3:
                raise StoreError(code="VALIDATION_FAILED", message="Live frame is not fresh")
            owned = tuple(
                await session.scalars(
                    statement=select(DockerResource)
                    .where(
                        DockerResource.run_id == lease.run_id,
                        DockerResource.state.not_in(other=RESOLVED),
                    )
                    .limit(limit=5)
                )
            )
            if len(owned) > 4:
                raise StoreError(code="VALIDATION_FAILED", message="Live membership exceeds bounds")
            identities = {row.id: row for row in owned}
            names = {row.name for row in owned}
            if any(name not in names for name in frame.unavailable_backends) or any(
                sample.resource_id not in identities
                or sample.container_id != identities[sample.resource_id].container_id
                or sample.health.backend_id != identities[sample.resource_id].name
                for sample in frame.backends
            ):
                raise StoreError(code="VALIDATION_FAILED", message="Live backend was not owned")
            state.latest_progress = frame.model_dump(mode="json")
            await session.flush()
            await guard(session=session, lease=lease)
            if (
                not 0
                <= ((await database_now(session=session)) - frame.observed_at).total_seconds()
                <= 3
            ):
                raise StoreError(
                    code="VALIDATION_FAILED", message="Live frame expired before commit"
                )

        await self.database.transaction(operation=operation)
