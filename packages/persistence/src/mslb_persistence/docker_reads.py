"""Coherent bounded operational history and publication reads with strict mode isolation."""

from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import DockerManifest, DockerProgress
from mslb_contracts.docker_views import DockerResourceView, DockerRunView
from sqlalchemy import select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.docker_control import RESOLVED
from mslb_persistence.docker_models import (
    DockerControlSlot,
    DockerPublication,
    DockerResource,
    DockerRunState,
)
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import Job, Run
from mslb_persistence.reads import check_limit
from mslb_persistence.transaction_clock import database_now


def view(*, run: Run, job: Job) -> DockerRunView:
    """Validate stored JSON at the public boundary; internal paths never enter the DTO."""
    return DockerRunView.model_validate_json(
        json_data=canonical_bytes(
            value={
                "id": str(object=run.id),
                "dataset_id": str(object=run.dataset_id),
                "snapshot_digest": run.snapshot_digest,
                "dataset_digest": run.dataset_digest,
                "configuration": run.configuration,
                "status": job.status,
                "attempts": job.attempts,
                "created_at": run.created_at.isoformat(),
                "completed_at": None if job.completed_at is None else job.completed_at.isoformat(),
                "error_code": job.error_code,
            }
        )
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class DockerRead:
    """Carry verified internal publication metadata separately from its public run view."""

    run: DockerRunView
    resources: tuple[DockerResourceView, ...]
    cleanup_pending: bool
    manifest: DockerManifest | None
    progress: DockerProgress | None = None


class DockerReads:
    """Read Docker-only rows in bounded repeatable-read transactions."""

    def __init__(self, *, database: Database) -> None:
        self.database = database

    async def runs(
        self, *, after: tuple[datetime, UUID] | None = None, limit: int = 100
    ) -> tuple[DockerRunView, ...]:
        check_limit(limit=limit)

        async def operation(session: AsyncSession) -> tuple[DockerRunView, ...]:
            query = (
                select(Run, Job)
                .join(target=Job, onclause=Job.run_id == Run.id)
                .where(Job.kind == "docker")
            )
            if after is not None:
                query = query.where(tuple_(Run.created_at, Run.id) > after)
            rows = await session.execute(
                statement=query.order_by(Run.created_at, Run.id).limit(limit=limit)
            )
            return tuple(view(run=row[0], job=row[1]) for row in rows)

        return await self.database.transaction(operation=operation)

    async def run(self, *, run_id: UUID) -> DockerRead:
        async def operation(session: AsyncSession) -> DockerRead:
            row = (
                await session.execute(
                    statement=select(Run, Job)
                    .join(target=Job, onclause=Job.run_id == Run.id)
                    .where(Run.id == run_id, Job.kind == "docker")
                )
            ).one_or_none()
            if row is None:
                raise StoreError(code="NOT_FOUND", message="Docker run does not exist")
            run, job = row
            resources = tuple(
                await session.scalars(
                    statement=select(DockerResource)
                    .where(DockerResource.run_id == run_id)
                    .order_by(DockerResource.ordinal)
                    .limit(limit=65)
                )
            )
            if len(resources) > 64:
                raise StoreError(
                    code="VALIDATION_FAILED", message="Resource history exceeds bounds"
                )
            public = tuple(
                DockerResourceView.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "id": str(object=resource.id),
                            "name": resource.name,
                            "kind": resource.kind,
                            "state": resource.state,
                        }
                    )
                )
                for resource in resources
            )
            pending = any(resource.state not in RESOLVED for resource in resources)
            slot = await session.get(entity=DockerControlSlot, ident=1)
            pending = pending or (
                slot is not None and slot.run_id == run_id and slot.phase == "cleanup"
            )
            publication = await session.get(entity=DockerPublication, ident=run_id)
            manifest = None
            if publication is not None:
                manifest = DockerManifest.model_validate_json(
                    json_data=canonical_bytes(value=publication.manifest)
                )
                if (
                    job.status != "completed"
                    or pending
                    or manifest.run_id != run_id
                    or manifest.snapshot_digest != run.snapshot_digest
                    or manifest.digest() != publication.digest
                    or manifest.publication_fence != job.fence - 1
                ):
                    raise StoreError(
                        code="VALIDATION_FAILED", message="Docker publication identity differs"
                    )
            elif job.status == "completed":
                raise StoreError(
                    code="VALIDATION_FAILED", message="Completed Docker publication is missing"
                )
            progress = None
            state = await session.get(entity=DockerRunState, ident=run_id)
            now = await database_now(session=session)
            if (
                state is not None
                and state.latest_progress is not None
                and job.status == "running"
                and slot is not None
                and slot.run_id == run_id
                and slot.phase == "running"
                and slot.fence == job.fence
                and slot.lease_expires_at is not None
                and slot.lease_expires_at > now
            ):
                candidate = DockerProgress.model_validate_json(
                    json_data=canonical_bytes(value=state.latest_progress)
                )
                if (
                    candidate.run_id != run_id
                    or candidate.fence != job.fence
                    or candidate.snapshot_digest != run.snapshot_digest
                ):
                    raise StoreError(
                        code="VALIDATION_FAILED", message="Stored live frame scope differs"
                    )
                if 0 <= (now - candidate.observed_at).total_seconds() <= 3 and all(
                    0 <= (now - sample.observed_at).total_seconds() <= 3
                    for sample in candidate.backends
                ):
                    identities = {item.id: item for item in resources if item.state not in RESOLVED}
                    if any(
                        name not in {item.name for item in identities.values()}
                        for name in candidate.unavailable_backends
                    ) or any(
                        sample.resource_id not in identities
                        or sample.container_id != identities[sample.resource_id].container_id
                        or sample.health.backend_id != identities[sample.resource_id].name
                        for sample in candidate.backends
                    ):
                        raise StoreError(
                            code="VALIDATION_FAILED",
                            message="Stored live backend is no longer owned",
                        )
                    progress = candidate
            return DockerRead(
                run=view(run=run, job=job),
                resources=public,
                cleanup_pending=pending,
                manifest=manifest,
                progress=progress,
            )

        return await self.database.transaction(operation=operation)
