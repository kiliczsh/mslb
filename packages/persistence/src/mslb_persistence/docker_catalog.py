"""Immutable Docker enrollment and atomic shared run/job/outbox registration."""

import hashlib
from datetime import timedelta
from uuid import UUID, uuid4

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import DeploymentSpec, DockerSnapshot
from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.catalog import _installation
from mslb_persistence.catalog_models import Dataset
from mslb_persistence.database import Database
from mslb_persistence.docker_models import DockerDeployment, DockerRunState
from mslb_persistence.docker_records import DeploymentRecord
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import IdempotencyRecord, Job, Outbox, Run
from mslb_persistence.jobs import _clock
from mslb_persistence.records import RunRecord


async def _prune_deployments(
    *, session: AsyncSession, keep_recent: int = 2, limit: int = 16
) -> int:
    """Prune only bounded expired enrollments that no run/resource still references."""
    retained = (
        select(DockerDeployment.id)
        .order_by(DockerDeployment.created_at.desc(), DockerDeployment.id.desc())
        .limit(limit=keep_recent)
        .correlate(None)
    )
    referenced = (
        select(DockerRunState.run_id)
        .where(DockerRunState.deployment_id == DockerDeployment.id)
        .exists()
    )
    candidates = (
        select(DockerDeployment.id)
        .where(DockerDeployment.id.not_in(other=retained), ~referenced)
        .order_by(DockerDeployment.created_at, DockerDeployment.id)
        .limit(limit=limit)
    )
    removed = tuple(
        await session.scalars(
            statement=delete(table=DockerDeployment)
            .where(DockerDeployment.id.in_(other=candidates), ~referenced)
            .returning(DockerDeployment.id)
        )
    )
    return len(removed)


class DockerCatalog:
    """Freeze trusted deployment inputs and enqueue distinct operational Docker jobs."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def enroll(self, definition: DeploymentSpec) -> DeploymentRecord:
        """Enroll only trusted inspect-adapter output; there is no public enrollment API."""

        async def operation(session: AsyncSession) -> DeploymentRecord:
            await _installation(session=session)
            existing = await session.scalar(
                statement=select(DockerDeployment).where(
                    DockerDeployment.fingerprint == definition.digest()
                )
            )
            if existing is not None:
                if existing.definition != definition.model_dump(mode="json"):
                    raise StoreError(code="CONFLICT", message="Deployment fingerprint conflicts")
                return DeploymentRecord(id=existing.id, definition=definition)
            count = await session.scalar(
                statement=select(func.count()).select_from(DockerDeployment)
            )
            if count is not None and count >= 16:
                await _prune_deployments(session=session)
                count = await session.scalar(
                    statement=select(func.count()).select_from(DockerDeployment)
                )
            if count is not None and count >= 16:
                raise StoreError(
                    code="RATE_LIMITED", message="Deployment history requires explicit retention"
                )
            row = DockerDeployment(
                id=uuid4(),
                fingerprint=definition.digest(),
                definition=definition.model_dump(mode="json"),
            )
            session.add(instance=row)
            await session.flush()
            return DeploymentRecord(id=row.id, definition=definition)

        return await self.database.transaction(operation=operation)

    async def prune_deployments(self, *, keep_recent: int = 2, limit: int = 16) -> int:
        """Remove only unused old enrollment rows, under the registration serialization lock."""
        if (
            type(keep_recent) is not int
            or not 1 <= keep_recent <= 16
            or type(limit) is not int
            or not 1 <= limit <= 16
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid deployment retention bounds")

        async def operation(session: AsyncSession) -> int:
            await _installation(session=session)
            return await _prune_deployments(session=session, keep_recent=keep_recent, limit=limit)

        return await self.database.transaction(operation=operation)

    async def deployment(self, deployment_id: UUID) -> DeploymentRecord:
        async def operation(session: AsyncSession) -> DeploymentRecord:
            row = await session.get(entity=DockerDeployment, ident=deployment_id)
            if row is None:
                raise StoreError(code="NOT_FOUND", message="Deployment does not exist")
            definition = DeploymentSpec.model_validate_json(
                json_data=canonical_bytes(value=row.definition)
            )
            if definition.digest() != row.fingerprint:
                raise StoreError(
                    code="VALIDATION_FAILED", message="Deployment identity is inconsistent"
                )
            return DeploymentRecord(id=row.id, definition=definition)

        return await self.database.transaction(operation=operation)

    async def snapshot(self, run_id: UUID) -> DockerSnapshot:
        """Read one coherent trusted deployment and dataset view for operational submission."""

        async def operation(session: AsyncSession) -> DockerSnapshot:
            state = await session.get(entity=DockerRunState, ident=run_id)
            run = await session.get(entity=Run, ident=run_id)
            if state is None or run is None:
                raise StoreError(code="NOT_FOUND", message="Docker run does not exist")
            result = DockerSnapshot.model_validate_json(
                json_data=canonical_bytes(value=state.snapshot)
            )
            if (
                result.digest() != run.snapshot_digest
                or result.deployment_id != state.deployment_id
            ):
                raise StoreError(
                    code="VALIDATION_FAILED", message="Docker snapshot identity is inconsistent"
                )
            return result

        return await self.database.transaction(operation=operation)

    async def enqueue(
        self, snapshot: DockerSnapshot, key: str | None = None, scope: str = "local"
    ) -> RunRecord:
        """Validate frozen operational inputs and enqueue one durable Docker job atomically."""
        if not 1 <= len(scope.encode()) <= 128 or (
            key is not None and not 1 <= len(key.encode()) <= 128
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid idempotency identity")
        body_digest = hashlib.sha256(
            data=canonical_bytes(
                value={
                    "dataset_id": str(object=snapshot.dataset_id),
                    "settings": snapshot.configuration.model_dump(mode="json"),
                }
            )
        ).hexdigest()
        key_hash = hashlib.sha256(data=key.encode()).hexdigest() if key is not None else None

        async def operation(session: AsyncSession) -> RunRecord:
            await _installation(session=session)
            now = await _clock(session=session)
            expired = (
                select(IdempotencyRecord.scope, IdempotencyRecord.key_hash)
                .where(IdempotencyRecord.expires_at <= func.clock_timestamp())
                .limit(limit=500)
            )
            await session.execute(
                statement=delete(table=IdempotencyRecord).where(
                    tuple_(IdempotencyRecord.scope, IdempotencyRecord.key_hash).in_(other=expired)
                )
            )
            if key_hash is not None:
                previous = await session.get(entity=IdempotencyRecord, ident=(scope, key_hash))
                if previous is not None and previous.expires_at <= now:
                    await session.delete(instance=previous)
                    await session.flush()
                    previous = None
                if previous is not None:
                    if previous.body_digest != body_digest:
                        raise StoreError(
                            code="CONFLICT",
                            message="Idempotency key belongs to a different request",
                        )
                    existing = await session.get(entity=Run, ident=previous.run_id)
                    job = await session.get(entity=Job, ident=previous.run_id)
                    if existing is None or job is None or job.kind != "docker":
                        raise StoreError(
                            code="CONFLICT",
                            message="Idempotency key belongs to a different run mode",
                        )
                    return RunRecord.from_model(row=existing)
            deployment = await session.get(entity=DockerDeployment, ident=snapshot.deployment_id)
            dataset = await session.get(entity=Dataset, ident=snapshot.dataset_id)
            if dataset is None or deployment is None:
                raise StoreError(code="NOT_FOUND", message="Dataset or deployment does not exist")
            if (
                dataset.digest != snapshot.dataset_digest
                or dataset.request_count != snapshot.request_count
                or dataset.latest_arrival != snapshot.latest_arrival
                or deployment.fingerprint != snapshot.deployment_digest
            ):
                raise StoreError(
                    code="CONFLICT", message="Snapshot differs from immutable registered inputs"
                )
            queued = await session.scalar(
                statement=select(func.count()).select_from(Job).where(Job.status == "queued")
            )
            if queued is not None and queued >= 20:
                raise StoreError(code="RATE_LIMITED", message="Durable queue limit reached")
            run = Run(
                id=uuid4(),
                dataset_id=dataset.id,
                snapshot_digest=snapshot.digest(),
                snapshot_key=f"docker/snapshots/{snapshot.digest()}.json",
                dataset_digest=dataset.digest,
                server_snapshot_digest=deployment.fingerprint,
                engine_version="docker-controller/0.1.0",
                configuration=snapshot.configuration.model_dump(mode="json"),
                server_versions={},
            )
            session.add(instance=run)
            await session.flush()
            session.add(instance=Job(run_id=run.id, kind="docker"))
            session.add(
                instance=DockerRunState(
                    run_id=run.id,
                    deployment_id=deployment.id,
                    snapshot=snapshot.model_dump(mode="json"),
                )
            )
            await session.flush()
            session.add(instance=Outbox(id=uuid4(), run_id=run.id))
            if key_hash is not None:
                session.add(
                    instance=IdempotencyRecord(
                        scope=scope,
                        key_hash=key_hash,
                        body_digest=body_digest,
                        run_id=run.id,
                        expires_at=now + timedelta(hours=24),
                    )
                )
            await session.flush()
            return RunRecord.from_model(row=run)

        return await self.database.transaction(operation=operation)
