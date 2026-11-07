"""Version-checked live servers and atomic immutable run/job/outbox registration."""

import hashlib
import re
from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4

from mslb_contracts.capacity_profiles import request_limit
from mslb_contracts.catalog import DatasetRegistration, ServerView
from mslb_contracts.inputs import ServerInput
from mslb_contracts.runs import RunSnapshot, stored_request_limit, verify_snapshot_identity
from sqlalchemy import delete, func, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.catalog_models import Dataset, Installation, ManagedServer, ServerHistory
from mslb_persistence.database import Database
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import IdempotencyRecord, Job, Outbox, Run
from mslb_persistence.records import DatasetRecord, RunRecord, server_input, server_view
from mslb_persistence.resource_admission import require_admission


async def _installation(session: AsyncSession) -> Installation:
    row = await session.scalar(
        statement=select(Installation).where(Installation.id == 1).with_for_update()
    )
    if row is None:
        raise StoreError(code="NOT_READY", message="Database migration is required")
    row.revision += 1
    return row


def _history(row: ManagedServer, deleted: bool = False) -> ServerHistory:
    return ServerHistory(
        server_id=row.id,
        version=row.version,
        deleted=deleted,
        cpu_units_per_tick=row.cpu_units_per_tick,
        mem_mb=row.mem_mb,
        rate_limit_per_sec=row.rate_limit_per_sec,
    )


def _check_version(expected: int, actual: int) -> None:
    if type(expected) is not int or expected < 1 or expected != actual:
        raise StoreError(code="CONFLICT", message="Server version does not match")


class Catalog:
    """Manage versioned servers, immutable datasets and idempotent logical submissions."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def seed_once(self, servers: tuple[ServerInput, ...]) -> None:
        """Initialize supplied servers once without reseeding an intentionally empty catalog."""
        if not 1 <= len(servers) <= 64 or len({s.id for s in servers}) != len(servers):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid seed server population")

        async def operation(session: AsyncSession) -> None:
            marker = await _installation(session=session)
            if marker.seeded:
                return
            if await session.scalar(statement=select(func.count()).select_from(ManagedServer)) != 0:
                raise StoreError(
                    code="CONFLICT", message="Uninitialized database contains server configuration"
                )
            for server in servers:
                row = ManagedServer(**server.model_dump(), version=1)
                session.add(instance=row)
                session.add(instance=_history(row=row))
            marker.seeded = True

        await self.database.transaction(operation=operation)

    async def list_servers(self) -> tuple[ServerView, ...]:
        async def operation(session: AsyncSession) -> tuple[ServerView, ...]:
            rows = await session.scalars(
                statement=select(ManagedServer).order_by(ManagedServer.id).limit(limit=64)
            )
            return tuple(server_view(server=row) for row in rows)

        return await self.database.transaction(operation=operation)

    async def create_server(self, server: ServerInput) -> ServerView:
        """Create a normalized server and immutable first history version atomically."""

        async def operation(session: AsyncSession) -> ServerView:
            await _installation(session=session)
            if await session.get(entity=ManagedServer, ident=server.id) is not None:
                raise StoreError(code="CONFLICT", message="Server already exists")
            count = await session.scalar(statement=select(func.count()).select_from(ManagedServer))
            if count is not None and count >= 64:
                raise StoreError(code="RATE_LIMITED", message="Logical server limit reached")
            previous = await session.scalar(
                statement=select(func.max(ServerHistory.version)).where(
                    ServerHistory.server_id == server.id
                )
            )
            row = ManagedServer(**server.model_dump(), version=(previous or 0) + 1)
            session.add(instance=row)
            session.add(instance=_history(row=row))
            return server_view(server=row)

        return await self.database.transaction(operation=operation)

    async def update_server(self, server: ServerInput, expected_version: int) -> ServerView:
        """Require the expected version before recording changed future capacities."""

        async def operation(session: AsyncSession) -> ServerView:
            await _installation(session=session)
            row = await session.get(entity=ManagedServer, ident=server.id)
            if row is None:
                raise StoreError(code="NOT_FOUND", message="Server does not exist")
            _check_version(expected=expected_version, actual=row.version)
            row.version += 1
            row.cpu_units_per_tick, row.mem_mb, row.rate_limit_per_sec = (
                server.cpu_units_per_tick,
                server.mem_mb,
                server.rate_limit_per_sec,
            )
            session.add(instance=_history(row=row))
            return server_view(server=row)

        return await self.database.transaction(operation=operation)

    async def delete_server(self, server_id: str, expected_version: int) -> None:
        """Record version-checked deletion without rewriting historical snapshots."""

        async def operation(session: AsyncSession) -> None:
            await _installation(session=session)
            row = await session.get(entity=ManagedServer, ident=server_id)
            if row is None:
                raise StoreError(code="NOT_FOUND", message="Server does not exist")
            _check_version(expected=expected_version, actual=row.version)
            row.version += 1
            session.add(instance=_history(row=row, deleted=True))
            await session.delete(instance=row)

        await self.database.transaction(operation=operation)

    async def register_dataset(self, data: DatasetRegistration) -> DatasetRecord:
        """Reuse normalized identity while preserving actual upload provenance."""
        if request_limit(profile=data.capacity_profile) > request_limit(
            profile=self.database.capacity_profile
        ):
            raise StoreError(
                code="RESOURCE_LIMIT_EXCEEDED", message="Dataset exceeds installation capacity"
            )
        if data.latest_arrival < data.earliest_arrival or (
            (data.original_digest is None) != (data.source_size_bytes is None)
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid dataset arrival range")

        async def operation(session: AsyncSession) -> DatasetRecord:
            await _installation(session=session)
            row = await session.scalar(
                statement=select(Dataset).where(Dataset.digest == data.digest)
            )
            if row is not None:
                if (
                    row.request_count,
                    row.earliest_arrival,
                    row.latest_arrival,
                ) != (
                    data.request_count,
                    data.earliest_arrival,
                    data.latest_arrival,
                ):
                    raise StoreError(
                        code="CONFLICT", message="Dataset identity has different metadata"
                    )
                if row.size_bytes != data.size_bytes:
                    if row.original_digest is None and data.original_digest is not None:
                        row.size_bytes = data.size_bytes
                    else:
                        raise StoreError(
                            code="CONFLICT", message="Dataset identity has different byte metadata"
                        )
                if row.original_digest is None and data.original_digest is not None:
                    row.original_digest = data.original_digest
                    row.source_size_bytes = data.source_size_bytes
                    await session.flush()
                return DatasetRecord.from_model(row=row)
            await require_admission(database=self.database, session=session)
            row = Dataset(
                id=uuid4(), storage_key=f"datasets/{data.digest}.csv", **data.model_dump()
            )
            session.add(instance=row)
            await session.flush()
            await require_admission(database=self.database, session=session)
            return DatasetRecord.from_model(row=row)

        return await self.database.transaction(operation=operation)

    async def get_dataset(self, dataset_id: UUID) -> DatasetRecord:
        async def operation(session: AsyncSession) -> DatasetRecord:
            row = await session.get(entity=Dataset, ident=dataset_id)
            if row is None:
                raise StoreError(code="NOT_FOUND", message="Dataset does not exist")
            return DatasetRecord.from_model(row=row)

        return await self.database.transaction(operation=operation)

    async def get_run(self, run_id: UUID) -> RunRecord:
        async def operation(session: AsyncSession) -> RunRecord:
            row = await session.get(entity=Run, ident=run_id)
            if row is None:
                raise StoreError(code="NOT_FOUND", message="Run does not exist")
            return RunRecord.from_model(row=row)

        return await self.database.transaction(operation=operation)

    async def find_idempotent(
        self,
        key: str | None,
        body_digest: str,
        scope: str = "local",
        *,
        kind: Literal["logical", "docker"] = "logical",
    ) -> RunRecord | None:
        """Return an unexpired matching submission or reject changed key/body identity."""
        if key is None:
            return None
        if (
            not 1 <= len(key.encode()) <= 128
            or not 1 <= len(scope.encode()) <= 128
            or not re.fullmatch(pattern=r"[0-9a-f]{64}", string=body_digest)
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid idempotency identity")
        key_hash = hashlib.sha256(string=key.encode()).hexdigest()

        async def operation(session: AsyncSession) -> RunRecord | None:
            previous = await session.scalar(
                statement=select(IdempotencyRecord).where(
                    IdempotencyRecord.scope == scope,
                    IdempotencyRecord.key_hash == key_hash,
                    IdempotencyRecord.expires_at > func.clock_timestamp(),
                )
            )
            if previous is None:
                return None
            if previous.body_digest != body_digest:
                raise StoreError(
                    code="CONFLICT", message="Idempotency key belongs to a different request"
                )
            row = await session.get(entity=Run, ident=previous.run_id)
            if row is None:
                raise StoreError(
                    code="NOT_READY", message="Idempotency record references missing run"
                )
            job = await session.get(entity=Job, ident=previous.run_id)
            if job is None or job.kind != kind:
                raise StoreError(
                    code="CONFLICT", message="Idempotency key belongs to a different run mode"
                )
            return RunRecord.from_model(row=row)

        return await self.database.transaction(operation=operation)

    async def enqueue(
        self,
        dataset_id: UUID,
        snapshot: RunSnapshot,
        versions: tuple[tuple[str, int], ...],
        body_digest: str,
        key: str | None = None,
        scope: str = "local",
    ) -> RunRecord:
        """Freeze a coherent catalog/dataset view and create run, job, key and outbox atomically."""
        verify_snapshot_identity(snapshot=snapshot)
        if stored_request_limit(settings=snapshot.configuration) > request_limit(
            profile=self.database.capacity_profile
        ):
            raise StoreError(
                code="RESOURCE_LIMIT_EXCEEDED", message="Run exceeds installation capacity"
            )
        if (
            not re.fullmatch(pattern=r"[0-9a-f]{64}", string=body_digest)
            or not 1 <= len(scope.encode()) <= 128
            or (key is not None and not 1 <= len(key.encode()) <= 128)
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid idempotency identity")
        key_hash = hashlib.sha256(data=key.encode()).hexdigest() if key is not None else None

        async def operation(session: AsyncSession) -> RunRecord:
            await _installation(session=session)
            now = await session.scalar(statement=select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise StoreError(code="NOT_READY", message="Database clock unavailable")
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
                if previous is not None and previous.expires_at > now:
                    if previous.body_digest != body_digest:
                        raise StoreError(
                            code="CONFLICT",
                            message="Idempotency key belongs to a different request",
                        )
                    existing = await session.get(entity=Run, ident=previous.run_id)
                    if existing is None:
                        raise StoreError(
                            code="NOT_READY", message="Idempotency record references missing run"
                        )
                    job = await session.get(entity=Job, ident=previous.run_id)
                    if job is None or job.kind != "logical":
                        raise StoreError(
                            code="CONFLICT",
                            message="Idempotency key belongs to a different run mode",
                        )
                    return RunRecord.from_model(row=existing)
                if previous is not None:
                    await session.delete(instance=previous)
                    await session.flush()
            dataset = await session.get(entity=Dataset, ident=dataset_id)
            if dataset is None:
                raise StoreError(code="NOT_FOUND", message="Dataset does not exist")
            current = tuple(
                await session.scalars(
                    statement=select(ManagedServer).order_by(ManagedServer.id).limit(limit=64)
                )
            )
            if (
                tuple((s.id, s.version) for s in current) != versions
                or tuple(server_input(server=s) for s in current) != snapshot.servers
            ):
                raise StoreError(
                    code="CONFLICT", message="Server configuration changed before submission"
                )
            if dataset.digest != snapshot.dataset_digest or dataset.request_count != len(
                snapshot.requests
            ):
                raise StoreError(code="CONFLICT", message="Dataset differs from immutable snapshot")
            queued = await session.scalar(
                statement=select(func.count()).select_from(Job).where(Job.status == "queued")
            )
            if queued is not None and queued >= 20:
                raise StoreError(code="RATE_LIMITED", message="Durable queue limit reached")
            await require_admission(database=self.database, session=session)
            run = Run(
                id=uuid4(),
                dataset_id=dataset.id,
                snapshot_digest=snapshot.snapshot_digest,
                snapshot_key=f"snapshots/{snapshot.snapshot_digest}.json",
                dataset_digest=snapshot.dataset_digest,
                server_snapshot_digest=snapshot.server_snapshot_digest,
                engine_version=snapshot.engine_version,
                configuration=snapshot.configuration.model_dump(mode="json"),
                server_versions=dict(versions),
            )
            session.add(instance=run)
            await session.flush()
            session.add(instance=Job(run_id=run.id))
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
            await require_admission(database=self.database, session=session)
            return RunRecord.from_model(row=run)

        return await self.database.transaction(operation=operation)
