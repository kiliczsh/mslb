"""Shared durable dataset/run operations for CLI, HTTP, and MCP adapters."""

import asyncio
import hashlib
from typing import BinaryIO
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.dataset_views import DatasetPreview
from mslb_contracts.inputs import ServerInput
from mslb_contracts.runs import RunSettings, settings_for_profile
from mslb_persistence.catalog import Catalog
from mslb_persistence.records import DatasetRecord, RunRecord
from mslb_persistence.resource_evidence import ResourceEvidenceStore

from mslb_application.admission import MaterializationAdmission, settle
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.snapshots import make_snapshot


class Submissions:
    """Serialize input materialization and share bounded identical idempotent submissions."""

    def __init__(self, catalog: Catalog, files: ManagedFiles) -> None:
        self.catalog, self.files = catalog, files
        self.admission = MaterializationAdmission()
        self.pending: asyncio.Task[RunRecord] | None = None
        self.pending_identity: tuple[str, str] | None = None
        self.pending_digest: str | None = None
        self.followers = 0

    async def upload(self, source: BinaryIO, filename: str) -> DatasetRecord:
        if self.pending is not None:
            raise OperationError(
                code="RATE_LIMITED", message="Input processing capacity is occupied"
            )
        return await self.admission.run(
            operation=lambda: self._upload(source=source, filename=filename)
        )

    async def _upload(self, source: BinaryIO, filename: str) -> DatasetRecord:
        await self._admit()
        data = await asyncio.to_thread(self.files.ingest, source=source, filename=filename)
        return await self.catalog.register_dataset(data=data)

    async def _admit(self) -> None:
        """Check predictable materialization before allocation; SQL rechecks before enqueue."""
        if self.catalog.database.enforce_resource_guard:
            evidence = await ResourceEvidenceStore(database=self.catalog.database).read()
            if not evidence.decision.admit:
                raise OperationError(
                    code="RESOURCE_PRESSURE", message="Fresh safe resource evidence is required"
                )

    async def preview_dataset(self, *, dataset_id: UUID) -> DatasetPreview:
        """Verify stored input under existing materialization admission and return twenty rows."""

        async def operation() -> DatasetPreview:
            await self._admit()
            dataset = await self.catalog.get_dataset(dataset_id=dataset_id)
            requests = await asyncio.to_thread(
                self.files.load_requests, digest=dataset.digest, size_bytes=dataset.size_bytes
            )
            return DatasetPreview(
                dataset_id=dataset.id, total_rows=len(requests), rows=requests[:20]
            )

        return await self.admission.run(operation=operation)

    async def submit(
        self, dataset_id: UUID, settings: RunSettings, key: str | None = None, scope: str = "local"
    ) -> RunRecord:
        """Share bounded identical in-flight keys while rejecting changed concurrent bodies."""
        digest = hashlib.sha256(
            data=canonical_bytes(
                value={
                    "dataset_id": str(object=dataset_id),
                    "settings": settings_for_profile(
                        settings=settings, profile=self.files.limits.capacity_profile
                    ).model_dump(mode="json"),
                }
            )
        ).hexdigest()
        existing = await self.catalog.find_idempotent(key=key, body_digest=digest, scope=scope)
        if existing is not None:
            return existing
        if key is None:
            if self.pending is not None:
                raise OperationError(
                    code="RATE_LIMITED", message="Input processing capacity is occupied"
                )
            return await self.admission.run(
                operation=lambda: self._submit(
                    dataset_id=dataset_id, settings=settings, key=key, scope=scope, digest=digest
                )
            )
        identity = (scope, hashlib.sha256(data=key.encode()).hexdigest())
        if self.pending is not None and self.pending_identity == identity:
            if digest != self.pending_digest:
                raise OperationError(
                    code="CONFLICT", message="Idempotency key has a different request body"
                )
            if self.followers >= 31:
                raise OperationError(
                    code="RATE_LIMITED", message="Idempotent retry capacity is occupied"
                )
            self.followers += 1
            try:
                return await asyncio.shield(arg=self.pending)
            finally:
                self.followers -= 1
        if self.pending is not None or self.admission.lock.locked():
            raise OperationError(
                code="RATE_LIMITED", message="Input processing capacity is occupied"
            )
        task = asyncio.create_task(
            coro=self.admission.run(
                operation=lambda: self._submit(
                    dataset_id=dataset_id, settings=settings, key=key, scope=scope, digest=digest
                )
            )
        )
        self.pending, self.pending_identity, self.pending_digest = task, identity, digest
        try:
            return await settle(task=task)
        finally:
            self.pending = None
            self.pending_identity = None
            self.pending_digest = None

    async def _submit(
        self, dataset_id: UUID, settings: RunSettings, key: str | None, scope: str, digest: str
    ) -> RunRecord:
        """Load validated input, freeze current server versions and enqueue immutable work."""
        await self._admit()
        dataset = await self.catalog.get_dataset(dataset_id=dataset_id)
        requests = await asyncio.to_thread(
            self.files.load_requests, digest=dataset.digest, size_bytes=dataset.size_bytes
        )
        current = await self.catalog.list_servers()
        servers = tuple(
            ServerInput(
                id=s.id,
                cpu_units_per_tick=s.cpu_units_per_tick,
                mem_mb=s.mem_mb,
                rate_limit_per_sec=s.rate_limit_per_sec,
            )
            for s in current
        )
        snapshot = await asyncio.to_thread(
            make_snapshot,
            servers=servers,
            requests=requests,
            settings=settings_for_profile(
                settings=settings, profile=self.files.limits.capacity_profile
            ),
        )
        await asyncio.to_thread(self.files.save_snapshot, snapshot=snapshot)
        return await self.catalog.enqueue(
            dataset_id=dataset_id,
            snapshot=snapshot,
            versions=tuple((s.id, s.version) for s in current),
            body_digest=digest,
            key=key,
            scope=scope,
        )
