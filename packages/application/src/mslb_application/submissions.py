"""Shared durable dataset/run operations for CLI, HTTP, and MCP adapters."""

import asyncio
import hashlib
from typing import BinaryIO
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.inputs import ServerInput
from mslb_contracts.runs import RunSettings
from mslb_persistence.catalog import Catalog
from mslb_persistence.records import DatasetRecord, RunRecord

from mslb_application.admission import MaterializationAdmission, settle
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.snapshots import make_snapshot


class Submissions:
    def __init__(self, catalog: Catalog, files: ManagedFiles) -> None:
        self.catalog, self.files = catalog, files
        self.admission = MaterializationAdmission()
        self.pending: asyncio.Task[RunRecord] | None = None
        self.pending_identity: tuple[str, str] | None = None
        self.pending_digest: str | None = None
        self.followers = 0

    async def upload(self, source: BinaryIO, filename: str) -> DatasetRecord:
        if self.pending is not None:
            raise OperationError("RATE_LIMITED", "Input processing capacity is occupied")
        return await self.admission.run(lambda: self._upload(source, filename))

    async def _upload(self, source: BinaryIO, filename: str) -> DatasetRecord:
        data = await asyncio.to_thread(self.files.ingest, source, filename)
        return await self.catalog.register_dataset(data)

    async def submit(
        self, dataset_id: UUID, settings: RunSettings, key: str | None = None, scope: str = "local"
    ) -> RunRecord:
        digest = hashlib.sha256(
            canonical_bytes(
                {"dataset_id": str(dataset_id), "settings": settings.model_dump(mode="json")}
            )
        ).hexdigest()
        existing = await self.catalog.find_idempotent(key, digest, scope)
        if existing is not None:
            return existing
        if key is None:
            if self.pending is not None:
                raise OperationError("RATE_LIMITED", "Input processing capacity is occupied")
            return await self.admission.run(
                lambda: self._submit(dataset_id, settings, key, scope, digest)
            )
        identity = (scope, hashlib.sha256(key.encode()).hexdigest())
        if self.pending is not None and self.pending_identity == identity:
            if digest != self.pending_digest:
                raise OperationError("CONFLICT", "Idempotency key has a different request body")
            if self.followers >= 31:
                raise OperationError("RATE_LIMITED", "Idempotent retry capacity is occupied")
            self.followers += 1
            try:
                return await asyncio.shield(self.pending)
            finally:
                self.followers -= 1
        if self.pending is not None or self.admission.lock.locked():
            raise OperationError("RATE_LIMITED", "Input processing capacity is occupied")
        task = asyncio.create_task(
            self.admission.run(lambda: self._submit(dataset_id, settings, key, scope, digest))
        )
        self.pending, self.pending_identity, self.pending_digest = task, identity, digest
        try:
            return await settle(task)
        finally:
            self.pending = None
            self.pending_identity = None
            self.pending_digest = None

    async def _submit(
        self, dataset_id: UUID, settings: RunSettings, key: str | None, scope: str, digest: str
    ) -> RunRecord:
        dataset = await self.catalog.get_dataset(dataset_id)
        requests = await asyncio.to_thread(
            self.files.load_requests, dataset.digest, dataset.size_bytes
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
        snapshot = await asyncio.to_thread(make_snapshot, servers, requests, settings)
        await asyncio.to_thread(self.files.save_snapshot, snapshot)
        return await self.catalog.enqueue(
            dataset_id, snapshot, tuple((s.id, s.version) for s in current), digest, key, scope
        )
