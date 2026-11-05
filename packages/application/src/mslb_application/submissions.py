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

from mslb_application.files import ManagedFiles
from mslb_application.snapshots import make_snapshot


class Submissions:
    def __init__(self, catalog: Catalog, files: ManagedFiles) -> None:
        self.catalog, self.files = catalog, files

    async def upload(self, source: BinaryIO, filename: str) -> DatasetRecord:
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
