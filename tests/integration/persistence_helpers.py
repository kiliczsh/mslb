"""Shared durable-run and artifact fixtures for PostgreSQL integration tests."""

import asyncio
import hashlib
import io
import json
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO

from mslb_application.artifacts import execute_bundle, timeline_record
from mslb_application.checkpoints import PREFIX_NAMES, capture_terminal_seal, write_checkpoint
from mslb_application.files import FileReference, ManagedFiles, StorageLimits
from mslb_application.snapshots import canonical_bytes, domain_config, domain_inputs, make_snapshot
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.checkpoints import ArtifactPrefix
from mslb_contracts.durability import OutputChunk, PersistedEvent, ProgressCommit, StoredFile
from mslb_contracts.runs import BundleManifest, RunSettings, RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.simulator import Simulation
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database
from mslb_persistence.jobs import Jobs
from mslb_persistence.models import (
    Job,
)
from mslb_persistence.records import DatasetRecord, Lease, RunRecord
from sqlalchemy import func, update

from tests.integration.helpers import fixture_inputs


@dataclass(frozen=True, slots=True)
class Prepared:
    catalog: Catalog
    jobs: Jobs
    snapshot: RunSnapshot
    dataset: DatasetRecord
    versions: tuple[tuple[str, int], ...]

    async def enqueue(self, key: str | None = None) -> RunRecord:
        return await self.catalog.enqueue(
            dataset_id=self.dataset.id,
            snapshot=self.snapshot,
            versions=self.versions,
            body_digest=self.snapshot.snapshot_digest,
            key=key,
        )


async def prepare(database: Database) -> Prepared:
    servers, requests, size = await asyncio.to_thread(fixture_inputs)
    catalog, jobs = Catalog(database=database), Jobs(database=database)
    await catalog.seed_once(servers=servers)
    snapshot = make_snapshot(servers=servers, requests=requests, settings=RunSettings())
    dataset = await catalog.register_dataset(
        data=DatasetRegistration(
            digest=snapshot.dataset_digest,
            source_filename="requests.csv",
            request_count=len(requests),
            earliest_arrival=requests[0].t,
            latest_arrival=requests[-1].t,
            size_bytes=size,
        )
    )
    versions = tuple((server.id, server.version) for server in await catalog.list_servers())
    return Prepared(
        catalog=catalog, jobs=jobs, snapshot=snapshot, dataset=dataset, versions=versions
    )


async def expire(database: Database, lease: Lease) -> None:
    # Only this disposable test schema's lease is moved into the past; application
    # ownership checks still use PostgreSQL server time and real SQL predicates.
    async with database.engine.begin() as connection:
        await connection.execute(
            statement=update(table=Job)
            .where(Job.run_id == lease.run_id)
            .values(lease_expires_at=func.clock_timestamp() - timedelta(seconds=1))
        )


def stored(reference: FileReference) -> StoredFile:
    return StoredFile(key=reference.key, sha256=reference.sha256, size_bytes=reference.size_bytes)


class OutputFixture:
    """Small real engine/files producer for SQL protocol tests, not a worker."""

    def __init__(self, prepared: Prepared, lease: Lease, root: Path) -> None:
        self.snapshot, self.lease, self.root = prepared.snapshot, lease, root
        self.files = ManagedFiles(root=root / "files", limits=StorageLimits(min_free_bytes=0))
        servers, requests = domain_inputs(snapshot=prepared.snapshot)
        self.simulation = Simulation(
            servers=servers,
            requests=requests,
            config=domain_config(settings=prepared.snapshot.configuration),
        )
        self.metrics = MetricAccumulator(requests=requests, servers=servers)
        self.iterator = self.simulation.iter_ticks()
        self.streams: dict[str, BinaryIO] = {name: io.BytesIO() for name in PREFIX_NAMES}
        self.before = {
            name: ArtifactPrefix(
                name=name, size_bytes=0, records=0, sha256=hashlib.sha256(data=b"").hexdigest()
            )
            for name in PREFIX_NAMES
        }
        self.pending: list[PersistedEvent] = []
        self.prefix = f"runs/{lease.run_id}/attempt-{lease.fence}"

    def step(self) -> bool:
        frame = next(self.iterator)
        self.metrics.observe(frame=frame)
        self.streams["timeline.jsonl"].write(canonical_bytes(value=timeline_record(frame=frame)))
        for event in frame.events:
            self.streams["run.jsonl"].write(event.canonical_line().encode())
            sequence = self.before["run.jsonl"].records + len(self.pending) + 1
            self.pending.append(
                PersistedEvent.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "sequence": sequence,
                            "t": event.t,
                            "event": event.event,
                            "request_id": event.request_id,
                            "server_id": event.server_id,
                            "reason": event.reason,
                        }
                    )
                )
            )
            if event.reason is not None:
                self.streams["drop_reasons.jsonl"].write(
                    canonical_bytes(
                        value={"t": event.t, "request_id": event.request_id, "reason": event.reason}
                    )
                )
        if frame.scale_decision is not None:
            self.streams["autoscaling_decisions.jsonl"].write(
                frame.scale_decision.canonical_line().encode()
            )
        return frame.terminal

    def describe(self) -> ProgressCommit:
        report = None
        if self.simulation.terminal:
            report = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/metrics.json",
                    chunks=(canonical_bytes(value=self.metrics.summary()),),
                    maximum=256 * 1024**2,
                )
            )
            seal = capture_terminal_seal(
                snapshot=self.snapshot,
                simulation=self.simulation,
                metrics=self.metrics,
                streams=self.streams,
                report=report,
            )
            reference = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/terminal.json",
                    chunks=(canonical_bytes(value=seal.model_dump(mode="json")),),
                    maximum=1024**2,
                )
            )
            prefixes = seal.prefixes
        else:
            checkpoint = write_checkpoint(
                directory=self.root / "scratch",
                snapshot=self.snapshot,
                simulation=self.simulation,
                metrics=self.metrics,
                prefixes=self.streams,
            )
            with checkpoint.path.open(mode="rb") as stream:
                reference = stored(
                    reference=self.files.publish(
                        key=f"{self.prefix}/checkpoint-{checkpoint.next_tick}.json",
                        chunks=(stream.read(),),
                        maximum=32 * 1024**2,
                    )
                )
            prefixes = checkpoint.prefixes
        chunks: list[OutputChunk] = []
        for prefix in prefixes:
            before = self.before[prefix.name]
            if prefix.records == before.records:
                continue
            stream = self.streams[prefix.name]
            stream.seek(before.size_bytes)
            suffix = stream.read()
            stream.seek(0, io.SEEK_END)
            chunk = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/chunks/{prefix.name}-{before.records + 1}.jsonl",
                    chunks=(suffix,),
                    maximum=32 * 1024**2,
                )
            )
            ticks: list[int] = []
            for line in suffix.splitlines():
                tick = json.loads(s=line)["t"]
                assert isinstance(tick, int)
                ticks.append(tick)
            chunks.append(
                OutputChunk(
                    name=prefix.name,
                    file=chunk,
                    first_sequence=before.records + 1,
                    last_sequence=prefix.records,
                    byte_offset=before.size_bytes,
                    first_tick=min(ticks),
                    last_tick=max(ticks),
                )
            )
        return ProgressCommit(
            kind="terminal" if self.simulation.terminal else "checkpoint",
            reference=reference,
            snapshot_digest=self.snapshot.snapshot_digest,
            engine_version=self.snapshot.engine_version,
            next_tick=self.simulation.tick,
            executed_intervals=self.simulation.executed_intervals,
            prefixes=prefixes,
            metrics=report,
            chunks=tuple(chunks),
            events=tuple(self.pending),
        )

    def acknowledge(self, commit: ProgressCommit) -> None:
        self.before = {prefix.name: prefix for prefix in commit.prefixes}
        self.pending.clear()

    def candidate(self) -> tuple[StoredFile, BundleManifest]:
        staging = self.files.path(key=f"{self.prefix}/candidate")
        manifest = execute_bundle(snapshot=self.snapshot, destination=staging)
        final = self.files.path(key=f"{self.prefix}/bundles/{manifest.bundle_digest}")
        final.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(target=final)
        payload = (final / "manifest.json").read_bytes()
        return StoredFile(
            key=f"{self.prefix}/bundles/{manifest.bundle_digest}/manifest.json",
            sha256=hashlib.sha256(data=payload).hexdigest(),
            size_bytes=len(payload),
        ), manifest
