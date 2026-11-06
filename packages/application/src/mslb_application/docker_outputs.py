"""Bounded Docker spools and independently verified immutable artifact candidates."""

import asyncio
from collections.abc import Callable
from contextlib import ExitStack
from datetime import UTC, datetime
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import HttpRecord, LifecycleRecord, RuntimeRecord
from mslb_contracts.docker_runs import DockerManifest, DockerSnapshot
from mslb_contracts.docker_runtime import HttpEvent, TrafficSummary
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import RequestInput, input_digest
from mslb_persistence.docker_records import ResourceRecord

from mslb_application.admission import settle
from mslb_application.docker_output_validation import RESOLVED as RESOLVED
from mslb_application.docker_output_validation import chunks as chunks
from mslb_application.docker_output_validation import lifecycle_inventory as lifecycle_inventory
from mslb_application.docker_output_validation import require as require
from mslb_application.docker_output_validation import scoped as scoped
from mslb_application.docker_output_validation import verify_docker_outputs as verify_docker_outputs
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.work_file import WorkFile


class DockerOutputs:
    """Write bounded operational spools and publish independently verified Docker candidates."""

    def __init__(
        self,
        *,
        files: ManagedFiles,
        run_id: UUID,
        fence: int,
        snapshot: DockerSnapshot,
        requests: tuple[RequestInput, ...],
    ) -> None:
        require(
            condition=input_digest(records=requests) == snapshot.dataset_digest
            and len(requests) == snapshot.request_count
            and requests[-1].t == snapshot.latest_arrival,
            message="Docker output input identity differs",
        )
        self.files, self.run_id, self.fence = files, run_id, fence
        self.snapshot, self.requests = snapshot, requests
        self.prefix = f"docker/{run_id.hex}/{fence}/"
        self.stack = ExitStack()
        self.lock = asyncio.Lock()
        self.http: WorkFile | None = None
        self.lifecycle: WorkFile | None = None
        self.lifecycle_count = 0

    async def run[T](self, *, operation: Callable[[], T]) -> T:
        async with self.lock:
            task = asyncio.create_task(coro=asyncio.to_thread(operation))
            return await settle(task=task)

    async def __aenter__(self) -> "DockerOutputs":
        await self.run(operation=self.open)
        return self

    def open(self) -> None:
        try:
            self.http = self.stack.enter_context(
                cm=self.files.work_file(key=self.prefix + ".http.spool")
            )
            self.lifecycle = self.stack.enter_context(
                cm=self.files.work_file(key=self.prefix + ".lifecycle.spool")
            )
        except BaseException:
            self.stack.close()
            raise

    def close(self) -> None:
        self.stack.close()
        self.http, self.lifecycle = None, None

    async def __aexit__(self, *_exception: object) -> None:
        await self.run(operation=self.close)

    async def append(self, *, event: HttpEvent) -> None:
        await self.run(operation=lambda: self.write_event(event=event))

    def write_event(self, *, event: HttpEvent) -> None:
        if self.http is None:
            raise OperationError(code="INVALID_STATE", message="Docker output spool is not open")
        record = HttpRecord(
            **event.model_dump(),
            run_id=self.run_id,
            fence=self.fence,
            snapshot_digest=self.snapshot.digest(),
            observed_at=datetime.now(tz=UTC),
        )
        self.http.write(data=canonical_bytes(value=record.model_dump(mode="json")))

    async def observe(self, *, resource: ResourceRecord) -> None:
        await self.run(operation=lambda: self.write_resource(resource=resource))

    def write_resource(self, *, resource: ResourceRecord) -> None:
        """Append a scoped lifecycle observation from its trusted persisted definition."""
        if self.lifecycle is None:
            raise OperationError(code="INVALID_STATE", message="Docker lifecycle spool is not open")
        event = LifecycleRecord.model_validate(
            obj={
                "run_id": self.run_id,
                "fence": self.fence,
                "snapshot_digest": self.snapshot.digest(),
                "observed_at": datetime.now(tz=UTC),
                "sequence": self.lifecycle_count + 1,
                "resource_id": resource.id,
                "kind": resource.kind,
                "state": resource.state,
                "name": resource.name,
                "container_id": resource.container_id,
                "definition": resource.definition,
            }
        )
        self.lifecycle.write(data=canonical_bytes(value=event.model_dump(mode="json")))
        self.lifecycle_count += 1

    async def finalize(self, *, summary: TrafficSummary) -> DockerManifest:
        return await self.run(operation=lambda: self.publish(summary=summary))

    def publish(self, *, summary: TrafficSummary) -> DockerManifest:
        """Verify terminal accounting and return candidates for fenced database publication."""
        if self.http is None or self.lifecycle is None:
            raise OperationError(code="INVALID_STATE", message="Docker output spool is not open")
        self.http.sync()
        self.lifecycle.sync()
        runtime = RuntimeRecord(
            run_id=self.run_id,
            fence=self.fence,
            snapshot_digest=self.snapshot.digest(),
            observed_at=datetime.now(tz=UTC),
            traffic=summary,
            lifecycle_events=self.lifecycle_count,
        )
        artifacts: dict[str, StoredFile] = {}
        for name, blocks in (
            ("http_events.jsonl", chunks(stream=self.http.stream)),
            ("container_lifecycle.jsonl", chunks(stream=self.lifecycle.stream)),
            ("runtime_summary.json", (canonical_bytes(value=runtime.model_dump(mode="json")),)),
        ):
            saved = self.files.publish(key=self.prefix + name, chunks=blocks, maximum=256 * 1024**2)
            artifacts[name] = StoredFile(
                key=saved.key, sha256=saved.sha256, size_bytes=saved.size_bytes
            )
        manifest = DockerManifest(
            run_id=self.run_id,
            publication_fence=self.fence,
            snapshot_digest=self.snapshot.digest(),
            total=summary.total,
            completed=summary.completed,
            failed=summary.failed,
            dropped=summary.dropped,
            unexecuted=summary.unexecuted,
            artifacts=artifacts,
        )
        verify_docker_outputs(
            files=self.files, manifest=manifest, snapshot=self.snapshot, requests=self.requests
        )
        return manifest
