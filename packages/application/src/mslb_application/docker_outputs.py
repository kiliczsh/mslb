"""Bounded Docker spools and independently verified immutable artifact candidates."""

import asyncio
from collections.abc import Callable, Generator
from contextlib import ExitStack
from datetime import UTC, datetime
from typing import BinaryIO
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import HttpRecord, LifecycleRecord, RuntimeRecord, ScopeRecord
from mslb_contracts.docker_runs import CreationIntent, DockerManifest, DockerSnapshot
from mslb_contracts.docker_runtime import HttpEvent, TrafficSummary
from mslb_contracts.durability import StoredFile
from mslb_contracts.inputs import RequestInput, input_digest
from mslb_persistence.docker_records import ResourceRecord
from pydantic import ValidationError

from mslb_application.admission import settle
from mslb_application.errors import OperationError
from mslb_application.files import FileReference, ManagedFiles, WorkFile

RESOLVED = {"removed", "released", "abandoned"}


def require(*, condition: bool, message: str) -> None:
    if not condition:
        raise OperationError(code="VALIDATION_FAILED", message=message)


def chunks(*, stream: BinaryIO) -> Generator[bytes]:
    stream.seek(0)
    while block := stream.read(64 * 1024):
        yield block


def scoped(*, record: ScopeRecord, run_id: UUID, fence: int, snapshot: DockerSnapshot) -> None:
    require(
        condition=(record.run_id, record.fence, record.snapshot_digest)
        == (run_id, fence, snapshot.digest()),
        message="Docker artifact scope differs",
    )


def lifecycle_inventory(
    *, stream: BinaryIO, run_id: UUID, fence: int, snapshot: DockerSnapshot
) -> tuple[int, dict[str, tuple[int, int]]]:
    records: dict[UUID, LifecycleRecord] = {}
    capacities: dict[str, tuple[int, int]] = {}
    count = 0
    while raw := stream.readline(16 * 1024 + 1):
        count += 1
        require(
            condition=count <= 1024 and len(raw) <= 16 * 1024 and raw.endswith(b"\n"),
            message="Docker lifecycle stream exceeds its envelope",
        )
        event = LifecycleRecord.model_validate_json(json_data=raw)
        require(
            condition=canonical_bytes(value=event.model_dump(mode="json")) == raw,
            message="Docker lifecycle bytes are noncanonical",
        )
        scoped(record=event, run_id=run_id, fence=fence, snapshot=snapshot)
        require(
            condition=event.sequence == count
            and event.definition.deployment_id == snapshot.deployment_id,
            message="Docker lifecycle sequence/deployment differs",
        )
        previous = records.get(event.resource_id)
        if previous is None:
            require(
                condition=len(records) < 64
                and event.state == ("prepared" if event.kind == "dynamic" else "observed"),
                message="Docker lifecycle lacks an initial reservation",
            )
        else:
            require(
                condition=event.definition == previous.definition
                and event.kind == previous.kind
                and event.name == previous.name
                and previous.state not in RESOLVED
                and (previous.container_id is None or event.container_id == previous.container_id),
                message="Docker lifecycle immutable identity changed",
            )
        records[event.resource_id] = event
        active = [value for value in records.values() if value.state not in RESOLVED]
        require(
            condition=len(active) <= snapshot.configuration.max_replicas
            and len({value.definition.slot for value in active}) == len(active)
            and len({value.name for value in active}) == len(active)
            and len({value.container_id for value in active if value.container_id is not None})
            == sum(value.container_id is not None for value in active)
            and sum(value.kind == "dynamic" for value in active) <= 2,
            message="Docker lifecycle exceeds reserved capacity",
        )
        if event.state in {"observed", "started"}:
            definition = event.definition
            template = definition if isinstance(definition, CreationIntent) else definition.backend
            capacity = (template.logical_admission_mem_mb, template.max_inflight)
            require(
                condition=capacities.get(event.name, capacity) == capacity,
                message="Docker backend capacity changed identity",
            )
            capacities[event.name] = capacity
    require(
        condition=bool(records) and all(value.state in RESOLVED for value in records.values()),
        message="Docker lifecycle has unresolved resources",
    )
    return count, capacities


def verify_docker_outputs(
    *,
    files: ManagedFiles,
    manifest: DockerManifest,
    snapshot: DockerSnapshot,
    requests: tuple[RequestInput, ...],
) -> RuntimeRecord:
    require(
        condition=manifest.snapshot_digest == snapshot.digest()
        and manifest.total == len(requests)
        and input_digest(records=requests) == snapshot.dataset_digest,
        message="Docker publication inputs differ",
    )
    try:
        with files.verified_stream(
            reference=FileReference(**manifest.artifacts["runtime_summary.json"].model_dump())
        ) as stream:
            raw = stream.read(16 * 1024 + 1)
        require(condition=len(raw) <= 16 * 1024, message="Docker summary exceeds its envelope")
        runtime = RuntimeRecord.model_validate_json(json_data=raw)
        require(
            condition=canonical_bytes(value=runtime.model_dump(mode="json")) == raw,
            message="Docker summary bytes are noncanonical",
        )
        scoped(
            record=runtime,
            run_id=manifest.run_id,
            fence=manifest.publication_fence,
            snapshot=snapshot,
        )
        summary = runtime.traffic
        require(
            condition=summary.status == "completed"
            and summary.total == len(requests)
            and summary.peak_pending <= snapshot.configuration.max_pending
            and summary.peak_inflight <= snapshot.configuration.global_inflight,
            message="Docker summary is not a complete bounded execution",
        )
        with files.verified_stream(
            reference=FileReference(**manifest.artifacts["container_lifecycle.jsonl"].model_dump())
        ) as stream:
            lifecycle_count, capacities = lifecycle_inventory(
                stream=stream,
                run_id=manifest.run_id,
                fence=manifest.publication_fence,
                snapshot=snapshot,
            )
        require(
            condition=lifecycle_count == runtime.lifecycle_events,
            message="Docker lifecycle count differs",
        )
        expected = {row.request_id: row for row in requests}
        states: dict[str, HttpRecord] = {}
        counts = dict.fromkeys(("completed", "failed", "dropped", "unexecuted"), 0)
        sequence = 0
        last_elapsed = 0.0
        active_http: dict[str, HttpRecord] = {}
        with files.verified_stream(
            reference=FileReference(**manifest.artifacts["http_events.jsonl"].model_dump())
        ) as stream:
            while raw := stream.readline(16 * 1024 + 1):
                sequence += 1
                require(
                    condition=sequence <= len(requests) * 3
                    and len(raw) <= 16 * 1024
                    and raw.endswith(b"\n"),
                    message="Docker HTTP stream exceeds its envelope",
                )
                event = HttpRecord.model_validate_json(json_data=raw)
                require(
                    condition=canonical_bytes(value=event.model_dump(mode="json")) == raw,
                    message="Docker HTTP bytes are noncanonical",
                )
                scoped(
                    record=event,
                    run_id=manifest.run_id,
                    fence=manifest.publication_fence,
                    snapshot=snapshot,
                )
                require(
                    condition=event.sequence == sequence
                    and event.request_id in expected
                    and event.elapsed_seconds >= last_elapsed
                    and event.elapsed_seconds <= summary.elapsed_seconds,
                    message="Docker HTTP sequence/request/time differs",
                )
                last_elapsed = event.elapsed_seconds
                previous = states.get(event.request_id)
                allowed = {
                    None: {"arrived", "unexecuted"},
                    "arrived": {"admitted", "dropped", "unexecuted"},
                    "admitted": {"completed", "failed", "unexecuted"},
                }
                require(
                    condition=event.kind
                    in allowed.get(previous.kind if previous is not None else None, set()),
                    message="Docker HTTP outcome is duplicate or out of order",
                )
                if event.kind in {"admitted", "completed", "failed"}:
                    require(
                        condition=event.backend_id in capacities,
                        message="Docker HTTP backend was never owned/observed",
                    )
                if event.kind in {"completed", "failed"}:
                    require(
                        condition=previous is not None
                        and previous.backend_id == event.backend_id
                        and previous.elapsed_seconds < snapshot.configuration.duration_seconds,
                        message="Docker HTTP outcome changed backend",
                    )
                if event.kind == "arrived":
                    require(
                        condition=event.elapsed_seconds >= expected[event.request_id].t,
                        message="Docker arrival was observed before its offset",
                    )
                if event.kind == "admitted":
                    active_http[event.request_id] = event
                    require(
                        condition=len(active_http) <= snapshot.configuration.global_inflight,
                        message="Docker HTTP global inflight ceiling exceeded",
                    )
                    if event.backend_id is None:
                        raise OperationError(
                            code="VALIDATION_FAILED", message="Docker backend identity is missing"
                        )
                    memory, limit = capacities[event.backend_id]
                    assigned = [
                        item for item in active_http.values() if item.backend_id == event.backend_id
                    ]
                    require(
                        condition=len(assigned)
                        <= min(limit, snapshot.configuration.backend_inflight)
                        and sum(expected[item.request_id].mem_mb for item in assigned)
                        <= min(memory, snapshot.configuration.logical_admission_mem_mb),
                        message="Docker HTTP backend reservation ceiling exceeded",
                    )
                if event.result is not None:
                    require(
                        condition=event.result.operations
                        == min(expected[event.request_id].work_units * 2048, 2_000_000),
                        message="Docker HTTP operation count differs",
                    )
                if event.kind in counts:
                    counts[event.kind] += 1
                    active_http.pop(event.request_id, None)
                states[event.request_id] = event
        require(
            condition=set(states) == set(expected)
            and sum(counts.values()) == len(requests)
            and not active_http,
            message="Docker HTTP terminal accounting is incomplete",
        )
        for name, count in counts.items():
            require(
                condition=count == getattr(summary, name) == getattr(manifest, name),
                message="Docker HTTP terminal totals differ",
            )
        return runtime
    except (OSError, ValidationError):
        raise OperationError(
            code="VALIDATION_FAILED", message="Docker artifact is unavailable or invalid"
        ) from None


class DockerOutputs:
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
