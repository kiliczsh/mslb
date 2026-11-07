"""Operational artifact scope, lifecycle and HTTP accounting validation."""

from collections.abc import Generator
from typing import BinaryIO
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import HttpRecord, LifecycleRecord, RuntimeRecord, ScopeRecord
from mslb_contracts.docker_observations import BackendMeasurements
from mslb_contracts.docker_runs import CreationIntent, DockerManifest, DockerSnapshot
from mslb_contracts.inputs import RequestInput, input_digest
from mslb_contracts.traffic import MAX_OPERATIONS, OPERATIONS_PER_UNIT
from pydantic import ValidationError

from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.storage_values import FileReference

RESOLVED = {"removed", "released", "abandoned"}
# Bound each parsed record and summary before allocating Pydantic models. The
# larger version-2 summary includes scoped backend observations.
EVENT_LINE_BYTES = 16 * 1024
RUNTIME_SUMMARY_BYTES = 128 * 1024
MAX_LIFECYCLE_RECORDS = 1024
MAX_RESOURCE_IDENTITIES = 64


def require(*, condition: bool, message: str) -> None:
    """Reject an operational publication invariant with a stable validation error."""
    if not condition:
        raise OperationError(code="VALIDATION_FAILED", message=message)


def chunks(*, stream: BinaryIO) -> Generator[bytes]:
    """Yield artifact bytes in bounded blocks from the beginning of the stream."""
    stream.seek(0)
    while block := stream.read(64 * 1024):
        yield block


def scoped(*, record: ScopeRecord, run_id: UUID, fence: int, snapshot: DockerSnapshot) -> None:
    """Bind an operational record to the exact run, publication fence and snapshot."""
    require(
        condition=(record.run_id, record.fence, record.snapshot_digest)
        == (run_id, fence, snapshot.digest()),
        message="Docker artifact scope differs",
    )


def lifecycle_inventory(
    *,
    stream: BinaryIO,
    run_id: UUID,
    fence: int,
    snapshot: DockerSnapshot,
    observations: tuple[BackendMeasurements, ...] = (),
) -> tuple[int, dict[str, tuple[int, int]]]:
    """Verify resource history and derive approved capacities from scoped lifecycle records."""
    records: dict[UUID, LifecycleRecord] = {}
    capacities: dict[str, tuple[int, int]] = {}
    count = 0
    while raw := stream.readline(EVENT_LINE_BYTES + 1):
        count += 1
        require(
            condition=count <= MAX_LIFECYCLE_RECORDS
            and len(raw) <= EVENT_LINE_BYTES
            and raw.endswith(b"\n"),
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
                condition=len(records) < MAX_RESOURCE_IDENTITIES
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
    for item in observations:
        resource = records.get(item.first.resource_id)
        require(
            condition=resource is not None
            and resource.container_id == item.first.container_id
            and resource.name == item.first.health.backend_id,
            message="Measured backend was not present in the owned lifecycle",
        )
    return count, capacities


def verify_docker_outputs(
    *,
    files: ManagedFiles,
    manifest: DockerManifest,
    snapshot: DockerSnapshot,
    requests: tuple[RequestInput, ...],
) -> RuntimeRecord:
    """Reopen immutable operational files and independently verify scope and terminal accounting."""
    require(
        condition=manifest.snapshot_digest == snapshot.digest()
        and manifest.total == len(requests)
        and input_digest(records=requests) == snapshot.dataset_digest,
        message="Docker publication inputs differ",
    )
    try:
        runtime, capacities = _verified_runtime(
            files=files, manifest=manifest, snapshot=snapshot, requests=requests
        )
        summary = runtime.traffic
        expected = {row.request_id: row for row in requests}
        states: dict[str, HttpRecord] = {}
        counts: dict[str, int] = dict.fromkeys(("completed", "failed", "dropped", "unexecuted"), 0)
        sequence = 0
        last_elapsed = 0.0
        active_http: dict[str, HttpRecord] = {}
        pending: set[str] = set()
        peak_pending = peak_inflight = 0
        with files.verified_stream(
            reference=FileReference(**manifest.artifacts["http_events.jsonl"].model_dump())
        ) as stream:
            while raw := stream.readline(EVENT_LINE_BYTES + 1):
                sequence += 1
                require(
                    condition=sequence <= len(requests) * 3
                    and len(raw) <= EVENT_LINE_BYTES
                    and raw.endswith(b"\n"),
                    message="Docker HTTP stream exceeds its envelope",
                )
                event = HttpRecord.model_validate_json(json_data=raw)
                require(
                    condition=canonical_bytes(value=event.model_dump(mode="json")) == raw,
                    message="Docker HTTP bytes are noncanonical",
                )
                previous = states.get(event.request_id)
                _verify_http_event(
                    event=event,
                    previous=previous,
                    sequence=sequence,
                    last_elapsed=last_elapsed,
                    elapsed_seconds=summary.elapsed_seconds,
                    expected=expected,
                    capacities=capacities,
                    manifest=manifest,
                    snapshot=snapshot,
                )
                last_elapsed = event.elapsed_seconds
                if event.kind == "arrived":
                    require(
                        condition=event.elapsed_seconds >= expected[event.request_id].t,
                        message="Docker arrival was observed before its offset",
                    )
                    if len(pending) < snapshot.configuration.max_pending:
                        pending.add(event.request_id)
                        peak_pending = max(peak_pending, len(pending))
                if event.kind == "admitted":
                    require(
                        condition=event.request_id in pending,
                        message="Docker HTTP admission did not own a queue slot",
                    )
                    pending.remove(event.request_id)
                    active_http[event.request_id] = event
                    peak_inflight = max(peak_inflight, len(active_http))
                    require(
                        condition=len(active_http) <= snapshot.configuration.global_inflight,
                        message="Docker HTTP global inflight ceiling exceeded",
                    )
                    _verify_backend_reservation(
                        event=event,
                        active_http=active_http,
                        expected=expected,
                        capacities=capacities,
                        snapshot=snapshot,
                    )
                if event.result is not None:
                    require(
                        condition=event.result.operations
                        == min(
                            expected[event.request_id].work_units * OPERATIONS_PER_UNIT,
                            MAX_OPERATIONS,
                        ),
                        message="Docker HTTP operation count differs",
                    )
                if event.kind in counts:
                    if previous is not None and previous.kind == "arrived":
                        require(
                            condition=(event.request_id not in pending)
                            == (event.reason == "PENDING_OVERFLOW")
                            or event.kind == "unexecuted",
                            message="Docker pending refusal differs from queue membership",
                        )
                    pending.discard(event.request_id)
                    counts[event.kind] += 1
                    active_http.pop(event.request_id, None)
                states[event.request_id] = event
        _verify_http_totals(
            states=states,
            expected=expected,
            counts=counts,
            active_http=active_http,
            pending=pending,
            runtime=runtime,
            manifest=manifest,
            peak_pending=peak_pending,
            peak_inflight=peak_inflight,
        )
        return runtime
    except (OSError, ValidationError):
        raise OperationError(
            code="VALIDATION_FAILED", message="Docker artifact is unavailable or invalid"
        ) from None


def _verify_http_event(
    *,
    event: HttpRecord,
    previous: HttpRecord | None,
    sequence: int,
    last_elapsed: float,
    elapsed_seconds: float,
    expected: dict[str, RequestInput],
    capacities: dict[str, tuple[int, int]],
    manifest: DockerManifest,
    snapshot: DockerSnapshot,
) -> None:
    """Verify canonical scope, monotonic timing and a legal HTTP lifecycle transition."""
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
        and event.elapsed_seconds <= elapsed_seconds,
        message="Docker HTTP sequence/request/time differs",
    )
    allowed = {
        None: {"arrived", "unexecuted"},
        "arrived": {"admitted", "dropped", "unexecuted"},
        "admitted": {"completed", "failed", "unexecuted"},
    }
    require(
        condition=event.kind in allowed.get(previous.kind if previous is not None else None, set()),
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


def _verified_runtime(
    *,
    files: ManagedFiles,
    manifest: DockerManifest,
    snapshot: DockerSnapshot,
    requests: tuple[RequestInput, ...],
) -> tuple[RuntimeRecord, dict[str, tuple[int, int]]]:
    """Reopen the runtime summary and reconcile its complete owned resource history."""
    with files.verified_stream(
        reference=FileReference(**manifest.artifacts["runtime_summary.json"].model_dump())
    ) as stream:
        raw = stream.read(RUNTIME_SUMMARY_BYTES + 1)
    require(
        condition=len(raw) <= RUNTIME_SUMMARY_BYTES, message="Docker summary exceeds its envelope"
    )
    runtime = RuntimeRecord.model_validate_json(json_data=raw)
    require(
        condition=runtime.schema_version == 2 or len(raw) <= EVENT_LINE_BYTES,
        message="Legacy Docker summary exceeds its envelope",
    )
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
            observations=runtime.observations,
        )
    require(
        condition=lifecycle_count == runtime.lifecycle_events,
        message="Docker lifecycle count differs",
    )
    return runtime, capacities


def _verify_backend_reservation(
    *,
    event: HttpRecord,
    active_http: dict[str, HttpRecord],
    expected: dict[str, RequestInput],
    capacities: dict[str, tuple[int, int]],
    snapshot: DockerSnapshot,
) -> None:
    """Check actual backend membership, in-flight capacity and logical memory reservations."""
    if event.backend_id is None:
        raise OperationError(code="VALIDATION_FAILED", message="Docker backend identity is missing")
    memory, limit = capacities[event.backend_id]
    assigned = [item for item in active_http.values() if item.backend_id == event.backend_id]
    require(
        condition=len(assigned) <= min(limit, snapshot.configuration.backend_inflight)
        and sum(expected[item.request_id].mem_mb for item in assigned)
        <= min(memory, snapshot.configuration.logical_admission_mem_mb),
        message="Docker HTTP backend reservation ceiling exceeded",
    )


def _verify_http_totals(
    *,
    states: dict[str, HttpRecord],
    expected: dict[str, RequestInput],
    counts: dict[str, int],
    active_http: dict[str, HttpRecord],
    pending: set[str],
    runtime: RuntimeRecord,
    manifest: DockerManifest,
    peak_pending: int,
    peak_inflight: int,
) -> None:
    """Reconcile terminal request membership, outcome totals and measured concurrency peaks."""
    summary = runtime.traffic
    require(
        condition=set(states) == set(expected)
        and sum(counts.values()) == len(expected)
        and not active_http
        and not pending,
        message="Docker HTTP terminal accounting is incomplete",
    )
    for name, count in counts.items():
        require(
            condition=count == getattr(summary, name) == getattr(manifest, name),
            message="Docker HTTP terminal totals differ",
        )
    require(
        condition=(summary.peak_pending, summary.peak_inflight) == (peak_pending, peak_inflight),
        message="Docker HTTP observed peaks differ from the event history",
    )
