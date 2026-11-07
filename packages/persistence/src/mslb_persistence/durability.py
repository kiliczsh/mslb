"""Atomic fenced chunk/checkpoint/seal commits and final artifact visibility."""

import hashlib
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import MAX_LOGICAL_EVENTS
from mslb_contracts.checkpoints import ArtifactPrefix, PrefixName
from mslb_contracts.durability import (
    OutputChunk,
    PersistedEvent,
    ProgressCommit,
    RecoveryState,
    StoredFile,
)
from mslb_contracts.runs import BundleManifest, StoredRunSettings, stored_request_limit
from pydantic import TypeAdapter, ValidationError
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from mslb_persistence.database import Database
from mslb_persistence.errors import StoreError
from mslb_persistence.job_models import Job, Run
from mslb_persistence.jobs import _clock, _end_attempt, _slot, ownership_predicate
from mslb_persistence.output_models import (
    CommittedChunk,
    IndexedEvent,
    OutputProgress,
    PublishedArtifact,
    SavedCheckpoint,
)
from mslb_persistence.records import JobRecord, Lease

NAMES: tuple[PrefixName, ...] = (
    "run.jsonl",
    "autoscaling_decisions.jsonl",
    "timeline.jsonl",
    "drop_reasons.jsonl",
)
EMPTY_HASH = hashlib.sha256(data=b"").hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise StoreError(code="INVALID_STATE", message=message)


def _file(value: dict[str, object]) -> StoredFile:
    return StoredFile.model_validate_json(json_data=canonical_bytes(value=value))


def _owned(lease: Lease, reference: StoredFile) -> None:
    _require(
        condition=reference.key.startswith(f"runs/{lease.run_id}/attempt-{lease.fence}/")
        and all(part not in {"", ".", ".."} for part in reference.key.split(sep="/")),
        message="Output must use the current run/attempt namespace",
    )


def _prefixes(row: OutputProgress | None) -> dict[str, ArtifactPrefix]:
    if row is None:
        return {
            name: ArtifactPrefix(name=name, size_bytes=0, records=0, sha256=EMPTY_HASH)
            for name in NAMES
        }
    result = tuple(
        ArtifactPrefix.model_validate_json(json_data=canonical_bytes(value=item))
        for item in row.prefixes
    )
    return {prefix.name: prefix for prefix in result}


async def _job(session: AsyncSession, lease: Lease) -> Job:
    job = await session.scalar(
        statement=select(Job).where(ownership_predicate(lease=lease)).with_for_update()
    )
    if job is None:
        raise StoreError(code="FENCE_LOST", message="Worker ownership is no longer current")
    if job.status == "cancel_requested":
        raise StoreError(code="INTERRUPTED", message="Cancellation wins over output publication")
    return job


async def _guard(session: AsyncSession, lease: Lease) -> None:
    # This is the last mutation before commit. Earlier chunk/index/reference
    # inserts roll back together if the lease expired during the transaction.
    current = await session.scalar(
        statement=update(table=Job)
        .where(ownership_predicate(lease=lease), Job.status == "running")
        .values(fence=Job.fence)
        .returning(Job.run_id)
    )
    if current is None:
        raise StoreError(code="FENCE_LOST", message="Worker lease expired before progress commit")


async def _verify_index(
    session: AsyncSession, run_id: UUID, prefixes: dict[str, ArtifactPrefix]
) -> None:
    """Verify canonical indexed events match the exact committed chunk accounting."""
    trace, drops, count = hashlib.sha256(), hashlib.sha256(), 0
    rows = await session.stream_scalars(
        statement=select(IndexedEvent)
        .where(IndexedEvent.run_id == run_id)
        .order_by(IndexedEvent.sequence),
        execution_options={"yield_per": 500},
    )
    try:
        async for row in rows:
            try:
                event = PersistedEvent.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "sequence": row.sequence,
                            "t": row.t,
                            "event": row.event,
                            "request_id": row.request_id,
                            "server_id": row.server_id,
                            "reason": row.reason,
                        }
                    )
                )
            except ValidationError:
                raise StoreError(
                    code="VALIDATION_FAILED", message="Committed event index schema is invalid"
                ) from None
            count += 1
            if event.sequence != count:
                raise StoreError(
                    code="VALIDATION_FAILED", message="Committed event index is not contiguous"
                )
            trace.update(event.canonical_trace_line())
            if event.reason is not None:
                drops.update(
                    canonical_bytes(
                        value={"t": event.t, "request_id": event.request_id, "reason": event.reason}
                    )
                )
    finally:
        await rows.close()
    if (
        count != prefixes["run.jsonl"].records
        or trace.hexdigest() != prefixes["run.jsonl"].sha256
        or drops.hexdigest() != prefixes["drop_reasons.jsonl"].sha256
    ):
        raise StoreError(
            code="VALIDATION_FAILED", message="Committed event index differs from sealed output"
        )


def _validate(
    lease: Lease, data: ProgressCommit, previous: OutputProgress | None, run: Run
) -> None:
    """Reject noncontiguous prefixes and inconsistent checkpoint/seal accounting before commit."""
    _owned(lease=lease, reference=data.reference)
    _require(
        condition=data.snapshot_digest == run.snapshot_digest
        and data.engine_version == run.engine_version,
        message="Progress belongs to incompatible frozen inputs",
    )
    _require(
        condition=previous is None or previous.kind != "terminal",
        message="Terminal output is already sealed",
    )
    old_tick = previous.next_tick if previous is not None else 0
    old_intervals = previous.executed_intervals if previous is not None else 0
    horizon = run.configuration.get("max_simulation_ticks")
    try:
        request_capacity = stored_request_limit(
            settings=TypeAdapter(StoredRunSettings).validate_python(run.configuration)
        )
    except ValueError:
        raise StoreError(
            code="VALIDATION_FAILED", message="Stored capacity profile is invalid"
        ) from None
    _require(
        condition=type(horizon) is int and data.next_tick <= horizon,
        message="Progress exceeds the frozen horizon",
    )
    if data.kind == "checkpoint":
        _require(
            condition=data.metrics is None
            and old_tick < data.next_tick
            and old_intervals < data.executed_intervals <= data.next_tick
            and 0 < data.reference.size_bytes <= 32 * 1024**2,
            message="Checkpoint must represent a new completed interval",
        )
    else:
        _require(
            condition=data.metrics is not None
            and data.next_tick >= old_tick
            and old_intervals <= data.executed_intervals <= data.next_tick
            and 0 < data.reference.size_bytes <= 1024**2,
            message="Terminal seal has invalid boundary or metrics",
        )
        if data.metrics is not None:
            _owned(lease=lease, reference=data.metrics)
    prefixes = {prefix.name: prefix for prefix in data.prefixes}
    _require(
        condition=set(prefixes) == set(NAMES), message="All unique output prefixes are required"
    )
    previous_prefixes = _prefixes(row=previous)
    for name in NAMES:
        before, after = previous_prefixes[name], prefixes[name]
        sequence, offset = before.records + 1, before.size_bytes
        chunks = sorted(
            (chunk for chunk in data.chunks if chunk.name == name), key=lambda c: c.first_sequence
        )
        for chunk in chunks:
            _owned(lease=lease, reference=chunk.file)
            _require(
                condition=chunk.first_sequence == sequence
                and chunk.byte_offset == offset
                and chunk.last_sequence >= chunk.first_sequence
                and 0 < chunk.file.size_bytes <= 32 * 1024**2
                and old_tick <= chunk.first_tick <= chunk.last_tick <= data.next_tick
                and (data.kind == "terminal" or chunk.last_tick < data.next_tick),
                message="Output chunk ranges are not contiguous",
            )
            sequence, offset = chunk.last_sequence + 1, offset + chunk.file.size_bytes
        _require(
            condition=after.records == sequence - 1
            and after.size_bytes == offset
            and (bool(chunks) or after.sha256 == before.sha256),
            message="Prefix does not match committed chunks",
        )
    _require(
        condition=prefixes["run.jsonl"].records <= 3 * request_capacity
        and prefixes["drop_reasons.jsonl"].records <= request_capacity
        and prefixes["timeline.jsonl"].records
        == data.executed_intervals + (data.kind == "terminal"),
        message="Output counts exceed the frozen execution envelope",
    )
    first = previous_prefixes["run.jsonl"].records + 1
    _require(
        condition=tuple(event.sequence for event in data.events)
        == tuple(range(first, prefixes["run.jsonl"].records + 1)),
        message="Event index must commit exactly the contiguous trace suffix",
    )
    _require(
        condition=all(
            old_tick <= event.t <= data.next_tick
            and (data.kind == "terminal" or event.t < data.next_tick)
            for event in data.events
        ),
        message="Event index crosses an unprocessed boundary",
    )
    _require(
        condition=sum(prefix.size_bytes for prefix in data.prefixes)
        + data.reference.size_bytes
        + (data.metrics.size_bytes if data.metrics is not None else 0)
        <= 512 * 1024**2,
        message="Committed output exceeds the run storage ceiling",
    )


def _recovery(row: OutputProgress, run: Run) -> RecoveryState:
    return RecoveryState.model_validate_json(
        json_data=canonical_bytes(
            value={
                "kind": row.kind,
                "reference": row.reference,
                "snapshot_digest": run.snapshot_digest,
                "engine_version": run.engine_version,
                "next_tick": row.next_tick,
                "executed_intervals": row.executed_intervals,
                "prefixes": row.prefixes,
                "metrics": row.metrics,
                "revision": row.revision,
            }
        )
    )


class Durability:
    """Commit checkpoint, chunk and publication references under a current logical-job fence."""

    def __init__(self, database: Database) -> None:
        self.database = database

    async def removable(self, lease: Lease, key: str) -> bool:
        """Authorize exact, unreferenced run files while current ownership is valid."""
        prefix = f"runs/{lease.run_id}/attempt-"
        suffix = key.removeprefix(prefix).split(sep="/", maxsplit=1)
        if (
            not key.startswith(prefix)
            or len(suffix) != 2
            or not suffix[0].isdigit()
            or not 1 <= int(suffix[0]) <= lease.fence
            or any(part in {"", ".", ".."} for part in key.split(sep="/"))
        ):
            raise StoreError(
                code="INVALID_STATE", message="Cleanup is outside the owned run namespace"
            )

        async def operation(session: AsyncSession) -> bool:
            await _job(session=session, lease=lease)
            queries = (
                select(SavedCheckpoint.run_id).where(
                    SavedCheckpoint.reference["key"].as_string() == key
                ),
                select(OutputProgress.run_id).where(
                    OutputProgress.reference["key"].as_string() == key
                ),
                select(OutputProgress.run_id).where(
                    OutputProgress.metrics["key"].as_string() == key
                ),
                select(CommittedChunk.run_id).where(CommittedChunk.file["key"].as_string() == key),
                select(PublishedArtifact.run_id).where(
                    PublishedArtifact.manifest["key"].as_string() == key
                ),
            )
            for query in queries:
                if await session.scalar(statement=select(query.exists())) is True:
                    return False
            return True

        return await self.database.transaction(operation=operation)

    async def recovery(self, lease: Lease) -> RecoveryState | None:
        async def operation(session: AsyncSession) -> RecoveryState | None:
            await _job(session=session, lease=lease)
            run = await session.get(entity=Run, ident=lease.run_id)
            if run is None:
                raise StoreError(code="NOT_READY", message="Frozen run is missing")
            row = await session.get(entity=OutputProgress, ident=lease.run_id)
            return _recovery(row=row, run=run) if row is not None else None

        return await self.database.transaction(operation=operation)

    async def commit(self, lease: Lease, data: ProgressCommit) -> tuple[StoredFile, ...]:
        """Commit all references/indexes/progress together; return retired checkpoints."""

        async def operation(session: AsyncSession) -> tuple[StoredFile, ...]:
            await _job(session=session, lease=lease)
            run = await session.get(entity=Run, ident=lease.run_id)
            if run is None:
                raise StoreError(code="NOT_READY", message="Frozen run is missing")
            previous = await session.get(entity=OutputProgress, ident=lease.run_id)
            _validate(lease=lease, data=data, previous=previous, run=run)
            revision = previous.revision + 1 if previous is not None else 1
            for chunk in data.chunks:
                session.add(
                    instance=CommittedChunk(
                        run_id=lease.run_id,
                        name=chunk.name,
                        first_sequence=chunk.first_sequence,
                        last_sequence=chunk.last_sequence,
                        byte_offset=chunk.byte_offset,
                        first_tick=chunk.first_tick,
                        last_tick=chunk.last_tick,
                        attempt=lease.attempt,
                        fence=lease.fence,
                        file=chunk.file.model_dump(mode="json"),
                    )
                )
            for event in data.events:
                session.add(instance=IndexedEvent(run_id=lease.run_id, **event.model_dump()))
            values = {
                "revision": revision,
                "kind": data.kind,
                "reference": data.reference.model_dump(mode="json"),
                "next_tick": data.next_tick,
                "executed_intervals": data.executed_intervals,
                "prefixes": [p.model_dump(mode="json") for p in data.prefixes],
                "metrics": data.metrics.model_dump(mode="json")
                if data.metrics is not None
                else None,
            }
            if previous is None:
                session.add(instance=OutputProgress(run_id=lease.run_id, **values))
            else:
                previous.revision, previous.kind = revision, data.kind
                previous.reference, previous.next_tick = (
                    data.reference.model_dump(mode="json"),
                    data.next_tick,
                )
                previous.executed_intervals = data.executed_intervals
                previous.prefixes = [p.model_dump(mode="json") for p in data.prefixes]
                previous.metrics = (
                    data.metrics.model_dump(mode="json") if data.metrics is not None else None
                )
            if data.kind == "checkpoint":
                session.add(
                    instance=SavedCheckpoint(
                        run_id=lease.run_id,
                        revision=revision,
                        attempt=lease.attempt,
                        fence=lease.fence,
                        reference=data.reference.model_dump(mode="json"),
                        next_tick=data.next_tick,
                    )
                )
            await session.flush()
            retired = tuple(
                await session.scalars(
                    statement=select(SavedCheckpoint)
                    .where(SavedCheckpoint.run_id == lease.run_id)
                    .order_by(SavedCheckpoint.revision.desc())
                    .offset(offset=2)
                    .limit(limit=500)
                )
            )
            references = tuple(_file(value=row.reference) for row in retired)
            for row in retired:
                await session.delete(instance=row)
            await session.flush()
            await _guard(session=session, lease=lease)
            return references

        return await self.database.transaction(operation=operation)

    async def chunks(
        self, run_id: UUID, name: PrefixName, after_sequence: int = 0, limit: int = 500
    ) -> tuple[OutputChunk, ...]:
        """Read a bounded ordered chunk page from committed output references."""
        if (
            type(limit) is not int
            or not 1 <= limit <= 500
            or type(after_sequence) is not int
            or after_sequence < 0
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid chunk page")

        async def operation(session: AsyncSession) -> tuple[OutputChunk, ...]:
            rows = await session.scalars(
                statement=select(CommittedChunk)
                .where(
                    CommittedChunk.run_id == run_id,
                    CommittedChunk.name == name,
                    CommittedChunk.first_sequence > after_sequence,
                )
                .order_by(CommittedChunk.first_sequence)
                .limit(limit=limit)
            )
            return tuple(
                OutputChunk.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "name": row.name,
                            "file": row.file,
                            "first_sequence": row.first_sequence,
                            "last_sequence": row.last_sequence,
                            "byte_offset": row.byte_offset,
                            "first_tick": row.first_tick,
                            "last_tick": row.last_tick,
                        }
                    )
                )
                for row in rows
            )

        return await self.database.transaction(operation=operation)

    async def events(
        self,
        run_id: UUID,
        after_sequence: int = 0,
        limit: int = 100,
        start_tick: int = 0,
        end_tick: int = 1_000_000,
    ) -> tuple[PersistedEvent, ...]:
        """Read only committed canonical events in ascending sequence."""
        if any(
            type(value) is not int for value in (after_sequence, limit, start_tick, end_tick)
        ) or not (
            0 <= after_sequence <= MAX_LOGICAL_EVENTS
            and 1 <= limit <= 500
            and 0 <= start_tick <= end_tick <= 1_000_000
        ):
            raise StoreError(code="VALIDATION_ERROR", message="Invalid event page")

        async def operation(session: AsyncSession) -> tuple[PersistedEvent, ...]:
            if await session.get(entity=Job, ident=run_id) is None:
                raise StoreError(code="NOT_FOUND", message="Run does not exist")
            rows = await session.scalars(
                statement=select(IndexedEvent)
                .where(
                    IndexedEvent.run_id == run_id,
                    IndexedEvent.sequence > after_sequence,
                    IndexedEvent.t >= start_tick,
                    IndexedEvent.t <= end_tick,
                )
                .order_by(IndexedEvent.sequence)
                .limit(limit=limit)
            )
            return tuple(
                PersistedEvent.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "sequence": row.sequence,
                            "t": row.t,
                            "event": row.event,
                            "request_id": row.request_id,
                            "server_id": row.server_id,
                            "reason": row.reason,
                        }
                    )
                )
                for row in rows
            )

        return await self.database.transaction(operation=operation)

    async def complete(
        self, lease: Lease, reference: StoredFile, manifest: BundleManifest
    ) -> JobRecord:
        """Publish only a worker-validated candidate matching a committed terminal seal."""
        _owned(lease=lease, reference=reference)
        _require(
            condition=reference.key.endswith(f"/bundles/{manifest.bundle_digest}/manifest.json"),
            message="Candidate path is not content addressed",
        )

        async def operation(session: AsyncSession) -> JobRecord:
            slot = await _slot(session=session)
            if slot is None or slot.run_id != lease.run_id:
                raise StoreError(code="FENCE_LOST", message="Execution slot has another owner")
            job = await _job(session=session, lease=lease)
            run = await session.get(entity=Run, ident=lease.run_id)
            progress = await session.get(entity=OutputProgress, ident=lease.run_id)
            _require(
                condition=run is not None and progress is not None and progress.kind == "terminal",
                message="Completion requires a committed terminal seal",
            )
            if run is None or progress is None:
                raise StoreError(code="NOT_READY", message="Completion references are missing")
            prefixes = _prefixes(row=progress)
            _require(
                condition=manifest.snapshot_digest == run.snapshot_digest
                and manifest.engine_version == run.engine_version
                and manifest.terminal_tick == progress.next_tick
                and manifest.event_count == prefixes["run.jsonl"].records
                and manifest.decision_count == prefixes["autoscaling_decisions.jsonl"].records,
                message="Validated manifest does not match terminal identity or sequences",
            )
            for prefix in prefixes.values():
                artifact = manifest.artifacts.get(prefix.name)
                _require(
                    condition=artifact is not None
                    and artifact.sha256 == prefix.sha256
                    and artifact.size_bytes == prefix.size_bytes,
                    message="Validated artifact differs from the sealed prefix",
                )
            _require(
                condition=progress.metrics is not None,
                message="Terminal metrics reference is missing",
            )
            if progress.metrics is None:
                raise StoreError(code="NOT_READY", message="Terminal metrics are missing")
            metrics = _file(value=progress.metrics)
            report = manifest.artifacts.get("metrics.json")
            _require(
                condition=report is not None
                and report.sha256 == metrics.sha256
                and report.size_bytes == metrics.size_bytes,
                message="Validated metrics differ from terminal seal",
            )
            encoded = canonical_bytes(value=manifest.model_dump(mode="json"))
            content_digest = hashlib.sha256(
                data=canonical_bytes(
                    value=manifest.model_dump(mode="json", exclude={"bundle_digest"})
                )
            ).hexdigest()
            _require(
                condition=reference.size_bytes == len(encoded)
                and reference.sha256 == hashlib.sha256(data=encoded).hexdigest()
                and manifest.bundle_digest == content_digest,
                message="Candidate manifest byte identity mismatch",
            )
            await _verify_index(session=session, run_id=lease.run_id, prefixes=prefixes)
            session.add(
                instance=PublishedArtifact(
                    run_id=lease.run_id,
                    manifest=reference.model_dump(mode="json"),
                    bundle_digest=manifest.bundle_digest,
                )
            )
            now = await _clock(session=session)
            await _end_attempt(session=session, job=job, outcome="completed", now=now)
            slot.run_id = None
            await session.flush()
            updated = await session.scalar(
                statement=update(table=Job)
                .where(ownership_predicate(lease=lease), Job.status == "running")
                .values(
                    status="completed",
                    fence=Job.fence + 1,
                    owner=None,
                    lease_expires_at=None,
                    error_code=None,
                    completed_at=func.clock_timestamp(),
                )
                .returning(Job)
            )
            if updated is None:
                raise StoreError(
                    code="FENCE_LOST", message="Worker lost ownership before completion commit"
                )
            return JobRecord.from_model(row=updated)

        return await self.database.transaction(operation=operation)

    async def published(self, run_id: UUID) -> StoredFile:
        """Return only the final manifest reference associated with durable completion."""

        async def operation(session: AsyncSession) -> StoredFile:
            job = await session.get(entity=Job, ident=run_id)
            if job is None:
                raise StoreError(code="NOT_FOUND", message="Run does not exist")
            if job.status != "completed":
                raise StoreError(code="INVALID_STATE", message="Run has no completed artifact")
            artifact = await session.get(entity=PublishedArtifact, ident=run_id)
            if artifact is None:
                raise StoreError(code="NOT_READY", message="Completed artifact pointer is missing")
            return _file(value=artifact.manifest)

        return await self.database.transaction(operation=operation)
