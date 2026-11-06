"""Attempt-private journals, committed-prefix recovery, and sealed finalization."""

import asyncio
import hashlib
import json
import tempfile
from collections.abc import Awaitable, Callable, Generator, Iterable
from contextlib import ExitStack
from dataclasses import replace
from pathlib import Path
from typing import BinaryIO

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.checkpoints import ArtifactPrefix, PrefixName
from mslb_contracts.durability import (
    OutputChunk,
    PersistedEvent,
    ProgressCommit,
    RecoveryState,
    StoredFile,
    TerminalSeal,
)
from mslb_contracts.runs import BundleManifest, RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.models import TickFrame
from mslb_engine.simulator import Simulation
from mslb_persistence.durability import Durability
from mslb_persistence.records import Lease

from mslb_application.artifact_validation import validate_artifact_files
from mslb_application.artifact_writers import timeline_record
from mslb_application.checkpoint_seals import capture_terminal_seal, read_terminal_seal
from mslb_application.checkpoint_values import PREFIX_NAMES, CheckpointArtifact
from mslb_application.checkpoints import checkpoint_due, read_checkpoint, write_checkpoint
from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.snapshots import domain_config, domain_inputs
from mslb_application.storage_encoding import request_bytes
from mslb_application.storage_values import FileReference
from mslb_application.work_file import WorkFile


def on_loop[T](loop: asyncio.AbstractEventLoop, operation: Awaitable[T]) -> T:
    """Bridge the execution thread to the independently running async supervisor."""

    async def wait() -> T:
        return await operation

    future = asyncio.run_coroutine_threadsafe(coro=wait(), loop=loop)
    try:
        return future.result(timeout=20)
    except TimeoutError:
        future.cancel()
        raise


def json_chunks(value: object) -> Generator[bytes]:
    encoder = json.JSONEncoder(
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    for piece in encoder.iterencode(o=value):
        yield piece.encode()
    yield b"\n"


def stored(reference: FileReference) -> StoredFile:
    return StoredFile(key=reference.key, sha256=reference.sha256, size_bytes=reference.size_bytes)


def file_reference(reference: StoredFile) -> FileReference:
    return FileReference(
        key=reference.key, sha256=reference.sha256, size_bytes=reference.size_bytes
    )


def stream_chunks(stream: BinaryIO, length: int) -> Generator[bytes]:
    remaining = length
    while remaining:
        block = stream.read(min(64 * 1024, remaining))
        if not block:
            raise OperationError(code="VALIDATION_FAILED", message="Committed stream was truncated")
        remaining -= len(block)
        yield block


class Journal:
    """Own one attempt's spools, committed prefixes, recovery and sealed publication."""

    def __init__(
        self,
        files: ManagedFiles,
        snapshot: RunSnapshot,
        lease: Lease,
        repository: Durability,
        loop: asyncio.AbstractEventLoop,
        check: Callable[[], None],
        stack: ExitStack,
        recovery: RecoveryState | None,
    ) -> None:
        self.files, self.snapshot, self.lease = files, snapshot, lease
        self.repository, self.loop, self.check = repository, loop, check
        self.prefix = f"runs/{lease.run_id}/attempt-{lease.fence}"
        self.writers: dict[PrefixName, WorkFile] = {
            name: stack.enter_context(cm=files.work_file(key=f"{self.prefix}/work/{name}.part"))
            for name in PREFIX_NAMES
        }
        for writer in self.writers.values():
            writer.check = check
        self.streams: dict[str, BinaryIO] = {
            name: writer.stream for name, writer in self.writers.items()
        }
        self.before = {
            name: ArtifactPrefix(
                name=name, size_bytes=0, records=0, sha256=hashlib.sha256(data=b"").hexdigest()
            )
            for name in PREFIX_NAMES
        }
        self.records = dict.fromkeys(PREFIX_NAMES, 0)
        self.first_ticks: dict[str, int] = {}
        self.last_ticks: dict[str, int] = {}
        self.pending: list[PersistedEvent] = []
        self.last_checkpoint: CheckpointArtifact | None = None
        self.pending_checkpoint: CheckpointArtifact | None = None
        self.seal: TerminalSeal | None = None
        self.simulation: Simulation | None = None
        self.metrics: MetricAccumulator | None = None
        if recovery is None:
            servers, requests = domain_inputs(snapshot=snapshot)
            self.simulation = Simulation(
                servers=servers,
                requests=requests,
                config=domain_config(settings=snapshot.configuration),
            )
            self.metrics = MetricAccumulator(
                requests=requests,
                servers=servers,
                max_series=snapshot.configuration.max_metric_series,
            )
        else:
            self.restore(recovery=recovery)

    def restore(self, recovery: RecoveryState) -> None:
        """Verify committed prefixes and restore interval state or sealed finalization."""
        if (
            recovery.snapshot_digest != self.snapshot.snapshot_digest
            or recovery.engine_version != self.snapshot.engine_version
        ):
            raise OperationError(
                code="VALIDATION_FAILED", message="Recovery input identity mismatch"
            )
        for prefix in recovery.prefixes:
            count, offset, digest = 0, 0, hashlib.sha256()
            after = 0
            while True:
                page = on_loop(
                    loop=self.loop,
                    operation=self.repository.chunks(
                        run_id=self.lease.run_id, name=prefix.name, after_sequence=after
                    ),
                )
                if not page:
                    break
                for chunk in page:
                    self.check()
                    if chunk.first_sequence != count + 1 or chunk.byte_offset != offset:
                        raise OperationError(
                            code="VALIDATION_FAILED", message="Committed chunks are not contiguous"
                        )
                    records = 0
                    with self.files.verified_stream(
                        reference=file_reference(reference=chunk.file)
                    ) as stream:
                        while data := stream.read(64 * 1024):
                            self.check()
                            self.writers[prefix.name].write(data=data)
                            digest.update(data)
                            records += data.count(b"\n")
                    if records != chunk.last_sequence - chunk.first_sequence + 1:
                        raise OperationError(
                            code="VALIDATION_FAILED", message="Chunk sequence count mismatch"
                        )
                    count, offset, after = (
                        chunk.last_sequence,
                        offset + chunk.file.size_bytes,
                        chunk.last_sequence,
                    )
            self.writers[prefix.name].sync()
            if (count, offset, digest.hexdigest()) != (
                prefix.records,
                prefix.size_bytes,
                prefix.sha256,
            ):
                raise OperationError(
                    code="VALIDATION_FAILED", message="Recovered prefix digest mismatch"
                )
            self.before[prefix.name], self.records[prefix.name] = prefix, prefix.records
        with self.files.verified_stream(
            reference=file_reference(reference=recovery.reference)
        ) as state:
            if recovery.kind == "terminal":
                seal = read_terminal_seal(
                    stream=state, expected_digest=recovery.reference.sha256, snapshot=self.snapshot
                )
                if (
                    seal.prefixes != recovery.prefixes
                    or seal.metrics != recovery.metrics
                    or seal.terminal_tick != recovery.next_tick
                    or seal.executed_intervals != recovery.executed_intervals
                ):
                    raise OperationError(
                        code="VALIDATION_FAILED",
                        message="Terminal seal differs from committed metadata",
                    )
                self.seal = seal
            else:
                restored = read_checkpoint(
                    stream=state,
                    expected_digest=recovery.reference.sha256,
                    snapshot=self.snapshot,
                    prefixes=self.streams,
                )
                if (
                    restored.simulation.tick != recovery.next_tick
                    or restored.prefixes != recovery.prefixes
                ):
                    raise OperationError(
                        code="VALIDATION_FAILED",
                        message="Checkpoint differs from committed progress",
                    )
                self.simulation, self.metrics = restored.simulation, restored.metrics
                self.last_checkpoint = CheckpointArtifact(
                    path=self.files.path(key=recovery.reference.key),
                    sha256=recovery.reference.sha256,
                    size_bytes=recovery.reference.size_bytes,
                    next_tick=recovery.next_tick,
                    executed_intervals=recovery.executed_intervals,
                    event_count=self.records["run.jsonl"],
                    prefixes=recovery.prefixes,
                )

    def _record(self, name: PrefixName, pieces: Iterable[bytes], tick: int) -> None:
        self.first_ticks.setdefault(name, tick)
        self.last_ticks[name] = tick
        for piece in pieces:
            self.writers[name].write(data=piece)
        self.records[name] += 1

    def append(self, frame: TickFrame) -> None:
        """Append one observed frame to attempt-private spools and exact event indexes."""
        if self.metrics is None:
            raise OperationError(
                code="INVALID_STATE", message="Sealed attempts cannot execute again"
            )
        self.metrics.observe(frame=frame)
        self._record(
            name="timeline.jsonl",
            pieces=json_chunks(value=timeline_record(frame=frame)),
            tick=frame.t,
        )
        for event in frame.events:
            self._record(name="run.jsonl", pieces=(event.canonical_line().encode(),), tick=frame.t)
            self.pending.append(
                PersistedEvent.model_validate_json(
                    json_data=canonical_bytes(
                        value={
                            "sequence": self.records["run.jsonl"],
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
                self._record(
                    name="drop_reasons.jsonl",
                    pieces=json_chunks(
                        value={"t": event.t, "request_id": event.request_id, "reason": event.reason}
                    ),
                    tick=frame.t,
                )
        if frame.scale_decision is not None:
            self._record(
                name="autoscaling_decisions.jsonl",
                pieces=(frame.scale_decision.canonical_line().encode(),),
                tick=frame.t,
            )

    def checkpoint_needed(self) -> bool:
        return self.simulation is not None and (
            checkpoint_due(simulation=self.simulation, previous=self.last_checkpoint)
            or any(
                writer.position - self.before[name].size_bytes >= 256 * 1024
                for name, writer in self.writers.items()
            )
        )

    def describe(self) -> ProgressCommit:
        """Publish immutable contiguous chunks without making their suffix visible early."""
        self.check()
        simulation, metrics = self.simulation, self.metrics
        if simulation is None or metrics is None:
            raise OperationError(
                code="INVALID_STATE", message="Cannot write progress for sealed execution"
            )
        for writer in self.writers.values():
            writer.sync()
        report = None
        if simulation.terminal:
            report = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/metrics.json",
                    chunks=json_chunks(value=metrics.summary()),
                    maximum=256 * 1024**2,
                )
            )
            seal = capture_terminal_seal(
                snapshot=self.snapshot,
                simulation=simulation,
                metrics=metrics,
                streams=self.streams,
                report=report,
            )
            reference = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/terminal.json",
                    chunks=json_chunks(value=seal.model_dump(mode="json")),
                    maximum=1024**2,
                )
            )
            prefixes = seal.prefixes
            self.seal = seal
        else:
            with tempfile.TemporaryDirectory(prefix="mslb-checkpoint-") as temporary:
                checkpoint = write_checkpoint(
                    directory=Path(temporary),
                    snapshot=self.snapshot,
                    simulation=simulation,
                    metrics=metrics,
                    prefixes=self.streams,
                )
                with checkpoint.path.open(mode="rb") as source:
                    reference = stored(
                        reference=self.files.publish(
                            key=f"{self.prefix}/checkpoints/{checkpoint.sha256}.json",
                            chunks=stream_chunks(stream=source, length=checkpoint.size_bytes),
                            maximum=32 * 1024**2,
                        )
                    )
                prefixes = checkpoint.prefixes
                self.pending_checkpoint = replace(
                    checkpoint, path=self.files.path(key=reference.key)
                )
        chunks: list[OutputChunk] = []
        for prefix in prefixes:
            before = self.before[prefix.name]
            if before.records == prefix.records:
                continue
            self.check()
            stream = self.streams[prefix.name]
            stream.seek(before.size_bytes)
            reference_chunk = stored(
                reference=self.files.publish(
                    key=f"{self.prefix}/chunks/{prefix.name}-{before.records + 1:07d}.jsonl",
                    chunks=stream_chunks(
                        stream=stream, length=prefix.size_bytes - before.size_bytes
                    ),
                    maximum=32 * 1024**2,
                )
            )
            chunks.append(
                OutputChunk(
                    name=prefix.name,
                    file=reference_chunk,
                    first_sequence=before.records + 1,
                    last_sequence=prefix.records,
                    byte_offset=before.size_bytes,
                    first_tick=self.first_ticks[prefix.name],
                    last_tick=self.last_ticks[prefix.name],
                )
            )
        return ProgressCommit(
            kind="terminal" if simulation.terminal else "checkpoint",
            reference=reference,
            snapshot_digest=self.snapshot.snapshot_digest,
            engine_version=self.snapshot.engine_version,
            next_tick=simulation.tick,
            executed_intervals=simulation.executed_intervals,
            prefixes=prefixes,
            metrics=report,
            chunks=tuple(chunks),
            events=tuple(self.pending),
        )

    def commit(self) -> None:
        """Commit a flushed safe boundary under the current attempt and fencing token."""
        data = self.describe()
        retired = on_loop(
            loop=self.loop, operation=self.repository.commit(lease=self.lease, data=data)
        )
        self.before = {prefix.name: prefix for prefix in data.prefixes}
        self.pending.clear()
        self.first_ticks.clear()
        self.last_ticks.clear()
        if self.pending_checkpoint is not None:
            self.last_checkpoint, self.pending_checkpoint = self.pending_checkpoint, None
        for reference in retired:
            if on_loop(
                loop=self.loop,
                operation=self.repository.removable(lease=self.lease, key=reference.key),
            ):
                self.files.remove(key=reference.key)

    def finalize(self) -> tuple[StoredFile, BundleManifest]:
        """Validate sealed prefixes and publish completion only under current ownership."""
        self.check()
        seal = self.seal
        if seal is None:
            raise OperationError(
                code="INVALID_STATE", message="Finalization requires committed terminal output"
            )
        directory_key = f"{self.prefix}/candidate"
        for prefix in seal.prefixes:
            self.check()
            stream = self.streams[prefix.name]
            stream.seek(0)
            result = self.files.publish(
                key=f"{directory_key}/{prefix.name}",
                chunks=stream_chunks(stream=stream, length=prefix.size_bytes),
                maximum=256 * 1024**2,
            )
            if (result.sha256, result.size_bytes) != (prefix.sha256, prefix.size_bytes):
                raise OperationError(
                    code="VALIDATION_FAILED", message="Finalization changed a sealed prefix"
                )
        with self.files.verified_stream(reference=file_reference(reference=seal.metrics)) as stream:
            self.files.publish(
                key=f"{directory_key}/metrics.json",
                chunks=stream_chunks(stream=stream, length=seal.metrics.size_bytes),
                maximum=256 * 1024**2,
            )
        self.files.publish(
            key=f"{directory_key}/snapshot.json",
            chunks=json_chunks(value=self.snapshot.model_dump(mode="json")),
            maximum=32 * 1024**2,
        )
        self.files.publish(
            key=f"{directory_key}/servers.json",
            chunks=json_chunks(
                value={
                    "tick_seconds": 1,
                    "servers": [server.model_dump() for server in self.snapshot.servers],
                }
            ),
            maximum=256 * 1024,
        )
        self.files.publish(
            key=f"{directory_key}/requests.csv",
            chunks=request_bytes(requests=self.snapshot.requests),
            maximum=10 * 1024**2,
        )
        counts = {prefix.name: prefix.records for prefix in seal.prefixes}
        manifest = validate_artifact_files(
            snapshot=self.snapshot,
            directory=self.files.path(key=directory_key),
            terminal_tick=seal.terminal_tick,
            event_count=counts["run.jsonl"],
            decision_count=counts["autoscaling_decisions.jsonl"],
        )
        self.check()
        reference = self.files.publish(
            key=f"{directory_key}/manifest.json",
            chunks=json_chunks(value=manifest.model_dump(mode="json")),
            maximum=1024**2,
        )
        self.check()
        final_key = f"{self.prefix}/bundles/{manifest.bundle_digest}"
        final = self.files.path(key=final_key)
        final.parent.mkdir(parents=True, exist_ok=True)
        self.files.path(key=directory_key).rename(target=final)
        self.files._sync_directory(directory=final.parent)
        self.files._sync_directory(directory=final.parent.parent)
        return StoredFile(
            key=f"{final_key}/manifest.json",
            sha256=reference.sha256,
            size_bytes=reference.size_bytes,
        ), manifest
