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

from mslb_application.artifacts import timeline_record, validate_artifact_files
from mslb_application.checkpoints import (
    PREFIX_NAMES,
    CheckpointArtifact,
    capture_terminal_seal,
    checkpoint_due,
    read_checkpoint,
    read_terminal_seal,
    write_checkpoint,
)
from mslb_application.errors import OperationError
from mslb_application.files import FileReference, ManagedFiles, WorkFile, request_bytes
from mslb_application.snapshots import domain_config, domain_inputs


def on_loop[T](loop: asyncio.AbstractEventLoop, operation: Awaitable[T]) -> T:
    """Bridge the execution thread to the independently running async supervisor."""

    async def wait() -> T:
        return await operation

    future = asyncio.run_coroutine_threadsafe(wait(), loop)
    try:
        return future.result(timeout=20)
    except TimeoutError:
        future.cancel()
        raise


def json_chunks(value: object) -> Generator[bytes]:
    encoder = json.JSONEncoder(
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    for piece in encoder.iterencode(value):
        yield piece.encode()
    yield b"\n"


def stored(reference: FileReference) -> StoredFile:
    return StoredFile(key=reference.key, sha256=reference.sha256, size_bytes=reference.size_bytes)


def file_reference(reference: StoredFile) -> FileReference:
    return FileReference(reference.key, reference.sha256, reference.size_bytes)


def stream_chunks(stream: BinaryIO, length: int) -> Generator[bytes]:
    remaining = length
    while remaining:
        block = stream.read(min(64 * 1024, remaining))
        if not block:
            raise OperationError("VALIDATION_FAILED", "Committed stream was truncated")
        remaining -= len(block)
        yield block


class Journal:
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
            name: stack.enter_context(files.work_file(f"{self.prefix}/work/{name}.part"))
            for name in PREFIX_NAMES
        }
        for writer in self.writers.values():
            writer.check = check
        self.streams: dict[str, BinaryIO] = {
            name: writer.stream for name, writer in self.writers.items()
        }
        self.before = {
            name: ArtifactPrefix(
                name=name, size_bytes=0, records=0, sha256=hashlib.sha256(b"").hexdigest()
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
            servers, requests = domain_inputs(snapshot)
            self.simulation = Simulation(servers, requests, domain_config(snapshot.configuration))
            self.metrics = MetricAccumulator(
                requests, servers, snapshot.configuration.max_metric_series
            )
        else:
            self.restore(recovery)

    def restore(self, recovery: RecoveryState) -> None:
        if (
            recovery.snapshot_digest != self.snapshot.snapshot_digest
            or recovery.engine_version != self.snapshot.engine_version
        ):
            raise OperationError("VALIDATION_FAILED", "Recovery input identity mismatch")
        for prefix in recovery.prefixes:
            count, offset, digest = 0, 0, hashlib.sha256()
            after = 0
            while True:
                page = on_loop(
                    self.loop, self.repository.chunks(self.lease.run_id, prefix.name, after)
                )
                if not page:
                    break
                for chunk in page:
                    self.check()
                    if chunk.first_sequence != count + 1 or chunk.byte_offset != offset:
                        raise OperationError(
                            "VALIDATION_FAILED", "Committed chunks are not contiguous"
                        )
                    records = 0
                    with self.files.verified_stream(file_reference(chunk.file)) as stream:
                        while data := stream.read(64 * 1024):
                            self.check()
                            self.writers[prefix.name].write(data)
                            digest.update(data)
                            records += data.count(b"\n")
                    if records != chunk.last_sequence - chunk.first_sequence + 1:
                        raise OperationError("VALIDATION_FAILED", "Chunk sequence count mismatch")
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
                raise OperationError("VALIDATION_FAILED", "Recovered prefix digest mismatch")
            self.before[prefix.name], self.records[prefix.name] = prefix, prefix.records
        with self.files.verified_stream(file_reference(recovery.reference)) as state:
            if recovery.kind == "terminal":
                seal = read_terminal_seal(state, recovery.reference.sha256, self.snapshot)
                if (
                    seal.prefixes != recovery.prefixes
                    or seal.metrics != recovery.metrics
                    or seal.terminal_tick != recovery.next_tick
                    or seal.executed_intervals != recovery.executed_intervals
                ):
                    raise OperationError(
                        "VALIDATION_FAILED", "Terminal seal differs from committed metadata"
                    )
                self.seal = seal
            else:
                restored = read_checkpoint(
                    state, recovery.reference.sha256, self.snapshot, self.streams
                )
                if (
                    restored.simulation.tick != recovery.next_tick
                    or restored.prefixes != recovery.prefixes
                ):
                    raise OperationError(
                        "VALIDATION_FAILED", "Checkpoint differs from committed progress"
                    )
                self.simulation, self.metrics = restored.simulation, restored.metrics
                self.last_checkpoint = CheckpointArtifact(
                    self.files.path(recovery.reference.key),
                    recovery.reference.sha256,
                    recovery.reference.size_bytes,
                    recovery.next_tick,
                    recovery.executed_intervals,
                    self.records["run.jsonl"],
                    recovery.prefixes,
                )

    def _record(self, name: PrefixName, pieces: Iterable[bytes], tick: int) -> None:
        self.first_ticks.setdefault(name, tick)
        self.last_ticks[name] = tick
        for piece in pieces:
            self.writers[name].write(piece)
        self.records[name] += 1

    def append(self, frame: TickFrame) -> None:
        if self.metrics is None:
            raise OperationError("INVALID_STATE", "Sealed attempts cannot execute again")
        self.metrics.observe(frame)
        self._record("timeline.jsonl", json_chunks(timeline_record(frame)), frame.t)
        for event in frame.events:
            self._record("run.jsonl", (event.canonical_line().encode(),), frame.t)
            self.pending.append(
                PersistedEvent.model_validate_json(
                    canonical_bytes(
                        dict(
                            sequence=self.records["run.jsonl"],
                            t=event.t,
                            event=event.event,
                            request_id=event.request_id,
                            server_id=event.server_id,
                            reason=event.reason,
                        )
                    )
                )
            )
            if event.reason is not None:
                self._record(
                    "drop_reasons.jsonl",
                    json_chunks(dict(t=event.t, request_id=event.request_id, reason=event.reason)),
                    frame.t,
                )
        if frame.scale_decision is not None:
            self._record(
                "autoscaling_decisions.jsonl",
                (frame.scale_decision.canonical_line().encode(),),
                frame.t,
            )

    def checkpoint_needed(self) -> bool:
        return self.simulation is not None and (
            checkpoint_due(self.simulation, self.last_checkpoint)
            or any(
                writer.position - self.before[name].size_bytes >= 256 * 1024
                for name, writer in self.writers.items()
            )
        )

    def describe(self) -> ProgressCommit:
        self.check()
        simulation, metrics = self.simulation, self.metrics
        if simulation is None or metrics is None:
            raise OperationError("INVALID_STATE", "Cannot write progress for sealed execution")
        for writer in self.writers.values():
            writer.sync()
        report = None
        if simulation.terminal:
            report = stored(
                self.files.publish(
                    f"{self.prefix}/metrics.json", json_chunks(metrics.summary()), 256 * 1024**2
                )
            )
            seal = capture_terminal_seal(self.snapshot, simulation, metrics, self.streams, report)
            reference = stored(
                self.files.publish(
                    f"{self.prefix}/terminal.json",
                    json_chunks(seal.model_dump(mode="json")),
                    1024**2,
                )
            )
            prefixes = seal.prefixes
            self.seal = seal
        else:
            with tempfile.TemporaryDirectory(prefix="mslb-checkpoint-") as temporary:
                checkpoint = write_checkpoint(
                    Path(temporary), self.snapshot, simulation, metrics, self.streams
                )
                with checkpoint.path.open("rb") as source:
                    reference = stored(
                        self.files.publish(
                            f"{self.prefix}/checkpoints/{checkpoint.sha256}.json",
                            stream_chunks(source, checkpoint.size_bytes),
                            32 * 1024**2,
                        )
                    )
                prefixes = checkpoint.prefixes
                self.pending_checkpoint = replace(checkpoint, path=self.files.path(reference.key))
        chunks: list[OutputChunk] = []
        for prefix in prefixes:
            before = self.before[prefix.name]
            if before.records == prefix.records:
                continue
            self.check()
            stream = self.streams[prefix.name]
            stream.seek(before.size_bytes)
            reference_chunk = stored(
                self.files.publish(
                    f"{self.prefix}/chunks/{prefix.name}-{before.records + 1:07d}.jsonl",
                    stream_chunks(stream, prefix.size_bytes - before.size_bytes),
                    32 * 1024**2,
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
        data = self.describe()
        retired = on_loop(self.loop, self.repository.commit(self.lease, data))
        self.before = {prefix.name: prefix for prefix in data.prefixes}
        self.pending.clear()
        self.first_ticks.clear()
        self.last_ticks.clear()
        if self.pending_checkpoint is not None:
            self.last_checkpoint, self.pending_checkpoint = self.pending_checkpoint, None
        for reference in retired:
            if on_loop(self.loop, self.repository.removable(self.lease, reference.key)):
                self.files.remove(reference.key)

    def finalize(self) -> tuple[StoredFile, BundleManifest]:
        self.check()
        seal = self.seal
        if seal is None:
            raise OperationError("INVALID_STATE", "Finalization requires committed terminal output")
        directory_key = f"{self.prefix}/candidate"
        for prefix in seal.prefixes:
            self.check()
            stream = self.streams[prefix.name]
            stream.seek(0)
            result = self.files.publish(
                f"{directory_key}/{prefix.name}",
                stream_chunks(stream, prefix.size_bytes),
                256 * 1024**2,
            )
            if (result.sha256, result.size_bytes) != (prefix.sha256, prefix.size_bytes):
                raise OperationError("VALIDATION_FAILED", "Finalization changed a sealed prefix")
        with self.files.verified_stream(file_reference(seal.metrics)) as stream:
            self.files.publish(
                f"{directory_key}/metrics.json",
                stream_chunks(stream, seal.metrics.size_bytes),
                256 * 1024**2,
            )
        self.files.publish(
            f"{directory_key}/snapshot.json",
            json_chunks(self.snapshot.model_dump(mode="json")),
            32 * 1024**2,
        )
        self.files.publish(
            f"{directory_key}/servers.json",
            json_chunks(
                dict(
                    tick_seconds=1,
                    servers=[server.model_dump() for server in self.snapshot.servers],
                )
            ),
            256 * 1024,
        )
        self.files.publish(
            f"{directory_key}/requests.csv", request_bytes(self.snapshot.requests), 10 * 1024**2
        )
        counts = {prefix.name: prefix.records for prefix in seal.prefixes}
        manifest = validate_artifact_files(
            self.snapshot,
            self.files.path(directory_key),
            seal.terminal_tick,
            counts["run.jsonl"],
            counts["autoscaling_decisions.jsonl"],
        )
        self.check()
        reference = self.files.publish(
            f"{directory_key}/manifest.json", json_chunks(manifest.model_dump(mode="json")), 1024**2
        )
        self.check()
        final_key = f"{self.prefix}/bundles/{manifest.bundle_digest}"
        final = self.files.path(final_key)
        final.parent.mkdir(parents=True, exist_ok=True)
        self.files.path(directory_key).rename(final)
        self.files._sync_directory(final.parent)
        self.files._sync_directory(final.parent.parent)
        return StoredFile(
            key=f"{final_key}/manifest.json",
            sha256=reference.sha256,
            size_bytes=reference.size_bytes,
        ), manifest
