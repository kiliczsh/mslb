"""Bounded artifact writes and immutable validated local bundle publication."""

import csv
import hashlib
import io
import json
import os
import subprocess
import sys
import tempfile
from collections.abc import Callable, Iterable
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from mslb_contracts.inputs import read_requests, read_servers
from mslb_contracts.runs import ArtifactMetadata, BundleManifest, RunSnapshot
from mslb_engine.metrics import MetricAccumulator, exact_value
from mslb_engine.models import TickFrame
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation
from pydantic import ValidationError

from mslb_application.errors import OperationError
from mslb_application.snapshots import (
    canonical_bytes,
    domain_config,
    domain_inputs,
    read_snapshot,
    verify_snapshot,
)

ORIGINAL_VALIDATOR = Path("scripts/validate_run.py").resolve()
ORIGINAL_VALIDATOR_DIGEST = "a1476898f041bdf5823d2c4158717d173369269914001d3b129a33c3b2d2ab34"
ARTIFACT_NAMES = frozenset(
    {
        "run.jsonl",
        "autoscaling_decisions.jsonl",
        "timeline.jsonl",
        "drop_reasons.jsonl",
        "metrics.json",
        "snapshot.json",
        "servers.json",
        "requests.csv",
    }
)


@dataclass(frozen=True, slots=True)
class BundleLimits:
    artifact_bytes: int = 256 * 1024 * 1024
    total_bytes: int = 512 * 1024 * 1024

    def __post_init__(self) -> None:
        if (
            type(self.artifact_bytes) is not int
            or not 1 <= self.artifact_bytes <= 256 * 1024 * 1024
            or type(self.total_bytes) is not int
            or not 1 <= self.total_bytes <= 512 * 1024 * 1024
        ):
            raise OperationError("VALIDATION_ERROR", "Invalid artifact storage limits")


class _Budget:
    def __init__(self, limits: BundleLimits) -> None:
        self.limits = limits
        self.total = 0


class _Artifact:
    def __init__(self, path: Path, budget: _Budget, stack: ExitStack) -> None:
        self.stream = stack.enter_context(path.open("wb", buffering=256 * 1024))
        self.budget = budget
        self.size = 0
        self.digest = hashlib.sha256()

    def write(self, data: bytes) -> None:
        if (
            self.size + len(data) > self.budget.limits.artifact_bytes
            or self.budget.total + len(data) > self.budget.limits.total_bytes
        ):
            raise OperationError("STORAGE_LIMIT_EXCEEDED", "Artifact storage budget exceeded")
        if self.stream.write(data) != len(data):
            raise OSError("Short artifact write")
        self.size += len(data)
        self.budget.total += len(data)
        self.digest.update(data)

    def json(self, value: object) -> None:
        encoder = json.JSONEncoder(
            ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        for piece in encoder.iterencode(value):
            self.write(piece.encode("utf-8"))
        self.write(b"\n")

    def sync(self) -> None:
        self.stream.flush()
        os.fsync(self.stream.fileno())

    def metadata(self) -> ArtifactMetadata:
        return ArtifactMetadata(size_bytes=self.size, sha256=self.digest.hexdigest())


def _rows(artifact: _Artifact, rows: Iterable[Iterable[str | int]]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow(row)
        artifact.write(buffer.getvalue().encode("utf-8"))
        buffer.seek(0)
        buffer.truncate()


def timeline_record(frame: TickFrame) -> dict[str, object]:
    return {
        "t": frame.t,
        "duration_ticks": frame.duration_ticks,
        "terminal": frame.terminal,
        "pending_ids": frame.pending_ids,
        "membership": [
            {
                "id": s.id,
                "cpu_units_per_tick": s.cpu_units_per_tick,
                "mem_mb": s.mem_mb,
                "rate_limit_per_sec": s.rate_limit_per_sec,
            }
            for s in frame.membership
        ],
        "servers": [
            {
                "server_id": s.server_id,
                "active_ids": s.active_ids,
                "reserved_mem_mb": s.reserved_mem_mb,
                "capacity": s.capacity,
                "memory_capacity": s.memory_capacity,
                "starts": s.starts,
                "allocated_cpu": exact_value(s.allocated_cpu),
                "consumed_cpu": exact_value(s.consumed_cpu),
            }
            for s in frame.samples
        ],
    }


def _hash_file(path: Path, maximum: int) -> tuple[int, str]:
    digest, size = hashlib.sha256(), 0
    with path.open("rb") as stream:
        while block := stream.read(64 * 1024):
            size += len(block)
            if size > maximum:
                raise OperationError(
                    "STORAGE_LIMIT_EXCEEDED", "Artifact exceeds its recorded length"
                )
            digest.update(block)
    return size, digest.hexdigest()


def _original_validation(
    directory: Path, snapshot: RunSnapshot
) -> Literal["passed", "not_applicable"]:
    if (
        snapshot.configuration.mode != "sequential_compat"
        or snapshot.configuration.autoscaling.enabled
    ):
        return "not_applicable"
    if _hash_file(ORIGINAL_VALIDATOR, 256 * 1024)[1] != ORIGINAL_VALIDATOR_DIGEST:
        raise OperationError("VALIDATION_FAILED", "Original evaluator identity mismatch")
    with tempfile.TemporaryFile() as diagnostics:
        try:
            result = subprocess.run(  # noqa: S603 -- fixed interpreter, verified evaluator, managed input paths
                [
                    sys.executable,
                    str(ORIGINAL_VALIDATOR),
                    "--servers",
                    str(directory / "servers.json"),
                    "--requests",
                    str(directory / "requests.csv"),
                    "--run",
                    str(directory / "run.jsonl"),
                ],
                stdout=diagnostics,
                stderr=subprocess.STDOUT,
                timeout=60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise OperationError("VALIDATION_FAILED", "Original evaluator timed out") from None
        size = diagnostics.seek(0, os.SEEK_END)
    if result.returncode or size > 1024 * 1024:
        raise OperationError("VALIDATION_FAILED", "Original evaluator rejected the trace")
    return "passed"


def _check_cancel(cancelled: Callable[[], bool] | None) -> None:
    if cancelled is not None and cancelled():
        raise OperationError("INTERRUPTED", "Run cancelled before artifact publication")


def validate_artifact_files(
    snapshot: RunSnapshot,
    directory: Path,
    terminal_tick: int,
    event_count: int,
    decision_count: int,
) -> BundleManifest:
    """Validate assembled files without executing the simulation a second time."""
    verify_snapshot(snapshot)
    if {path.name for path in directory.iterdir()} != ARTIFACT_NAMES:
        raise OperationError("VALIDATION_FAILED", "Assembled artifact set is incomplete")
    metadata: dict[str, ArtifactMetadata] = {}
    for name in sorted(ARTIFACT_NAMES):
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise OperationError("VALIDATION_FAILED", "Unsafe assembled artifact")
        size, digest = _hash_file(path, 256 * 1024**2)
        metadata[name] = ArtifactMetadata(size_bytes=size, sha256=digest)
    with (directory / "snapshot.json").open("rb") as stream:
        if read_snapshot(stream) != snapshot:
            raise OperationError("VALIDATION_FAILED", "Assembled snapshot differs")
    with (directory / "servers.json").open("rb") as stream:
        if read_servers(stream) != snapshot.servers:
            raise OperationError("VALIDATION_FAILED", "Assembled server inputs differ")
    with (directory / "requests.csv").open("rb") as stream:
        if read_requests(stream) != snapshot.requests:
            raise OperationError("VALIDATION_FAILED", "Assembled request inputs differ")
    servers, requests = domain_inputs(snapshot)
    with (
        (directory / "run.jsonl").open("rb") as trace,
        (directory / "autoscaling_decisions.jsonl").open("rb") as sidecar,
    ):
        replay = validate_trace(
            trace, servers, requests, domain_config(snapshot.configuration), sidecar
        )
        sidecar.seek(0)
        decisions = sum(block.count(b"\n") for block in iter(lambda: sidecar.read(64 * 1024), b""))
    if (
        replay.terminal_tick != terminal_tick
        or replay.events != event_count
        or decisions != decision_count
    ):
        raise OperationError(
            "VALIDATION_FAILED", "Assembled output differs from terminal accounting"
        )
    original = _original_validation(directory, snapshot)
    manifest = BundleManifest(
        engine_version=snapshot.engine_version,
        snapshot_digest=snapshot.snapshot_digest,
        bundle_digest="0" * 64,
        terminal_tick=terminal_tick,
        event_count=event_count,
        decision_count=decision_count,
        independent_replay="passed",
        original_validator=original,
        original_validator_digest=ORIGINAL_VALIDATOR_DIGEST,
        artifacts=metadata,
    )
    digest = hashlib.sha256(
        canonical_bytes(manifest.model_dump(mode="json", exclude={"bundle_digest"}))
    ).hexdigest()
    result = manifest.model_copy(update={"bundle_digest": digest})
    if (
        sum(item.size_bytes for item in metadata.values())
        + len(canonical_bytes(result.model_dump(mode="json")))
        > 512 * 1024**2
    ):
        raise OperationError("STORAGE_LIMIT_EXCEEDED", "Assembled bundle exceeds its byte budget")
    return result


def execute_bundle(
    snapshot: RunSnapshot,
    destination: Path,
    limits: BundleLimits | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> BundleManifest:
    """Publish a new immutable directory only after independent validation."""
    verify_snapshot(snapshot)
    _check_cancel(cancelled)
    destination = destination.resolve()
    existing: BundleManifest | None = None
    if destination.exists():
        try:
            existing = verify_bundle(destination)
        except (ValueError, OSError):
            raise OperationError(
                "CONFLICT", "Destination is not an intact verified bundle"
            ) from None
        if existing.snapshot_digest != snapshot.snapshot_digest:
            raise OperationError("CONFLICT", "Destination belongs to different run inputs")
    destination.parent.mkdir(parents=True, exist_ok=True)
    servers, requests = domain_inputs(snapshot)
    config = domain_config(snapshot.configuration)
    with tempfile.TemporaryDirectory(prefix=".mslb-attempt-", dir=destination.parent) as attempt:
        directory = Path(attempt)
        budget = _Budget(limits or BundleLimits())
        event_count = decision_count = 0
        simulation = Simulation(servers, requests, config)
        metrics = MetricAccumulator(requests, servers, config.max_metric_series)
        with ExitStack() as stack:
            outputs = {
                name: _Artifact(directory / name, budget, stack) for name in sorted(ARTIFACT_NAMES)
            }
            outputs["snapshot.json"].json(snapshot.model_dump(mode="json"))
            outputs["servers.json"].json(
                {"tick_seconds": 1, "servers": [server.model_dump() for server in snapshot.servers]}
            )
            rows = (
                (request.t, request.request_id, request.work_units, request.mem_mb)
                for request in requests
            )
            _rows(outputs["requests.csv"], [("t", "request_id", "work_units", "mem_mb")])
            _rows(outputs["requests.csv"], rows)
            for frame in simulation.iter_ticks():
                _check_cancel(cancelled)
                metrics.observe(frame)
                outputs["timeline.jsonl"].json(timeline_record(frame))
                for event in frame.events:
                    outputs["run.jsonl"].write(event.canonical_line().encode("utf-8"))
                    event_count += 1
                    if event.reason is not None:
                        outputs["drop_reasons.jsonl"].json(
                            {"t": event.t, "request_id": event.request_id, "reason": event.reason}
                        )
                if frame.scale_decision is not None:
                    outputs["autoscaling_decisions.jsonl"].write(
                        frame.scale_decision.canonical_line().encode("utf-8")
                    )
                    decision_count += 1
            outputs["metrics.json"].json(metrics.summary())
            for artifact in outputs.values():
                artifact.sync()
            manifest = validate_artifact_files(
                snapshot, directory, simulation.tick, event_count, decision_count
            )
            final = _Artifact(directory / "manifest.json", budget, stack)
            final.json(manifest.model_dump(mode="json"))
            final.sync()
        _check_cancel(cancelled)
        if existing is not None:
            if existing.bundle_digest != manifest.bundle_digest:
                raise OperationError(
                    "VALIDATION_FAILED", "Fresh execution differs from existing verified bundle"
                )
            return existing
        if destination.exists():
            raise OperationError("CONFLICT", "Bundle destination appeared during execution")
        directory.rename(destination)
        return manifest


def verify_bundle(directory: Path) -> BundleManifest:
    """Verify exact file identity, normalized inputs, snapshot, and replay again."""
    directory = directory.resolve()
    manifest_path = directory / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise OperationError("VALIDATION_FAILED", "Manifest is missing or unsafe")
    with manifest_path.open("rb") as stream:
        payload = stream.read(1024 * 1024 + 1)
    if len(payload) > 1024 * 1024:
        raise OperationError("VALIDATION_FAILED", "Manifest exceeds its size limit")
    try:
        manifest = BundleManifest.model_validate_json(payload)
    except ValidationError:
        raise OperationError("VALIDATION_FAILED", "Invalid artifact manifest schema") from None
    digest = hashlib.sha256(
        canonical_bytes(manifest.model_dump(mode="json", exclude={"bundle_digest"}))
    ).hexdigest()
    if manifest.bundle_digest != digest or set(manifest.artifacts) != ARTIFACT_NAMES:
        raise OperationError("VALIDATION_FAILED", "Artifact manifest identity mismatch")
    if {path.name for path in directory.iterdir()} != ARTIFACT_NAMES | {"manifest.json"}:
        raise OperationError("VALIDATION_FAILED", "Unexpected bundle contents")
    total = len(payload)
    for name, metadata in manifest.artifacts.items():
        path = directory / name
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(directory):
            raise OperationError("VALIDATION_FAILED", "Unsafe artifact path")
        size, digest = _hash_file(path, metadata.size_bytes)
        total += size
        if size != metadata.size_bytes or digest != metadata.sha256 or total > 512 * 1024 * 1024:
            raise OperationError("VALIDATION_FAILED", "Artifact length or digest mismatch")
    with (directory / "snapshot.json").open("rb") as stream:
        snapshot = read_snapshot(stream)
    if (
        manifest.snapshot_digest != snapshot.snapshot_digest
        or manifest.engine_version != snapshot.engine_version
    ):
        raise OperationError("VALIDATION_FAILED", "Bundle belongs to a different snapshot")
    with (directory / "servers.json").open("rb") as stream:
        if read_servers(stream) != snapshot.servers:
            raise OperationError("VALIDATION_FAILED", "Server artifact differs from snapshot")
    with (directory / "requests.csv").open("rb") as stream:
        if read_requests(stream) != snapshot.requests:
            raise OperationError("VALIDATION_FAILED", "Request artifact differs from snapshot")
    servers, requests = domain_inputs(snapshot)
    with (
        (directory / "run.jsonl").open("rb") as trace,
        (directory / "autoscaling_decisions.jsonl").open("rb") as sidecar,
    ):
        replay = validate_trace(
            trace, servers, requests, domain_config(snapshot.configuration), sidecar
        )
    with (directory / "autoscaling_decisions.jsonl").open("rb") as stream:
        decisions = sum(block.count(b"\n") for block in iter(lambda: stream.read(64 * 1024), b""))
    if (
        replay.terminal_tick != manifest.terminal_tick
        or replay.events != manifest.event_count
        or decisions != manifest.decision_count
    ):
        raise OperationError("VALIDATION_FAILED", "Terminal seal does not match replay")
    if (
        _original_validation(directory, snapshot) != manifest.original_validator
        or manifest.original_validator_digest != ORIGINAL_VALIDATOR_DIGEST
    ):
        raise OperationError("VALIDATION_FAILED", "Original validation provenance mismatch")
    return manifest
