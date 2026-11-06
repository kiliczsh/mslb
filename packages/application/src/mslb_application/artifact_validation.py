"""Independent artifact replay and supplied-evaluator provenance."""

import hashlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Literal

from mslb_contracts.inputs import read_requests, read_servers
from mslb_contracts.runs import ArtifactMetadata, BundleManifest, RunSnapshot
from mslb_engine.replay import validate_trace

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


def _hash_file(path: Path, maximum: int) -> tuple[int, str]:
    digest, size = hashlib.sha256(), 0
    with path.open(mode="rb") as stream:
        while block := stream.read(64 * 1024):
            size += len(block)
            if size > maximum:
                raise OperationError(
                    code="STORAGE_LIMIT_EXCEEDED", message="Artifact exceeds its recorded length"
                )
            digest.update(block)
    return size, digest.hexdigest()


def _original_validation(
    directory: Path, snapshot: RunSnapshot
) -> Literal["passed", "not_applicable"]:
    """Verify evaluator identity and validate only applicable static sequential traces."""
    if snapshot.configuration.mode == "concurrent" or snapshot.configuration.autoscaling.enabled:
        return "not_applicable"
    if _hash_file(path=ORIGINAL_VALIDATOR, maximum=256 * 1024)[1] != ORIGINAL_VALIDATOR_DIGEST:
        raise OperationError(
            code="VALIDATION_FAILED", message="Original evaluator identity mismatch"
        )
    with tempfile.TemporaryFile() as diagnostics:
        try:
            result = subprocess.run(  # noqa: S603 -- fixed interpreter, verified evaluator, managed input paths
                args=[
                    sys.executable,
                    str(object=ORIGINAL_VALIDATOR),
                    "--servers",
                    str(object=directory / "servers.json"),
                    "--requests",
                    str(object=directory / "requests.csv"),
                    "--run",
                    str(object=directory / "run.jsonl"),
                ],
                stdout=diagnostics,
                stderr=subprocess.STDOUT,
                timeout=60,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise OperationError(
                code="VALIDATION_FAILED", message="Original evaluator timed out"
            ) from None
        size = diagnostics.seek(0, os.SEEK_END)
    if result.returncode or size > 1024 * 1024:
        raise OperationError(
            code="VALIDATION_FAILED", message="Original evaluator rejected the trace"
        )
    return "passed"


def validate_artifact_files(
    snapshot: RunSnapshot,
    directory: Path,
    terminal_tick: int,
    event_count: int,
    decision_count: int,
) -> BundleManifest:
    """Validate assembled files without executing the simulation a second time."""
    verify_snapshot(snapshot=snapshot)
    if {path.name for path in directory.iterdir()} != ARTIFACT_NAMES:
        raise OperationError(
            code="VALIDATION_FAILED", message="Assembled artifact set is incomplete"
        )
    metadata: dict[str, ArtifactMetadata] = {}
    for name in sorted(ARTIFACT_NAMES):
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise OperationError(code="VALIDATION_FAILED", message="Unsafe assembled artifact")
        size, digest = _hash_file(path=path, maximum=256 * 1024**2)
        metadata[name] = ArtifactMetadata(size_bytes=size, sha256=digest)
    with (directory / "snapshot.json").open(mode="rb") as stream:
        if read_snapshot(stream=stream) != snapshot:
            raise OperationError(code="VALIDATION_FAILED", message="Assembled snapshot differs")
    with (directory / "servers.json").open(mode="rb") as stream:
        if read_servers(stream=stream) != snapshot.servers:
            raise OperationError(code="VALIDATION_FAILED", message="Assembled server inputs differ")
    with (directory / "requests.csv").open(mode="rb") as stream:
        if read_requests(stream=stream) != snapshot.requests:
            raise OperationError(
                code="VALIDATION_FAILED", message="Assembled request inputs differ"
            )
    servers, requests = domain_inputs(snapshot=snapshot)
    with (
        (directory / "run.jsonl").open(mode="rb") as trace,
        (directory / "autoscaling_decisions.jsonl").open(mode="rb") as sidecar,
    ):
        replay = validate_trace(
            stream=trace,
            servers=servers,
            requests=requests,
            config=domain_config(settings=snapshot.configuration),
            scale_stream=sidecar,
        )
        sidecar.seek(0)
        decisions = sum(block.count(b"\n") for block in iter(lambda: sidecar.read(64 * 1024), b""))
    if (
        replay.terminal_tick != terminal_tick
        or replay.events != event_count
        or decisions != decision_count
    ):
        raise OperationError(
            code="VALIDATION_FAILED", message="Assembled output differs from terminal accounting"
        )
    original = _original_validation(directory=directory, snapshot=snapshot)
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
        data=canonical_bytes(value=manifest.model_dump(mode="json", exclude={"bundle_digest"}))
    ).hexdigest()
    result = manifest.model_copy(update={"bundle_digest": digest})
    if (
        sum(item.size_bytes for item in metadata.values())
        + len(canonical_bytes(value=result.model_dump(mode="json")))
        > 512 * 1024**2
    ):
        raise OperationError(
            code="STORAGE_LIMIT_EXCEEDED", message="Assembled bundle exceeds its byte budget"
        )
    return result
