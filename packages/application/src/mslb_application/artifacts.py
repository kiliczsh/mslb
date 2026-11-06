"""Bounded artifact writes and immutable validated local bundle publication."""

import hashlib
import tempfile
from collections.abc import Callable
from contextlib import ExitStack
from pathlib import Path

from mslb_contracts.inputs import read_requests, read_servers
from mslb_contracts.runs import BundleManifest, RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation
from pydantic import ValidationError

from mslb_application.artifact_validation import ARTIFACT_NAMES as ARTIFACT_NAMES
from mslb_application.artifact_validation import ORIGINAL_VALIDATOR as ORIGINAL_VALIDATOR
from mslb_application.artifact_validation import (
    ORIGINAL_VALIDATOR_DIGEST as ORIGINAL_VALIDATOR_DIGEST,
)
from mslb_application.artifact_validation import _hash_file as _hash_file
from mslb_application.artifact_validation import _original_validation as _original_validation
from mslb_application.artifact_validation import validate_artifact_files as validate_artifact_files
from mslb_application.artifact_writers import BundleLimits as BundleLimits
from mslb_application.artifact_writers import _Artifact as _Artifact
from mslb_application.artifact_writers import _Budget as _Budget
from mslb_application.artifact_writers import _rows as _rows
from mslb_application.artifact_writers import timeline_record as timeline_record
from mslb_application.errors import OperationError
from mslb_application.snapshots import (
    canonical_bytes,
    domain_config,
    domain_inputs,
    read_snapshot,
    verify_snapshot,
)


def _check_cancel(cancelled: Callable[[], bool] | None) -> None:
    if cancelled is not None and cancelled():
        raise OperationError(
            code="INTERRUPTED", message="Run cancelled before artifact publication"
        )


def execute_bundle(
    snapshot: RunSnapshot,
    destination: Path,
    limits: BundleLimits | None = None,
    cancelled: Callable[[], bool] | None = None,
) -> BundleManifest:
    """Publish a new immutable directory only after independent validation."""
    verify_snapshot(snapshot=snapshot)
    _check_cancel(cancelled=cancelled)
    destination = destination.resolve()
    existing: BundleManifest | None = None
    if destination.exists():
        try:
            existing = verify_bundle(directory=destination)
        except (ValueError, OSError):
            raise OperationError(
                code="CONFLICT", message="Destination is not an intact verified bundle"
            ) from None
        if existing.snapshot_digest != snapshot.snapshot_digest:
            raise OperationError(
                code="CONFLICT", message="Destination belongs to different run inputs"
            )
    destination.parent.mkdir(parents=True, exist_ok=True)
    servers, requests = domain_inputs(snapshot=snapshot)
    config = domain_config(settings=snapshot.configuration)
    with tempfile.TemporaryDirectory(prefix=".mslb-attempt-", dir=destination.parent) as attempt:
        directory = Path(attempt)
        budget = _Budget(limits=limits or BundleLimits())
        event_count = decision_count = 0
        simulation = Simulation(servers=servers, requests=requests, config=config)
        metrics = MetricAccumulator(
            requests=requests, servers=servers, max_series=config.max_metric_series
        )
        with ExitStack() as stack:
            outputs = {
                name: _Artifact(path=directory / name, budget=budget, stack=stack)
                for name in sorted(ARTIFACT_NAMES)
            }
            outputs["snapshot.json"].json(value=snapshot.model_dump(mode="json"))
            outputs["servers.json"].json(
                value={
                    "tick_seconds": 1,
                    "servers": [server.model_dump() for server in snapshot.servers],
                }
            )
            rows = (
                (request.t, request.request_id, request.work_units, request.mem_mb)
                for request in requests
            )
            _rows(
                artifact=outputs["requests.csv"], rows=[("t", "request_id", "work_units", "mem_mb")]
            )
            _rows(artifact=outputs["requests.csv"], rows=rows)
            for frame in simulation.iter_ticks():
                _check_cancel(cancelled=cancelled)
                metrics.observe(frame=frame)
                outputs["timeline.jsonl"].json(value=timeline_record(frame=frame))
                for event in frame.events:
                    outputs["run.jsonl"].write(data=event.canonical_line().encode(encoding="utf-8"))
                    event_count += 1
                    if event.reason is not None:
                        outputs["drop_reasons.jsonl"].json(
                            value={
                                "t": event.t,
                                "request_id": event.request_id,
                                "reason": event.reason,
                            }
                        )
                if frame.scale_decision is not None:
                    outputs["autoscaling_decisions.jsonl"].write(
                        data=frame.scale_decision.canonical_line().encode(encoding="utf-8")
                    )
                    decision_count += 1
            outputs["metrics.json"].json(value=metrics.summary())
            for artifact in outputs.values():
                artifact.sync()
            manifest = validate_artifact_files(
                snapshot=snapshot,
                directory=directory,
                terminal_tick=simulation.tick,
                event_count=event_count,
                decision_count=decision_count,
            )
            final = _Artifact(path=directory / "manifest.json", budget=budget, stack=stack)
            final.json(value=manifest.model_dump(mode="json"))
            final.sync()
        _check_cancel(cancelled=cancelled)
        if existing is not None:
            if existing.bundle_digest != manifest.bundle_digest:
                raise OperationError(
                    code="VALIDATION_FAILED",
                    message="Fresh execution differs from existing verified bundle",
                )
            return existing
        if destination.exists():
            raise OperationError(
                code="CONFLICT", message="Bundle destination appeared during execution"
            )
        directory.rename(target=destination)
        return manifest


def verify_bundle(directory: Path) -> BundleManifest:
    """Verify exact file identity, normalized inputs, snapshot, and replay again."""
    directory = directory.resolve()
    manifest_path = directory / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise OperationError(code="VALIDATION_FAILED", message="Manifest is missing or unsafe")
    with manifest_path.open(mode="rb") as stream:
        payload = stream.read(1024 * 1024 + 1)
    if len(payload) > 1024 * 1024:
        raise OperationError(code="VALIDATION_FAILED", message="Manifest exceeds its size limit")
    try:
        manifest = BundleManifest.model_validate_json(json_data=payload)
    except ValidationError:
        raise OperationError(
            code="VALIDATION_FAILED", message="Invalid artifact manifest schema"
        ) from None
    digest = hashlib.sha256(
        data=canonical_bytes(value=manifest.model_dump(mode="json", exclude={"bundle_digest"}))
    ).hexdigest()
    if manifest.bundle_digest != digest or set(manifest.artifacts) != ARTIFACT_NAMES:
        raise OperationError(
            code="VALIDATION_FAILED", message="Artifact manifest identity mismatch"
        )
    if {path.name for path in directory.iterdir()} != ARTIFACT_NAMES | {"manifest.json"}:
        raise OperationError(code="VALIDATION_FAILED", message="Unexpected bundle contents")
    total = len(payload)
    for name, metadata in manifest.artifacts.items():
        path = directory / name
        if path.is_symlink() or not path.is_file() or not path.resolve().is_relative_to(directory):
            raise OperationError(code="VALIDATION_FAILED", message="Unsafe artifact path")
        size, digest = _hash_file(path=path, maximum=metadata.size_bytes)
        total += size
        if size != metadata.size_bytes or digest != metadata.sha256 or total > 512 * 1024 * 1024:
            raise OperationError(
                code="VALIDATION_FAILED", message="Artifact length or digest mismatch"
            )
    with (directory / "snapshot.json").open(mode="rb") as stream:
        snapshot = read_snapshot(stream=stream)
    if (
        manifest.snapshot_digest != snapshot.snapshot_digest
        or manifest.engine_version != snapshot.engine_version
    ):
        raise OperationError(
            code="VALIDATION_FAILED", message="Bundle belongs to a different snapshot"
        )
    with (directory / "servers.json").open(mode="rb") as stream:
        if read_servers(stream=stream) != snapshot.servers:
            raise OperationError(
                code="VALIDATION_FAILED", message="Server artifact differs from snapshot"
            )
    with (directory / "requests.csv").open(mode="rb") as stream:
        if read_requests(stream=stream) != snapshot.requests:
            raise OperationError(
                code="VALIDATION_FAILED", message="Request artifact differs from snapshot"
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
    with (directory / "autoscaling_decisions.jsonl").open(mode="rb") as stream:
        decisions = sum(block.count(b"\n") for block in iter(lambda: stream.read(64 * 1024), b""))
    if (
        replay.terminal_tick != manifest.terminal_tick
        or replay.events != manifest.event_count
        or decisions != manifest.decision_count
    ):
        raise OperationError(
            code="VALIDATION_FAILED", message="Terminal seal does not match replay"
        )
    if (
        _original_validation(directory=directory, snapshot=snapshot) != manifest.original_validator
        or manifest.original_validator_digest != ORIGINAL_VALIDATOR_DIGEST
    ):
        raise OperationError(
            code="VALIDATION_FAILED", message="Original validation provenance mismatch"
        )
    return manifest
