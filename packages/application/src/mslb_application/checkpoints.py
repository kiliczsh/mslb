"""Bounded immutable checkpoint files and verified completed-interval restore."""

import hashlib
import json
import os
import re
import sys
import tempfile
from collections.abc import Mapping
from itertools import chain
from pathlib import Path
from typing import BinaryIO

from mslb_contracts.checkpoints import (
    IntervalCheckpoint,
)
from mslb_contracts.runs import RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.simulator import Simulation
from pydantic import ValidationError

from mslb_application.checkpoint_prefixes import _require as _require
from mslb_application.checkpoint_prefixes import _scan_prefix as _scan_prefix
from mslb_application.checkpoint_seals import capture_terminal_seal as capture_terminal_seal
from mslb_application.checkpoint_seals import read_terminal_seal as read_terminal_seal
from mslb_application.checkpoint_state import _capture as _capture
from mslb_application.checkpoint_state import _fraction as _fraction
from mslb_application.checkpoint_state import _preflight as _preflight
from mslb_application.checkpoint_state import _restore as _restore
from mslb_application.checkpoint_state import _restore_scaler as _restore_scaler
from mslb_application.checkpoint_state import _server_input as _server_input
from mslb_application.checkpoint_values import MAX_CHECKPOINT_BYTES as MAX_CHECKPOINT_BYTES
from mslb_application.checkpoint_values import PREFIX_NAMES as PREFIX_NAMES
from mslb_application.checkpoint_values import CheckpointArtifact as CheckpointArtifact
from mslb_application.checkpoint_values import RestoredCheckpoint as RestoredCheckpoint
from mslb_application.errors import OperationError
from mslb_application.snapshots import domain_config, domain_inputs, verify_snapshot


def checkpoint_due(simulation: Simulation, previous: CheckpointArtifact | None = None) -> bool:
    """Use executed intervals/events, never fast-forwarded wall-clock ticks."""
    intervals = previous.executed_intervals if previous is not None else 0
    events = previous.event_count if previous is not None else 0
    return simulation.checkpoint_ready and (
        simulation.executed_intervals - intervals >= 100 or simulation.event_count - events >= 1000
    )


def write_checkpoint(
    directory: Path,
    snapshot: RunSnapshot,
    simulation: Simulation,
    metrics: MetricAccumulator,
    prefixes: Mapping[str, BinaryIO],
    max_bytes: int = MAX_CHECKPOINT_BYTES,
) -> CheckpointArtifact:
    """Fsync a content-addressed file; callers still must commit its DB reference."""
    if sys.platform != "linux":
        raise RuntimeError("Durable checkpoint storage requires Linux containers")
    verify_snapshot(snapshot)
    _preflight(simulation, metrics, max_bytes)
    servers, requests = domain_inputs(snapshot)
    _require(
        simulation.initial_servers == servers
        and simulation.requests == requests
        and simulation.config == domain_config(snapshot.configuration)
        and metrics.requests == simulation.by_id
        and metrics.max_series == simulation.config.max_metric_series,
        "Execution differs from its immutable snapshot",
    )
    state = _capture(snapshot, simulation, metrics, prefixes)
    _restore(snapshot, state)
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory, prefix=".checkpoint-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            digest, size = hashlib.sha256(), 0
            encoder = json.JSONEncoder(
                ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            for piece in chain(encoder.iterencode(state.model_dump(mode="json")), ("\n",)):
                data = piece.encode("utf-8")
                size += len(data)
                if size > max_bytes:
                    raise OperationError(
                        "RESOURCE_LIMIT_EXCEEDED", "Checkpoint bytes exceed the budget"
                    )
                if stream.write(data) != len(data):
                    raise OSError("Short checkpoint write")
                digest.update(data)
            stream.flush()
            os.fsync(stream.fileno())
            checksum = digest.hexdigest()
            path = directory / f"checkpoint-{checksum}.json"
            try:
                os.link(temporary, path)
            except FileExistsError:
                _require(not path.is_symlink() and path.is_file(), "Unsafe existing checkpoint")
                with path.open("rb") as existing:
                    _require(
                        hashlib.sha256(existing.read(max_bytes + 1)).hexdigest() == checksum,
                        "Existing checkpoint identity mismatch",
                    )
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return CheckpointArtifact(
                path,
                checksum,
                size,
                state.next_tick,
                state.executed_intervals,
                state.event_count,
                state.prefixes,
            )
        finally:
            stream.close()
            temporary.unlink(missing_ok=True)


def read_checkpoint(
    stream: BinaryIO,
    expected_digest: str,
    snapshot: RunSnapshot,
    prefixes: Mapping[str, BinaryIO],
) -> RestoredCheckpoint:
    """Verify an externally recorded digest and exact prefixes before restore.

    Prefix streams may contain an uncommitted suffix; only the sealed byte range
    is read. On return each stream is positioned at its verified append offset.
    """
    verify_snapshot(snapshot)
    _require(bool(re.fullmatch(r"[0-9a-f]{64}", expected_digest)), "Invalid checkpoint digest")
    payload = stream.read(MAX_CHECKPOINT_BYTES + 1)
    _require(len(payload) <= MAX_CHECKPOINT_BYTES, "Checkpoint payload too large")
    _require(hashlib.sha256(payload).hexdigest() == expected_digest, "Checkpoint digest mismatch")
    try:
        state = IntervalCheckpoint.model_validate_json(payload)
    except ValidationError:
        raise OperationError("CHECKPOINT_INVALID", "Invalid checkpoint schema") from None
    _require(set(prefixes) == set(PREFIX_NAMES), "All four checkpoint prefixes are required")
    restored = _restore(snapshot, state)
    for prefix in state.prefixes:
        _scan_prefix(prefix.name, prefixes[prefix.name], prefix)
    return restored
