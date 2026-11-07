"""Bounded artifact writers and exact timeline encoding."""

import csv
import hashlib
import io
import json
import os
from collections.abc import Iterable
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

from mslb_contracts.runs import ArtifactMetadata
from mslb_engine.metrics import exact_value
from mslb_engine.models import TickFrame

from mslb_application.errors import OperationError


@dataclass(frozen=True, slots=True)
class BundleLimits:
    """Bound per-artifact and aggregate output bytes for a logical result bundle."""

    artifact_bytes: int = 256 * 1024 * 1024
    total_bytes: int = 512 * 1024 * 1024

    def __post_init__(self) -> None:
        if (
            type(self.artifact_bytes) is not int
            or not 1 <= self.artifact_bytes <= 256 * 1024 * 1024
            or type(self.total_bytes) is not int
            or not 1 <= self.total_bytes <= 512 * 1024 * 1024
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid artifact storage limits")


class _Budget:
    """Track remaining aggregate artifact bytes shared by all bundle writers."""

    def __init__(self, limits: BundleLimits) -> None:
        self.limits = limits
        self.total = 0


class _Artifact:
    """Stream one bounded artifact while accounting for the shared bundle byte budget."""

    def __init__(self, path: Path, budget: _Budget, stack: ExitStack) -> None:
        self.stream = stack.enter_context(cm=path.open(mode="wb", buffering=256 * 1024))
        self.budget = budget
        self.size = 0
        self.digest = hashlib.sha256()

    def write(self, data: bytes) -> None:
        """Check file/aggregate byte budgets, then write and account for successful bytes."""
        if (
            self.size + len(data) > self.budget.limits.artifact_bytes
            or self.budget.total + len(data) > self.budget.limits.total_bytes
        ):
            raise OperationError(
                code="STORAGE_LIMIT_EXCEEDED", message="Artifact storage budget exceeded"
            )
        if self.stream.write(data) != len(data):
            raise OSError("Short artifact write")
        self.size += len(data)
        self.budget.total += len(data)
        self.digest.update(data)

    def json(self, value: object) -> None:
        """Encode and append one canonical JSON record under the artifact budget."""
        encoder = json.JSONEncoder(
            ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
        for piece in encoder.iterencode(o=value):
            self.write(data=piece.encode(encoding="utf-8"))
        self.write(data=b"\n")

    def sync(self) -> None:
        """Flush buffered output and synchronize the file before publication."""
        self.stream.flush()
        os.fsync(fd=self.stream.fileno())

    def metadata(self) -> ArtifactMetadata:
        """Return the current byte count and SHA-256 digest without flushing the file."""
        return ArtifactMetadata(size_bytes=self.size, sha256=self.digest.hexdigest())


def _rows(artifact: _Artifact, rows: Iterable[Iterable[str | int]]) -> None:
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    for row in rows:
        writer.writerow(row)
        artifact.write(data=buffer.getvalue().encode(encoding="utf-8"))
        buffer.seek(0)
        buffer.truncate()


def timeline_record(frame: TickFrame) -> dict[str, object]:
    """Encode exact interval membership, reservations and resource samples."""
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
                "allocated_cpu": exact_value(value=s.allocated_cpu),
                "consumed_cpu": exact_value(value=s.consumed_cpu),
            }
            for s in frame.samples
        ],
    }
