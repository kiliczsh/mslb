"""Exact committed-prefix identity and checkpoint invariant checks."""

import hashlib
from typing import BinaryIO

from mslb_contracts.checkpoints import (
    ArtifactPrefix,
    PrefixName,
)

from mslb_application.errors import OperationError


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise OperationError("CHECKPOINT_INVALID", message)


def _scan_prefix(
    name: PrefixName, stream: BinaryIO, expected: ArtifactPrefix | None = None
) -> ArtifactPrefix:
    """Hash a bounded canonical prefix and retain its verified append position."""
    stream.seek(0)
    digest, size, records, last = hashlib.sha256(), 0, 0, b""
    maximum = expected.size_bytes if expected is not None else 256 * 1024 * 1024
    while size < maximum:
        block = stream.read(min(64 * 1024, maximum - size))
        if not block:
            break
        digest.update(block)
        size += len(block)
        records += block.count(b"\n")
        last = block[-1:]
    if expected is None:
        _require(not stream.read(1), "Checkpoint prefix exceeds its storage limit")
        stream.seek(size)
    _require(not size or last == b"\n", "Checkpoint prefix ends inside a record")
    actual = ArtifactPrefix(name=name, size_bytes=size, records=records, sha256=digest.hexdigest())
    _require(expected is None or actual == expected, "Checkpoint prefix length or digest mismatch")
    return actual
