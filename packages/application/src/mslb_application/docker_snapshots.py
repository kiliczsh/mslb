"""Bounded immutable operational snapshot storage and verification."""

import hashlib

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import DockerSnapshot
from pydantic import ValidationError

from mslb_application.errors import OperationError
from mslb_application.managed_files import ManagedFiles
from mslb_application.storage_values import FileReference

MAX_SNAPSHOT_BYTES = 16 * 1024


def save_docker_snapshot(*, files: ManagedFiles, snapshot: DockerSnapshot) -> FileReference:
    # Frozen models can still be constructed with an unchecked model_copy update.
    checked = DockerSnapshot.model_validate(obj=snapshot.model_dump())
    return files.publish(
        key=f"docker/snapshots/{checked.digest()}.json",
        chunks=(canonical_bytes(value=checked.model_dump(mode="json")),),
        maximum=MAX_SNAPSHOT_BYTES,
    )


def load_docker_snapshot(*, files: ManagedFiles, key: str, expected_digest: str) -> DockerSnapshot:
    """Verify bounded canonical operational snapshot bytes and their recorded identity."""
    if key != f"docker/snapshots/{expected_digest}.json":
        raise OperationError(code="VALIDATION_FAILED", message="Docker snapshot namespace differs")
    try:
        with files.path(key=key).open(mode="rb") as stream:
            raw = stream.read(MAX_SNAPSHOT_BYTES + 1)
        if (
            len(raw) > MAX_SNAPSHOT_BYTES
            or hashlib.sha256(string=raw).hexdigest() != expected_digest
        ):
            raise OperationError(code="VALIDATION_FAILED", message="Docker snapshot bytes differ")
        snapshot = DockerSnapshot.model_validate_json(json_data=raw)
        if (
            snapshot.digest() != expected_digest
            or canonical_bytes(value=snapshot.model_dump(mode="json")) != raw
        ):
            raise OperationError(
                code="VALIDATION_FAILED", message="Docker snapshot identity differs"
            )
        return snapshot
    except (OSError, ValidationError):
        raise OperationError(
            code="VALIDATION_FAILED", message="Docker snapshot is unavailable or invalid"
        ) from None
