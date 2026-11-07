"""A bounded stdin-only Windows evidence bridge; no host bind, network or daemon API."""

import os
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import BinaryIO

from mslb_contracts.resources import HostMemoryEvidence
from pydantic import ValidationError

DIRECTORY = Path("/run/host-memory")
FRAME_BYTES = 512


class OutdatedHostFrame(ValueError):
    """Reject queued, future or replayed counters without replacing current evidence."""


def read_host_memory() -> HostMemoryEvidence | None:
    """Read only a root-owned regular frame from the guard's read-only volume."""
    if sys.platform != "linux":
        return None
    try:
        descriptor = os.open(
            path=DIRECTORY / "memory.json", flags=os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
        )
        with os.fdopen(fd=descriptor, mode="rb") as stream:
            info = os.fstat(fd=stream.fileno())
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != 0
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != 0o644
            ):
                return None
            raw = stream.read(FRAME_BYTES + 1)
        if len(raw) > FRAME_BYTES:
            return None
        return HostMemoryEvidence.model_validate_json(json_data=raw)
    except (OSError, ValidationError):
        return None


def write_frame(*, raw: bytes, directory: Path, previous: datetime | None) -> datetime:
    """Validate before atomically replacing the sole numeric evidence frame."""
    if sys.platform != "linux":
        raise RuntimeError("Host evidence volume writes require Linux containers")
    if len(raw) > FRAME_BYTES or not raw.endswith(b"\n"):
        raise ValueError("Host memory evidence exceeds its fixed frame")
    frame = HostMemoryEvidence.model_validate_json(json_data=raw)
    if not 0 <= (datetime.now(tz=UTC) - frame.measured_at).total_seconds() <= 3 or (
        previous is not None and frame.measured_at <= previous
    ):
        raise OutdatedHostFrame("Host memory evidence is stale or repeated")
    temporary = directory / "memory.tmp"
    descriptor = os.open(
        path=temporary,
        flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
        mode=0o644,
    )
    with os.fdopen(fd=descriptor, mode="wb") as stream:
        os.fchmod(fd=stream.fileno(), mode=0o644)
        stream.write(frame.model_dump_json().encode(encoding="ascii"))
        stream.flush()
        os.fsync(fd=stream.fileno())
    temporary.replace(target=directory / "memory.json")
    return frame.measured_at


def consume(*, stream: BinaryIO, directory: Path) -> None:
    """Keep at most one bounded line in memory; EOF leaves evidence to expire."""
    previous = None
    while raw := stream.readline(FRAME_BYTES + 1):
        try:
            previous = write_frame(raw=raw, directory=directory, previous=previous)
        except OutdatedHostFrame:
            # Startup/transport pauses can leave older frames in the bounded pipe. Keep
            # rejecting them until actual fresh counters arrive; stored evidence still expires.
            continue


def main() -> None:
    # Linux-only writer image. Exclusive volume lock rejects a second host feeder.
    if sys.platform != "linux":
        raise RuntimeError("Host evidence volume writes require Linux containers")
    import fcntl

    DIRECTORY.chmod(mode=0o755)
    descriptor = os.open(
        path=DIRECTORY / "writer.lock",
        flags=os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW,
        mode=0o600,
    )
    with os.fdopen(fd=descriptor, mode="wb") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        consume(stream=sys.stdin.buffer, directory=DIRECTORY)


if __name__ == "__main__":
    main()
