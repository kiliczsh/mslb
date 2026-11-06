"""Cheap stdlib-only publication liveness; importing this never imports the sampler stack."""

import json
import math
import os
import stat
import sys
import time
from pathlib import Path

HEALTH = Path("/run/mslb-tmp/guard-health.json")


def write_health() -> None:
    if sys.platform != "linux":
        raise RuntimeError("Guard heartbeat storage requires Linux containers")
    temporary = HEALTH.with_suffix(suffix=".tmp")
    descriptor = os.open(
        path=temporary,
        flags=os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW,
        mode=0o600,
    )
    with os.fdopen(fd=descriptor, mode="w") as stream:
        stream.write(json.dumps(obj={"updated": time.monotonic()}))
    temporary.replace(target=HEALTH)


def healthy() -> bool:
    """Fail closed unless bounded private publisher heartbeat/evidence is current."""
    if sys.platform != "linux":
        return False
    try:
        descriptor = os.open(path=HEALTH, flags=os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd=descriptor, mode="rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
                return False
            raw = stream.read(1025)
        if len(raw) > 1024:
            return False
        updated: object = json.loads(s=raw).get("updated")
        if (
            isinstance(updated, bool)
            or not isinstance(updated, (int, float))
            or not math.isfinite(updated)
        ):
            return False
        return 0 <= time.monotonic() - updated <= 8
    except (OSError, ValueError, AttributeError, OverflowError):
        return False


if __name__ == "__main__":
    sys.exit(0 if healthy() else 1)
