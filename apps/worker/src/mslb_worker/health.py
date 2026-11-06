"""Small heartbeat reader/writer shared with the independent Docker health process."""

import json
import os
import tempfile
import time
from pathlib import Path

HEALTH = Path(tempfile.gettempdir()) / "mslb-worker" / "health.json"


def healthy() -> bool:
    try:
        with HEALTH.open(mode="rb") as stream:
            record = json.loads(s=stream.read(1024))
        if not isinstance(record, dict):
            return False
        value = record.get("updated")
        return isinstance(value, (float, int)) and 0 <= time.monotonic() - value < 10
    except (OSError, ValueError):
        return False


def write_health() -> None:
    if HEALTH.parent.is_symlink():
        raise ValueError("Unsafe worker health directory")
    HEALTH.parent.mkdir(mode=0o700, exist_ok=True)
    metadata = HEALTH.parent.stat()
    if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
        raise ValueError("Worker health directory must be private")
    temporary = HEALTH.with_suffix(suffix=".tmp")
    temporary.write_text(data=json.dumps(obj=dict(updated=time.monotonic())))
    temporary.replace(target=HEALTH)


if __name__ == "__main__":
    raise SystemExit(0 if healthy() else 1)
