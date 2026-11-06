"""Bounded nullable Linux process/cgroup observations."""

import mmap
from pathlib import Path
from typing import TypedDict


def _number(path: str) -> int | None:
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(129)
        return int(raw) if len(raw) <= 128 and raw.strip().isdigit() else None
    except (OSError, ValueError):
        return None


class Measurements(TypedDict):
    rss_bytes: int | None
    cgroup_memory_bytes: int | None
    cpu_usage_usec: int | None
    throttled_usec: int | None


def _observations() -> Measurements:
    rss: int | None = None
    cpu: dict[str, int] = {}
    try:
        with Path("/proc/self/statm").open("rb") as stream:
            fields = stream.read(256).split()
        rss = int(fields[1]) * mmap.PAGESIZE
        with Path("/sys/fs/cgroup/cpu.stat").open("rb") as stream:
            for line in stream.read(4096).splitlines():
                name, value = line.split()
                if name in {b"usage_usec", b"throttled_usec"}:
                    cpu[name.decode("ascii")] = int(value)
    except (OSError, ValueError, IndexError):
        pass  # Unsupported measurement is represented by null, never fabricated zero.
    return {
        "rss_bytes": rss,
        "cgroup_memory_bytes": _number("/sys/fs/cgroup/memory.current"),
        "cpu_usage_usec": cpu.get("usage_usec"),
        "throttled_usec": cpu.get("throttled_usec"),
    }
