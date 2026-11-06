"""Finite CPU work with cooperative cancellation and a real monotonic deadline."""

import time
from dataclasses import dataclass
from threading import Event

OPERATIONS_PER_UNIT = 2048
MAX_OPERATIONS = 2_000_000
WORK_TIMEOUT_SECONDS = 2.0


class WorkStopped(Exception):
    """Execution stopped without a completed outcome."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class CpuResult:
    operations: int
    checksum: str
    duration_ms: float


def execute(work_units: int, stopped: Event, deadline: float) -> CpuResult:
    """Mix bounded 32-bit integers; no physical allocation follows logical mem_mb.

    Check cancellation/deadline every 256 operations. A successful result always
    performs min(work_units * 2048, 2_000_000) operations; interrupted work never
    reports completed. The deadline includes executor scheduling time.
    """
    started = time.monotonic()
    if type(work_units) is not int or not 1 <= work_units <= 1_000_000_000:
        raise ValueError("Work units exceed the supported envelope")
    count = min(work_units * OPERATIONS_PER_UNIT, MAX_OPERATIONS)
    value = 0x811C9DC5
    for offset in range(0, count, 256):
        if stopped.is_set():
            raise WorkStopped("WORK_CANCELLED")
        if time.monotonic() >= deadline:
            raise WorkStopped("WORK_TIMEOUT")
        for index in range(offset, min(offset + 256, count)):
            value = ((value ^ index) * 16_777_619) & 0xFFFFFFFF
    ended = time.monotonic()
    if stopped.is_set():
        raise WorkStopped("WORK_CANCELLED")
    if ended >= deadline:
        raise WorkStopped("WORK_TIMEOUT")
    return CpuResult(count, f"{value:08x}", (ended - started) * 1000)
