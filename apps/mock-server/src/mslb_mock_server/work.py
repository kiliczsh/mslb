"""Finite CPU work with cooperative cancellation and a real monotonic deadline."""

import time
from dataclasses import dataclass
from threading import Event

from mslb_contracts.traffic import MAX_OPERATIONS as MAX_OPERATIONS
from mslb_contracts.traffic import OPERATIONS_PER_UNIT as OPERATIONS_PER_UNIT

from mslb_mock_server.errors import WorkStopped as WorkStopped

WORK_TIMEOUT_SECONDS = 2.0
# FNV-style 32-bit mixing keeps the work deterministic and its integer size bounded.
CHECK_INTERVAL_OPERATIONS = 256
CHECKSUM_SEED = 0x811C9DC5
CHECKSUM_PRIME = 16_777_619
CHECKSUM_MASK = 0xFFFFFFFF


@dataclass(frozen=True, slots=True)
class CpuResult:
    """Return measured CPU work and elapsed time from one bounded executor task."""

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
    value = CHECKSUM_SEED
    for offset in range(0, count, CHECK_INTERVAL_OPERATIONS):
        if stopped.is_set():
            raise WorkStopped(code="WORK_CANCELLED")
        if time.monotonic() >= deadline:
            raise WorkStopped(code="WORK_TIMEOUT")
        for index in range(offset, min(offset + CHECK_INTERVAL_OPERATIONS, count)):
            value = ((value ^ index) * CHECKSUM_PRIME) & CHECKSUM_MASK
    ended = time.monotonic()
    if stopped.is_set():
        raise WorkStopped(code="WORK_CANCELLED")
    if ended >= deadline:
        raise WorkStopped(code="WORK_TIMEOUT")
    return CpuResult(
        operations=count, checksum=f"{value:08x}", duration_ms=(ended - started) * 1000
    )
