"""Bounded backend admission, execution and shutdown lifecycle."""

import asyncio
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from threading import Event

from mslb_contracts.traffic import BackendHealth, WorkCompleted, WorkRequest

from mslb_mock_server.errors import WorkStopped
from mslb_mock_server.observations import _observations
from mslb_mock_server.settings import Settings
from mslb_mock_server.work import WORK_TIMEOUT_SECONDS, CpuResult, execute


class Backend:
    """Admission is non-queuing and remains reserved until CPU work actually stops."""

    def __init__(
        self,
        settings: Settings,
        work: Callable[[int, Event, float], CpuResult] = execute,
    ) -> None:
        self.settings = settings
        self.work = work
        self.inflight = 0
        self.reserved = 0
        self.pool = ThreadPoolExecutor(max_workers=settings.max_inflight, thread_name_prefix="work")
        self.active: dict[asyncio.Future[CpuResult], Event] = {}
        self.closed = False

    async def run(self, payload: WorkRequest) -> WorkCompleted:
        """Execute bounded real CPU work with explicit duration and cancellation outcomes."""
        if (
            self.closed
            or self.inflight >= self.settings.max_inflight
            or self.reserved + payload.mem_mb > self.settings.logical_admission_mem_mb
        ):
            raise WorkStopped(code="BACKEND_BUSY")
        self.inflight += 1
        self.reserved += payload.mem_mb
        stopped = Event()
        deadline = time.monotonic() + WORK_TIMEOUT_SECONDS
        try:
            future = asyncio.get_running_loop().run_in_executor(
                self.pool, self.work, payload.work_units, stopped, deadline
            )
        except (RuntimeError, OSError):
            self.inflight -= 1
            self.reserved -= payload.mem_mb
            raise WorkStopped(code="RESOURCE_PRESSURE") from None
        self.active[future] = stopped

        def release(done: asyncio.Future[CpuResult]) -> None:
            if self.active.pop(done, None) is None:
                return
            self.inflight -= 1
            self.reserved -= payload.mem_mb
            if not done.cancelled():
                # Retrieve detached-caller failures without logging input.
                done.exception()

        future.add_done_callback(release)
        try:
            result = await asyncio.shield(arg=future)
            return WorkCompleted(
                backend_id=self.settings.backend_id,
                request_id=payload.request_id,
                operations=result.operations,
                checksum=result.checksum,
                duration_ms=result.duration_ms,
            )
        except asyncio.CancelledError:
            stopped.set()
            # Retain admission until the future stops, including repeated caller cancellation.
            raise
        finally:
            if future.done():
                release(done=future)

    async def close(self) -> None:
        """Release backend executor resources during application shutdown."""
        self.closed = True
        for stopped in self.active.values():
            stopped.set()
        if self.active:
            await asyncio.gather(*self.active, return_exceptions=True)
        self.pool.shutdown(wait=True, cancel_futures=True)

    def health(self) -> BackendHealth:
        """Return backend identity, configured capacity and cumulative work counters."""
        return BackendHealth(
            backend_id=self.settings.backend_id,
            ready=not self.closed,
            logical_admission_mem_mb=self.settings.logical_admission_mem_mb,
            max_inflight=self.settings.max_inflight,
            inflight=self.inflight,
            reserved_mem_mb=self.reserved,
            **_observations(),
        )
