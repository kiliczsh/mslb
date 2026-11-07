"""Single-use bounded real-time arrival scheduler; persistence/lifecycle live outside it."""

import asyncio
import math
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field, replace

from mslb_contracts.docker_runtime import HttpEvent, HttpReason, StopReason, TrafficSummary
from mslb_contracts.docker_settings import DockerSettings
from mslb_contracts.inputs import RequestInput
from mslb_contracts.traffic import WorkCompleted, WorkRequest

from mslb_controller.backend_http import BackendHttp
from mslb_controller.errors import DispatchRefused as DispatchRefused
from mslb_controller.errors import TrafficError
from mslb_controller.router import Router
from mslb_controller.routing_models import BackendSpec, HealthObserver, Reservation
from mslb_controller.traffic_clock import MonotonicClock as MonotonicClock
from mslb_controller.traffic_protocols import Clock as Clock
from mslb_controller.traffic_protocols import EventSink as EventSink
from mslb_controller.traffic_protocols import Membership


@dataclass(slots=True, kw_only=True)
class TrafficDriver:
    """Schedule bounded real HTTP arrivals using monotonic time and owned reservations."""

    configuration: DockerSettings
    specs: tuple[BackendSpec, ...]
    http: BackendHttp
    sink: EventSink
    clock: Clock = field(default_factory=MonotonicClock)
    stop: Callable[[], StopReason | None] = lambda: None
    membership: Membership | None = None
    health_observer: HealthObserver | None = None
    router: Router = field(init=False)
    used: bool = field(default=False, init=False)
    started: float = field(default=0, init=False)
    sequence: int = field(default=0, init=False)
    peak_pending: int = field(default=0, init=False)
    peak_inflight: int = field(default=0, init=False)
    counts: dict[str, int] = field(
        default_factory=lambda: dict.fromkeys(("completed", "failed", "dropped", "unexecuted"), 0),
        init=False,
    )
    pending: deque[RequestInput] = field(default_factory=deque, init=False)
    inflight: dict[asyncio.Task[WorkCompleted], Reservation] = field(
        default_factory=dict, init=False
    )

    def __post_init__(self) -> None:
        """Reject inconsistent traffic policy before owning tasks or reservations."""
        settings = self.configuration
        if settings.physical_scaling.enabled and self.membership is None:
            raise ValueError("Physical scaling requires a durable membership owner")
        if (
            self.http.request_seconds != settings.request_timeout_ms / 1000
            or self.http.health_seconds != settings.health_timeout_ms / 1000
        ):
            raise ValueError("HTTP transport timeouts must match the frozen configuration")
        if len(self.specs) != settings.initial_replicas:
            raise ValueError("Initial inspected inventory does not match the frozen configuration")
        self.router = Router(
            specs=tuple(
                replace(spec, starts_per_second=settings.starts_per_backend_second)
                for spec in self.specs
            ),
            strategy=settings.strategy,
            http=self.http,
            max_inflight=settings.global_inflight,
            backend_inflight=settings.backend_inflight,
            admission_mem_mb=settings.logical_admission_mem_mb,
            clock=self.clock.now,
            health_observer=self.health_observer,
        )

    def elapsed(self) -> float:
        elapsed = self.clock.now() - self.started
        if not math.isfinite(elapsed) or elapsed < 0:
            raise TrafficError(code="CLOCK_UNAVAILABLE")
        return elapsed

    def halt(self) -> StopReason | None:
        reason = self.stop()
        if reason is not None:
            if reason not in {"CANCELLED", "FENCE_LOST", "RESOURCE_PRESSURE"}:
                raise ValueError("Unsupported external traffic stop reason")
            return reason
        return "RUN_DEADLINE" if self.elapsed() >= self.configuration.duration_seconds else None

    async def emit(
        self,
        *,
        request: RequestInput,
        kind: str,
        backend_id: str | None = None,
        reason: HttpReason | None = None,
        result: WorkCompleted | None = None,
    ) -> None:
        """Append a contiguous real-time event without inventing a logical finish."""
        self.sequence += 1
        event = HttpEvent.model_validate(
            obj={
                "sequence": self.sequence,
                "kind": kind,
                "elapsed_seconds": self.elapsed(),
                "request_id": request.request_id,
                "backend_id": backend_id,
                "reason": reason,
                "result": result,
            }
        )
        await self.sink.append(event=event)
        if kind in self.counts:
            self.counts[kind] += 1

    async def collect(self) -> None:
        """Settle completed HTTP tasks and release only their owned reservations."""
        for task in tuple(self.inflight):
            if not task.done():
                continue
            reservation = self.inflight.pop(task)
            request = RequestInput(t=0, **reservation.request.model_dump())
            try:
                result = task.result()
            except DispatchRefused as error:
                await self.emit(request=request, kind="unexecuted", reason=error.reason)
            except TrafficError as error:
                reason: HttpReason = (
                    error.code
                    if error.code
                    in ("HTTP_STATUS", "CONNECTION_FAILED", "INVALID_RESPONSE", "HTTP_TIMEOUT")
                    else "HTTP_FAILURE"
                )
                reservation.backend.healthy = False
                await self.emit(
                    request=request,
                    kind="failed",
                    backend_id=reservation.backend.spec.id,
                    reason=reason,
                )
            else:
                await self.emit(
                    request=request,
                    kind="completed",
                    backend_id=reservation.backend.spec.id,
                    result=result,
                )
            finally:
                reservation.release()

    async def send(self, *, request: RequestInput, reservation: Reservation) -> WorkCompleted:
        reason = self.halt()
        if reason is not None:
            raise DispatchRefused(reason=reason)
        if self.elapsed() - request.t >= self.configuration.pending_timeout_seconds:
            raise DispatchRefused(reason="PENDING_TIMEOUT")
        return await self.http.work(reservation=reservation)

    async def dispatch(self) -> None:
        """Recheck stop/age limits before issuing one bounded real HTTP call."""
        while self.pending and self.halt() is None:
            request = self.pending[0]
            if self.elapsed() - request.t >= self.configuration.pending_timeout_seconds:
                self.pending.popleft()
                await self.emit(request=request, kind="dropped", reason="PENDING_TIMEOUT")
                continue
            try:
                reservation = self.router.reserve(
                    request=WorkRequest(
                        request_id=request.request_id,
                        work_units=request.work_units,
                        mem_mb=request.mem_mb,
                    )
                )
            except TrafficError as error:
                if error.code != "UNSCHEDULABLE_MEMORY":
                    break
                self.pending.popleft()
                await self.emit(request=request, kind="dropped", reason="UNSCHEDULABLE_MEMORY")
                continue
            self.pending.popleft()
            # Admission owns a reservation even if its event write delays or prevents dispatch.
            self.peak_inflight = max(self.peak_inflight, len(self.inflight) + 1)
            try:
                await self.emit(
                    request=request, kind="admitted", backend_id=reservation.backend.spec.id
                )
                reason = self.halt()
                if reason is not None:
                    await self.emit(request=request, kind="unexecuted", reason=reason)
                    continue
                if self.elapsed() - request.t >= self.configuration.pending_timeout_seconds:
                    await self.emit(request=request, kind="unexecuted", reason="PENDING_TIMEOUT")
                    continue
                task = asyncio.create_task(coro=self.send(request=request, reservation=reservation))
                self.inflight[task] = reservation
            finally:
                if not any(item is reservation for item in self.inflight.values()):
                    reservation.release()

    async def cancel_calls(self, *, reason: HttpReason) -> None:
        """Bound cancellation settlement while retaining unresolved reservations."""
        tasks = tuple(self.inflight)
        for task in tasks:
            task.cancel()
        if tasks:
            _done, remaining = await asyncio.wait(fs=tasks, timeout=1)
            if remaining:
                # Do not release reservations for transport tasks that have not settled.
                raise TrafficError(code="DRAIN_UNSETTLED")
        for task, reservation in tuple(self.inflight.items()):
            self.inflight.pop(task)
            try:
                if not task.cancelled():
                    task.exception()  # Consume a late exception; cancellation never claims success.
                await self.emit(
                    request=RequestInput(t=0, **reservation.request.model_dump()),
                    kind="failed",
                    backend_id=reservation.backend.spec.id,
                    reason=reason,
                )
            finally:
                reservation.release()

    async def execute(self, *, requests: tuple[RequestInput, ...]) -> TrafficSummary:
        """Replay arrivals with bounded pending/inflight work and explicit terminal accounting."""
        if self.used:
            raise ValueError("Real HTTP traffic is single-use and must never be replayed")
        if (
            not 1 <= len(requests) <= 10_000
            or len({row.request_id for row in requests}) != len(requests)
            or tuple(sorted(requests, key=lambda row: (row.t, row.request_id))) != requests
            or requests[-1].t >= self.configuration.duration_seconds
        ):
            raise ValueError("Traffic arrivals are not normalized within the frozen envelope")
        self.used = True
        # Startup health is outside the arrival epoch; the owning supervisor covers its lease.
        await self.router.refresh()
        self.started = self.clock.now()
        self.router.started = self.started
        cursor, next_health = 0, 1.0
        refresh: asyncio.Task[None] | None = None
        membership: asyncio.Task[None] | None = None
        stopped: StopReason | None = None
        try:
            while cursor < len(requests) or self.pending or self.inflight:
                await self.collect()
                stopped = self.halt()
                if stopped is not None:
                    break
                while (
                    self.pending
                    and self.elapsed() - self.pending[0].t
                    >= self.configuration.pending_timeout_seconds
                ):
                    await self.emit(
                        request=self.pending.popleft(), kind="dropped", reason="PENDING_TIMEOUT"
                    )
                await self.dispatch()
                while (
                    cursor < len(requests)
                    and requests[cursor].t <= self.elapsed()
                    and self.halt() is None
                ):
                    request = requests[cursor]
                    cursor += 1
                    queued = len(self.pending) < self.configuration.max_pending
                    if queued:
                        # Queue membership is established at arrival observation, before sink I/O.
                        self.pending.append(request)
                        self.peak_pending = max(self.peak_pending, len(self.pending))
                    await self.emit(request=request, kind="arrived")
                    arrival_stop = self.halt()
                    if arrival_stop is not None:
                        if queued:
                            self.pending.pop()
                        await self.emit(request=request, kind="unexecuted", reason=arrival_stop)
                    elif not queued:
                        await self.emit(request=request, kind="dropped", reason="PENDING_OVERFLOW")
                    else:
                        await self.dispatch()
                if refresh is not None and refresh.done():
                    refresh.result()
                    refresh = None
                if membership is not None and membership.done():
                    membership.result()
                    membership = None
                if self.elapsed() >= next_health and refresh is None:
                    refresh = asyncio.create_task(coro=self.router.refresh())
                    next_health = self.elapsed() + 1
                    if self.membership is not None and membership is None:
                        membership = asyncio.create_task(
                            coro=self.membership(router=self.router, pending=len(self.pending))
                        )
                if cursor < len(requests) or self.pending or self.inflight:
                    await self.clock.pause(seconds=0.02)
            if stopped is not None:
                while self.pending:
                    await self.emit(
                        request=self.pending.popleft(), kind="unexecuted", reason=stopped
                    )
                while cursor < len(requests):
                    await self.emit(request=requests[cursor], kind="unexecuted", reason=stopped)
                    cursor += 1
            drain_end = self.clock.now() + self.configuration.drain_timeout_seconds
            while self.inflight and self.clock.now() < drain_end:
                await self.collect()
                if self.inflight:
                    await self.clock.pause(seconds=0.02)
            await self.collect()
            if self.inflight:
                await self.cancel_calls(reason="DRAIN_TIMEOUT")
        finally:
            if membership is not None:
                membership.cancel()
                await asyncio.gather(membership, return_exceptions=True)
            if refresh is not None:
                refresh.cancel()
                await asyncio.gather(refresh, return_exceptions=True)
            if self.inflight:
                await self.cancel_calls(reason="CANCELLED")
        return TrafficSummary(
            status="cancelled"
            if stopped == "CANCELLED"
            else "failed"
            if stopped in {"FENCE_LOST", "RESOURCE_PRESSURE"}
            else "completed",
            stop_reason=stopped,
            total=len(requests),
            completed=self.counts["completed"],
            failed=self.counts["failed"],
            dropped=self.counts["dropped"],
            unexecuted=self.counts["unexecuted"],
            elapsed_seconds=self.elapsed(),
            peak_pending=self.peak_pending,
            peak_inflight=self.peak_inflight,
        )
