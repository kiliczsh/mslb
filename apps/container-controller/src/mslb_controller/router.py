"""Atomic backend selection and admission on one event loop."""

import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from mslb_contracts.traffic import WorkCompleted, WorkRequest

from mslb_controller.backend_http import BackendHttp
from mslb_controller.errors import TrafficError
from mslb_controller.routing_models import (
    STRATEGIES,
    BackendSpec,
    BackendState,
    Reservation,
    Strategy,
)


@dataclass(slots=True)
class Router:
    """Select/reserve on one asyncio loop, with no await between eligibility and reservation.

    The durable driver owns pending arrivals, health refresh cadence, run deadline,
    cancellation and membership lifecycle. These primitives neither queue work nor
    retry a real call whose outcome may be unknown.
    """

    specs: tuple[BackendSpec, ...]
    strategy: Strategy
    http: BackendHttp
    max_inflight: int = 16
    backend_inflight: int = 4
    admission_mem_mb: int = 1_000_000_000
    clock: Callable[[], float] = time.monotonic
    states: dict[str, BackendState] = field(init=False)
    anchor: str | None = field(default=None, init=False)
    started: float = field(init=False)

    def __post_init__(self) -> None:
        if (
            self.strategy not in STRATEGIES
            or type(self.max_inflight) is not int
            or not 1 <= self.max_inflight <= 16
            or type(self.backend_inflight) is not int
            or not 1 <= self.backend_inflight <= 4
            or type(self.admission_mem_mb) is not int
            or not 1 <= self.admission_mem_mb <= 1_000_000_000
            or not 1 <= len(self.specs) <= 4
            or len({spec.id for spec in self.specs}) != len(self.specs)
        ):
            raise ValueError("Invalid routing envelope")
        self.states = {
            spec.id: BackendState(spec=spec) for spec in sorted(self.specs, key=lambda s: s.id)
        }
        self.started = self.clock()

    async def refresh(self) -> None:
        """Bounded serial health probing; callers must not overlap refresh loops."""
        for state in self.states.values():
            try:
                state.observed = await self.http.health(spec=state.spec)
                state.healthy = state.observed.ready
            except TrafficError:
                state.observed = None
                state.healthy = False

    def reserve(self, request: WorkRequest) -> Reservation:
        """Choose an eligible backend and reserve memory, starts and inflight atomically."""
        if sum(state.inflight for state in self.states.values()) >= self.max_inflight:
            raise TrafficError(code="GLOBAL_INFLIGHT_LIMIT")
        elapsed = self.clock() - self.started
        if not math.isfinite(elapsed) or elapsed < 0:
            raise TrafficError(code="CLOCK_UNAVAILABLE")
        bucket = math.floor(elapsed)
        eligible: list[BackendState] = []
        for state in self.states.values():
            if state.start_bucket != bucket:
                state.start_bucket, state.starts = bucket, 0
            if (
                state.healthy
                and not state.draining
                and state.inflight < min(state.spec.max_inflight, self.backend_inflight)
                and state.starts < state.spec.starts_per_second
                and state.reserved_mem_mb + request.mem_mb
                <= min(state.spec.logical_admission_mem_mb, self.admission_mem_mb)
            ):
                eligible.append(state)
        if not eligible:
            reason = (
                "UNSCHEDULABLE_MEMORY"
                if all(
                    request.mem_mb > min(state.spec.logical_admission_mem_mb, self.admission_mem_mb)
                    for state in self.states.values()
                )
                else "NO_ELIGIBLE_BACKEND"
            )
            raise TrafficError(code=reason)
        if self.strategy == "round_robin":
            selected = next(
                (state for state in eligible if self.anchor is None or state.spec.id > self.anchor),
                eligible[0],
            )
            self.anchor = selected.spec.id
        elif self.strategy == "least_connections":
            selected = min(eligible, key=lambda state: (state.inflight, state.spec.id))
        elif self.strategy == "most_available_memory":
            selected = min(
                eligible,
                key=lambda state: (
                    -(
                        min(state.spec.logical_admission_mem_mb, self.admission_mem_mb)
                        - state.reserved_mem_mb
                    ),
                    state.spec.id,
                ),
            )
        else:
            selected = eligible[0]
        selected.inflight += 1
        selected.reserved_mem_mb += request.mem_mb
        selected.starts += 1
        return Reservation(backend=selected, request=request)

    async def send(self, request: WorkRequest) -> WorkCompleted:
        reservation = self.reserve(request=request)
        try:
            return await self.http.work(reservation=reservation)
        except TrafficError as error:
            if error.code in {"CONNECTION_FAILED", "HTTP_TIMEOUT", "INVALID_RESPONSE"}:
                reservation.backend.healthy = False
            raise
        finally:
            reservation.release()
