"""Non-queuing real HTTP routing primitives for the durable controller driver."""

import asyncio
import math
import re
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal

import httpx
from mslb_contracts.traffic import BackendHealth, WorkCompleted, WorkRequest
from pydantic import BaseModel, ValidationError

type Strategy = Literal["first_fit", "round_robin", "least_connections", "most_available_memory"]
STRATEGIES: tuple[Strategy, ...] = (
    "first_fit",
    "round_robin",
    "least_connections",
    "most_available_memory",
)


class TrafficError(Exception):
    """Safe operational refusal/failure; never a logical simulation outcome."""

    def __init__(
        self, code: str, backend_id: str | None = None, status_code: int | None = None
    ) -> None:
        self.code = code
        self.backend_id = backend_id
        self.status_code = status_code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class BackendSpec:
    """Trusted lifecycle inventory, never a public caller's URL/image configuration."""

    id: str
    logical_admission_mem_mb: int = 1024
    max_inflight: int = 4
    starts_per_second: int = 4

    def __post_init__(self) -> None:
        if not re.fullmatch(pattern=r"backend-[ab]|mslb-[0-9a-f]{32}-[0-3]", string=self.id):
            raise ValueError("Only dedicated backend network identities are permitted")
        for value, maximum in (
            (self.logical_admission_mem_mb, 1_000_000_000),
            (self.max_inflight, 4),
            (self.starts_per_second, 32),
        ):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("Backend admission configuration exceeds its envelope")

    @property
    def url(self) -> str:
        return f"http://{self.id}:8080"


@dataclass(slots=True)
class BackendState:
    spec: BackendSpec
    healthy: bool = False
    draining: bool = False
    inflight: int = 0
    reserved_mem_mb: int = 0
    start_bucket: int = -1
    starts: int = 0
    observed: BackendHealth | None = None


@dataclass(slots=True)
class Reservation:
    """An event-loop-owned reservation; release is idempotent across all terminal paths."""

    backend: BackendState
    request: WorkRequest
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        self.backend.inflight -= 1
        self.backend.reserved_mem_mb -= self.request.mem_mb


async def _json(response: httpx.Response, model: type[BaseModel]) -> BaseModel:
    if response.status_code != 200:
        raise TrafficError(code="HTTP_STATUS", status_code=response.status_code)
    if (
        response.headers.get(key="content-type", default="").split(sep=";")[0].lower()
        != "application/json"
        or response.headers.get(key="content-encoding", default="identity").lower() != "identity"
    ):
        raise TrafficError(code="INVALID_RESPONSE")
    payload = bytearray()
    async for chunk in response.aiter_raw(chunk_size=65536):
        if len(payload) + len(chunk) > 65536:
            raise TrafficError(code="INVALID_RESPONSE")
        payload.extend(chunk)
    try:
        return model.model_validate_json(json_data=payload)
    except ValidationError:
        raise TrafficError(code="INVALID_RESPONSE") from None


class BackendHttp:
    """No redirects, environment proxies, retries or unbounded response/pool state."""

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport | None = None,
        *,
        request_timeout_ms: int = 3000,
        health_timeout_ms: int = 1000,
    ) -> None:
        if (
            type(request_timeout_ms) is not int
            or not 100 <= request_timeout_ms <= 3000
            or type(health_timeout_ms) is not int
            or not 100 <= health_timeout_ms <= 1000
        ):
            raise ValueError("HTTP timeouts exceed the supported envelope")
        self.request_seconds = request_timeout_ms / 1000
        self.health_seconds = health_timeout_ms / 1000
        self.client = httpx.AsyncClient(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(timeout=self.request_seconds, connect=0.5, pool=0.1),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    async def health(self, spec: BackendSpec) -> BackendHealth:
        try:
            async with (
                asyncio.timeout(delay=self.health_seconds),
                self.client.stream(method="GET", url=spec.url + "/health") as response,
            ):
                value = await _json(response=response, model=BackendHealth)
            if not isinstance(value, BackendHealth) or value.backend_id != spec.id:
                raise TrafficError(code="INVALID_RESPONSE", backend_id=spec.id)
            if (
                value.logical_admission_mem_mb != spec.logical_admission_mem_mb
                or value.max_inflight != spec.max_inflight
            ):
                raise TrafficError(code="BACKEND_CONFIGURATION_MISMATCH", backend_id=spec.id)
            return value
        except TrafficError as error:
            raise TrafficError(
                code=error.code, backend_id=spec.id, status_code=error.status_code
            ) from None
        except (TimeoutError, httpx.TimeoutException):
            raise TrafficError(code="HTTP_TIMEOUT", backend_id=spec.id) from None
        except httpx.HTTPError:
            raise TrafficError(code="CONNECTION_FAILED", backend_id=spec.id) from None

    async def work(self, reservation: Reservation) -> WorkCompleted:
        spec, payload = reservation.backend.spec, reservation.request
        try:
            async with (
                asyncio.timeout(delay=self.request_seconds),
                self.client.stream(
                    method="POST", url=spec.url + "/work", json=payload.model_dump()
                ) as response,
            ):
                value = await _json(response=response, model=WorkCompleted)
            if (
                not isinstance(value, WorkCompleted)
                or value.backend_id != spec.id
                or value.request_id != payload.request_id
                or value.operations != min(payload.work_units * 2048, 2_000_000)
            ):
                raise TrafficError(code="INVALID_RESPONSE", backend_id=spec.id)
            return value
        except TrafficError as error:
            raise TrafficError(
                code=error.code, backend_id=spec.id, status_code=error.status_code
            ) from None
        except (TimeoutError, httpx.TimeoutException):
            raise TrafficError(code="HTTP_TIMEOUT", backend_id=spec.id) from None
        except httpx.HTTPError:
            raise TrafficError(code="CONNECTION_FAILED", backend_id=spec.id) from None

    async def close(self) -> None:
        await self.client.aclose()


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
