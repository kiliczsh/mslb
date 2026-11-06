"""Bounded arrivals, honest terminal accounting and no replay of real HTTP calls."""

import asyncio
from dataclasses import dataclass, field
from typing import override

import pytest
from mslb_contracts.docker_runs import DockerSettings
from mslb_contracts.docker_runtime import HttpEvent, StopReason
from mslb_contracts.inputs import RequestInput
from mslb_contracts.traffic import BackendHealth, WorkCompleted
from mslb_controller.routing import BackendHttp, BackendSpec, Reservation, TrafficError
from mslb_controller.traffic import TrafficDriver
from pydantic import ValidationError


@dataclass(slots=True, kw_only=True)
class FakeClock:
    current: float = 100.0

    def now(self) -> float:
        return self.current

    async def pause(self, *, seconds: float) -> None:
        self.current += seconds
        await asyncio.sleep(delay=0)


@dataclass(slots=True, kw_only=True)
class Sink:
    events: list[HttpEvent] = field(default_factory=list)

    async def append(self, *, event: HttpEvent) -> None:
        self.events.append(event)


class Transport(BackendHttp):
    def __init__(self, *, hang: bool = False, failure: bool = False) -> None:
        super().__init__()
        self.hang, self.failure = hang, failure
        self.calls: list[tuple[str, str]] = []
        self.cancelled = 0

    @override
    async def health(self, spec: BackendSpec) -> BackendHealth:
        return BackendHealth(
            backend_id=spec.id,
            ready=True,
            logical_admission_mem_mb=1024,
            max_inflight=4,
            inflight=0,
            reserved_mem_mb=0,
            rss_bytes=None,
            cgroup_memory_bytes=None,
            cpu_usage_usec=None,
            throttled_usec=None,
        )

    @override
    async def work(self, reservation: Reservation) -> WorkCompleted:
        self.calls.append((reservation.request.request_id, reservation.backend.spec.id))
        if self.failure:
            raise TrafficError(code="CONNECTION_FAILED")
        if self.hang:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled += 1
                raise
        return WorkCompleted(
            backend_id=reservation.backend.spec.id,
            request_id=reservation.request.request_id,
            outcome="completed",
            operations=2048,
            checksum="00000000",
            duration_ms=0.1,
        )


def rows(*, count: int, arrival: int = 0, memory: int = 1) -> tuple[RequestInput, ...]:
    return tuple(
        RequestInput(t=arrival, request_id=f"r-{index:05}", work_units=1, mem_mb=memory)
        for index in range(count)
    )


def test_arrivals_and_four_strategies_preserve_one_terminal_outcome_without_task_per_row() -> None:
    async def run() -> None:
        for strategy in ("first_fit", "round_robin", "least_connections", "most_available_memory"):
            settings = DockerSettings.model_validate(
                obj={"strategy": strategy, "duration_seconds": 3}
            )
            http, sink, clock = Transport(), Sink(), FakeClock()
            driver = TrafficDriver(
                configuration=settings,
                specs=(BackendSpec(id="backend-a"), BackendSpec(id="backend-b")),
                http=http,
                sink=sink,
                clock=clock,
            )
            try:
                inputs = rows(count=6) + (
                    RequestInput(t=1, request_id="later", work_units=1, mem_mb=1),
                )
                summary = await driver.execute(requests=inputs)
                assert summary.completed == 7 and summary.failed == summary.unexecuted == 0
                assert summary.peak_inflight <= 8 and summary.peak_pending <= 64
                assert len(http.calls) == len(set(http.calls)) == 7
                assert len(sink.events) == 21
                assert [event.sequence for event in sink.events] == list(range(1, 22))
                assert (
                    next(
                        event for event in sink.events if event.request_id == "later"
                    ).elapsed_seconds
                    >= 1
                )
                for row in inputs:
                    events = [
                        event.kind for event in sink.events if event.request_id == row.request_id
                    ]
                    assert events == ["arrived", "admitted", "completed"]
                with pytest.raises(expected_exception=ValueError, match="single-use"):
                    await driver.execute(requests=inputs)
                assert all(
                    state.inflight == state.reserved_mem_mb == 0
                    for state in driver.router.states.values()
                )
            finally:
                await http.close()

    asyncio.run(main=run())


def test_large_burst_bounds_queue_tasks_expiry_and_drain() -> None:
    async def run() -> None:
        http, sink = Transport(hang=True), Sink()
        driver = TrafficDriver(
            configuration=DockerSettings(
                initial_replicas=1,
                duration_seconds=2,
                backend_inflight=1,
                global_inflight=1,
                max_pending=2,
                pending_timeout_seconds=1,
                drain_timeout_seconds=1,
            ),
            specs=(BackendSpec(id="backend-a"),),
            http=http,
            sink=sink,
            clock=FakeClock(),
        )
        try:
            summary = await driver.execute(requests=rows(count=10_000))
            assert summary.total == summary.failed + summary.dropped == 10_000
            assert summary.failed == 1 and summary.peak_inflight == 1 and summary.peak_pending == 2
            assert len(http.calls) == http.cancelled == 1
            reasons = [event.reason for event in sink.events]
            assert reasons.count("PENDING_OVERFLOW") == 9997
            assert reasons.count("PENDING_TIMEOUT") == 2
            assert reasons.count("DRAIN_TIMEOUT") == 1
            assert len(sink.events) <= 30_000
            assert driver.router.states["backend-a"].reserved_mem_mb == 0
        finally:
            await http.close()

    asyncio.run(main=run())


def test_frozen_controller_limits_can_be_lower_than_static_backend_capacity() -> None:
    async def run() -> None:
        http, sink = Transport(), Sink()
        driver = TrafficDriver(
            configuration=DockerSettings(
                initial_replicas=1,
                duration_seconds=2,
                logical_admission_mem_mb=8,
                backend_inflight=1,
                starts_per_backend_second=1,
            ),
            specs=(BackendSpec(id="backend-a"),),
            http=http,
            sink=sink,
            clock=FakeClock(),
        )
        try:
            inputs = (
                RequestInput(t=0, request_id="a-too-large", work_units=1, mem_mb=9),
                RequestInput(t=0, request_id="b-first", work_units=1, mem_mb=8),
                RequestInput(t=0, request_id="c-later-bucket", work_units=1, mem_mb=8),
            )
            summary = await driver.execute(requests=inputs)
            assert summary.completed == 2 and summary.dropped == 1 and summary.peak_inflight == 1
            admitted = [event for event in sink.events if event.kind == "admitted"]
            assert admitted[0].elapsed_seconds < 1 <= admitted[1].elapsed_seconds
            assert [event.reason for event in sink.events].count("UNSCHEDULABLE_MEMORY") == 1
        finally:
            await http.close()

    asyncio.run(main=run())


@pytest.mark.parametrize(
    argnames="reason", argvalues=["CANCELLED", "FENCE_LOST", "RESOURCE_PRESSURE"]
)
def test_stop_does_not_send_pending_or_future_arrivals(reason: StopReason) -> None:
    async def run() -> None:
        http, sink, clock = Transport(hang=True), Sink(), FakeClock()

        def stop() -> StopReason | None:
            return reason if clock.current >= 100.5 else None

        driver = TrafficDriver(
            configuration=DockerSettings(
                initial_replicas=1, duration_seconds=3, backend_inflight=1, drain_timeout_seconds=1
            ),
            specs=(BackendSpec(id="backend-a"),),
            http=http,
            sink=sink,
            clock=clock,
            stop=stop,
        )
        try:
            inputs = rows(count=2) + (
                RequestInput(t=2, request_id="z-future", work_units=1, mem_mb=1),
            )
            summary = await driver.execute(requests=inputs)
            assert summary.status == ("cancelled" if reason == "CANCELLED" else "failed")
            assert summary.failed == 1 and summary.unexecuted == 2
            assert summary.stop_reason == reason and len(http.calls) == 1
            assert not any(
                event.kind == "arrived" and event.request_id == "z-future" for event in sink.events
            )
        finally:
            await http.close()

    asyncio.run(main=run())


def test_unknown_http_outcome_is_failed_once_and_never_retried() -> None:
    async def run() -> None:
        http, sink = Transport(failure=True), Sink()
        driver = TrafficDriver(
            configuration=DockerSettings(initial_replicas=1, duration_seconds=1),
            specs=(BackendSpec(id="backend-a"),),
            http=http,
            sink=sink,
            clock=FakeClock(),
        )
        try:
            summary = await driver.execute(requests=rows(count=1))
            assert summary.failed == 1 and summary.completed == 0 and len(http.calls) == 1
            assert sink.events[-1].reason == "CONNECTION_FAILED"
        finally:
            await http.close()

    asyncio.run(main=run())


def test_deadline_after_admission_is_unexecuted_and_never_sends_http() -> None:
    async def run() -> None:
        http, clock = Transport(), FakeClock()
        events: list[HttpEvent] = []

        class DelayedSink:
            async def append(self, *, event: HttpEvent) -> None:
                events.append(event)
                if event.kind == "admitted":
                    clock.current += 1

        driver = TrafficDriver(
            configuration=DockerSettings(initial_replicas=1, duration_seconds=1),
            specs=(BackendSpec(id="backend-a"),),
            http=http,
            sink=DelayedSink(),
            clock=clock,
        )
        try:
            summary = await driver.execute(requests=rows(count=1))
            assert summary.unexecuted == 1 and http.calls == []
            assert [event.kind for event in events] == ["arrived", "admitted", "unexecuted"]
            assert events[-1].reason == "RUN_DEADLINE"
            assert driver.router.states["backend-a"].inflight == 0
        finally:
            await http.close()

    asyncio.run(main=run())


def test_sink_failure_cancels_outstanding_calls_and_cannot_return_success() -> None:
    async def run() -> None:
        http = Transport(hang=True)

        class FailedSink:
            async def append(self, *, event: HttpEvent) -> None:
                if event.request_id == "r-00001" and event.kind == "arrived":
                    raise OSError("test storage unavailable")

        driver = TrafficDriver(
            configuration=DockerSettings(initial_replicas=1, duration_seconds=1),
            specs=(BackendSpec(id="backend-a"),),
            http=http,
            sink=FailedSink(),
            clock=FakeClock(),
        )
        try:
            with pytest.raises(expected_exception=OSError, match="storage unavailable"):
                await driver.execute(requests=rows(count=2))
            assert driver.inflight == {} and driver.router.states["backend-a"].inflight == 0
        finally:
            await http.close()

    asyncio.run(main=run())


def test_runtime_events_cannot_claim_logical_or_unobserved_success() -> None:
    base = {"sequence": 1, "kind": "arrived", "elapsed_seconds": 0.0, "request_id": "r"}
    for change in (
        {"kind": "finish"},
        {"mode": "concurrent"},
        {"elapsed_seconds": float("nan")},
        {"sequence": True},
        {"kind": "completed", "backend_id": "backend-a"},
        {"kind": "failed", "backend_id": "backend-a"},
        {"reason": "CANCELLED"},
    ):
        with pytest.raises(expected_exception=ValidationError):
            HttpEvent.model_validate(obj=base | change)
