import asyncio
import json
import time
from collections.abc import AsyncIterator
from http import HTTPStatus
from threading import Event
from typing import override

import httpx
import pytest
from mslb_contracts.traffic import BackendHealth, WorkRequest
from mslb_controller.routing import BackendHttp, BackendSpec, Router, Strategy, TrafficError
from mslb_mock_server.app import Backend, Settings, create_app
from mslb_mock_server.work import CpuResult, WorkStopped, execute
from pydantic import ValidationError

from scripts.check_deployment import check


def payload(identifier: str = "request", memory: int = 1) -> WorkRequest:
    return WorkRequest(request_id=identifier, work_units=1, mem_mb=memory)


def health(identifier: str = "backend-a", memory: int = 1024) -> BackendHealth:
    return BackendHealth(
        backend_id=identifier,
        ready=True,
        logical_admission_mem_mb=memory,
        max_inflight=4,
        inflight=0,
        reserved_mem_mb=0,
        rss_bytes=None,
        cgroup_memory_bytes=None,
        cpu_usage_usec=None,
        throttled_usec=None,
    )


class Body(httpx.AsyncByteStream):
    def __init__(self, value: bytes) -> None:
        self.value = value
        self.closed = False

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        for offset in range(0, len(self.value), 4096):
            yield self.value[offset : offset + 4096]

    @override
    async def aclose(self) -> None:
        self.closed = True


def response(value: object, status: int = HTTPStatus.OK.value) -> httpx.Response:
    return httpx.Response(
        status,
        headers={"content-type": "application/json"},
        stream=Body(json.dumps(value).encode()),
    )


def test_cpu_work_is_finite_repeatable_and_deadline_or_cancellation_never_completes() -> None:
    first = execute(1, Event(), time.monotonic() + 2)
    second = execute(1, Event(), time.monotonic() + 2)
    assert first.operations == second.operations == 2048
    assert first.checksum == second.checksum
    assert len(first.checksum) == 8 and first.duration_ms >= 0
    with pytest.raises(WorkStopped, match="WORK_TIMEOUT"):
        execute(1_000_000_000, Event(), time.monotonic() - 1)
    stopped = Event()
    stopped.set()
    with pytest.raises(WorkStopped, match="WORK_CANCELLED"):
        execute(1, stopped, time.monotonic() + 2)
    for invalid in (0, -1, True, 1_000_000_001):
        with pytest.raises(ValueError):
            execute(invalid, Event(), time.monotonic() + 2)


def test_backend_independently_caps_memory_and_inflight_and_holds_cancelled_cpu_reservations() -> (
    None
):
    async def scenario() -> None:
        released = Event()

        def waiting(_units: int, stopped: Event, _deadline: float) -> CpuResult:
            if not released.wait(3):
                raise WorkStopped("WORK_TIMEOUT")
            if stopped.is_set():
                raise WorkStopped("WORK_CANCELLED")
            return CpuResult(2048, "00000000", 1.0)

        backend = Backend(Settings("backend-a", 8, 2), waiting)
        first = asyncio.create_task(backend.run(payload("first", 1)))
        second = asyncio.create_task(backend.run(payload("second", 1)))
        try:
            await asyncio.sleep(0.01)
            assert backend.inflight == 2 and backend.reserved == 2
            with pytest.raises(WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload("excess"))
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            assert backend.inflight == 2 and backend.reserved == 2
            with pytest.raises(WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload("still-excess"))
            released.set()
            await second
            for _ in range(100):
                if not backend.active:
                    break
                await asyncio.sleep(0.001)
            assert backend.inflight == backend.reserved == 0
            with pytest.raises(WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload("too-large", 9))
            released.clear()
            memory_owner = asyncio.create_task(backend.run(payload("memory-owner", 5)))
            await asyncio.sleep(0.01)
            assert backend.inflight == 1 and backend.reserved == 5
            with pytest.raises(WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload("aggregate-memory-excess", 4))
            released.set()
            await memory_owner
        finally:
            released.set()
            await backend.close()

    asyncio.run(scenario())


def test_backend_http_validates_streamed_bodies_and_exposes_real_health_without_echoing_input() -> (
    None
):
    async def scenario() -> None:
        settings = Settings("backend-a", 1_000_000_000)
        backend = Backend(settings)
        app = create_app(settings, backend)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://backend-a"
            ) as client:
                value = BackendHealth.model_validate((await client.get("/health")).json())
                assert value.backend_id == "backend-a" and value.inflight == 0
                # This reserves a billion logical MiB but executes only 2048 integer operations.
                actual = await client.post(
                    "/work", json=payload("real", 1_000_000_000).model_dump()
                )
                assert (
                    actual.status_code == HTTPStatus.OK.value
                    and actual.json()["operations"] == 2048
                )
                assert actual.json()["request_id"] == "real"
                for body in (
                    {"request_id": "sensitive-row", "work_units": "1", "mem_mb": 1},
                    {**payload().model_dump(), "command": "sensitive-row"},
                ):
                    invalid = await client.post("/work", json=body)
                    assert (
                        invalid.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                        and "sensitive-row" not in invalid.text
                    )
                assert (
                    await client.post(
                        "/work", content=b"{}", headers={"content-type": "text/plain"}
                    )
                ).status_code == HTTPStatus.UNSUPPORTED_MEDIA_TYPE.value

                async def large() -> AsyncIterator[bytes]:
                    for _ in range(17):
                        yield b"x" * 4096

                too_large = await client.post(
                    "/work", content=large(), headers={"content-type": "application/json"}
                )
                assert too_large.status_code == HTTPStatus.CONTENT_TOO_LARGE.value
                assert backend.inflight == backend.reserved == 0
        finally:
            await backend.close()

    asyncio.run(scenario())


def test_backend_timeout_is_an_explicit_error_and_shutdown_stops_admission() -> None:
    async def scenario() -> None:
        def expired(_units: int, _stopped: Event, _deadline: float) -> CpuResult:
            raise WorkStopped("WORK_TIMEOUT")

        settings = Settings("backend-b")
        backend = Backend(settings, expired)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(create_app(settings, backend)),
            base_url="http://backend-b",
        ) as client:
            timed_out = await client.post("/work", json=payload().model_dump())
            assert timed_out.status_code == HTTPStatus.GATEWAY_TIMEOUT.value
            assert timed_out.json() == {"error": {"code": "WORK_TIMEOUT"}}
            await backend.close()
            assert not (await client.get("/health")).json()["ready"]
            assert (
                await client.post("/work", json=payload().model_dump())
            ).status_code == HTTPStatus.TOO_MANY_REQUESTS.value

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "strategy,expected",
    [
        ("first_fit", ["backend-a", "backend-a", "backend-b"]),
        ("round_robin", ["backend-a", "backend-b", "backend-a"]),
        ("least_connections", ["backend-a", "backend-b", "backend-a"]),
        ("most_available_memory", ["backend-b", "backend-b", "backend-a"]),
    ],
)
def test_router_uses_all_strategies_with_stable_ties_and_reserved_logical_memory(
    strategy: Strategy, expected: list[str]
) -> None:
    async def scenario() -> None:
        http = BackendHttp()
        try:
            router = Router(
                (BackendSpec("backend-b", 16), BackendSpec("backend-a", 8)), strategy, http
            )
            for state in router.states.values():
                state.healthy = True
            reservations = [router.reserve(payload(f"r-{index}", 4)) for index in range(3)]
            assert [item.backend.spec.id for item in reservations] == expected
            for item in reservations:
                item.release()
                item.release()
            assert all(
                state.inflight == state.reserved_mem_mb == 0 for state in router.states.values()
            )
        finally:
            await http.close()

    asyncio.run(scenario())


def test_router_bounds_global_rate_health_drain_and_memory_independently() -> None:
    async def scenario() -> None:
        now = [10.0]
        http = BackendHttp()
        try:
            router = Router(
                (BackendSpec("backend-a", 8, 1, 1), BackendSpec("backend-b", 8, 1, 1)),
                "first_fit",
                http,
                max_inflight=1,
                clock=lambda: now[0],
            )
            with pytest.raises(TrafficError, match="NO_ELIGIBLE_BACKEND"):
                router.reserve(payload())
            for state in router.states.values():
                state.healthy = True
            first = router.reserve(payload())
            with pytest.raises(TrafficError, match="GLOBAL_INFLIGHT_LIMIT"):
                router.reserve(payload())
            first.release()
            router.states["backend-b"].draining = True
            with pytest.raises(TrafficError, match="NO_ELIGIBLE_BACKEND"):
                router.reserve(payload())
            now[0] += 1
            next_one = router.reserve(payload())
            assert next_one.backend.spec.id == "backend-a"
            next_one.release()
            with pytest.raises(TrafficError, match="UNSCHEDULABLE_MEMORY"):
                router.reserve(payload(memory=9))
            now[0] = float("nan")
            with pytest.raises(TrafficError, match="CLOCK_UNAVAILABLE"):
                router.reserve(payload())
        finally:
            await http.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "kind",
    ["success", "status", "redirect", "identity", "operations", "oversized", "timeout", "cancel"],
)
def test_real_transport_contracts_release_every_reservation_without_retries(kind: str) -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        calls = 0

        async def serve(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            if request.url.path == "/health":
                return response(health().model_dump())
            calls += 1
            entered.set()
            if kind == "timeout":
                raise httpx.ReadTimeout("sensitive-network-data")
            if kind == "cancel":
                await asyncio.Event().wait()
            result = {
                "backend_id": "backend-b" if kind == "identity" else "backend-a",
                "request_id": "request",
                "outcome": "completed",
                "operations": 1 if kind == "operations" else 2048,
                "checksum": "00000000",
                "duration_ms": 1.0,
            }
            if kind == "oversized":
                return httpx.Response(
                    HTTPStatus.OK.value,
                    headers={"content-type": "application/json"},
                    stream=Body(b"x" * 65537),
                )
            return response(
                result,
                HTTPStatus.SERVICE_UNAVAILABLE.value
                if kind == "status"
                else HTTPStatus.FOUND.value
                if kind == "redirect"
                else HTTPStatus.OK.value,
            )

        http = BackendHttp(httpx.MockTransport(serve))
        router = Router((BackendSpec("backend-a"),), "first_fit", http)
        try:
            await router.refresh()
            assert router.states["backend-a"].healthy
            task = asyncio.create_task(router.send(payload()))
            if kind == "cancel":
                await entered.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            elif kind == "success":
                assert (await task).backend_id == "backend-a"
            else:
                with pytest.raises(TrafficError) as error:
                    await task
                assert "sensitive" not in str(error.value)
                assert error.value.backend_id == "backend-a"
                if kind in {"status", "redirect"}:
                    assert error.value.status_code == (
                        HTTPStatus.SERVICE_UNAVAILABLE.value
                        if kind == "status"
                        else HTTPStatus.FOUND.value
                    )
            assert calls == 1
            assert router.states["backend-a"].inflight == 0
            assert router.states["backend-a"].reserved_mem_mb == 0
            assert router.states["backend-a"].starts == 1
        finally:
            await http.close()

    asyncio.run(scenario())


def test_inventory_rejects_arbitrary_endpoints_and_contracts_reject_bad_units() -> None:
    for identifier in (
        "localhost",
        "api",
        "169.254.169.254",
        "backend-a.evil.test",
        "backend-a:8080/path",
    ):
        with pytest.raises(ValueError):
            BackendSpec(identifier)
    for value in ("1", True, 0, 1_000_000_001):
        with pytest.raises(ValidationError):
            WorkRequest.model_validate({"request_id": "a", "work_units": value, "mem_mb": 1})
    with pytest.raises(ValidationError):
        WorkRequest(request_id="é" * 65, work_units=1, mem_mb=1)


@pytest.mark.parametrize(
    "service", ["backend-a", "backend-b", "container-controller", "docker-proxy", "resource-guard"]
)
def test_deployment_never_publishes_private_work_or_control_ports(service: str) -> None:
    with pytest.raises(ValueError, match="must not publish"):
        check(json.dumps({"services": {service: {"ports": [{"host_ip": "127.0.0.1"}]}}}).encode())
