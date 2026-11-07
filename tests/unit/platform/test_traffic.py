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
        status_code=status,
        headers={"content-type": "application/json"},
        stream=Body(value=json.dumps(obj=value).encode()),
    )


def test_cpu_work_is_finite_repeatable_and_deadline_or_cancellation_never_completes() -> None:
    first = execute(work_units=1, stopped=Event(), deadline=time.monotonic() + 2)
    second = execute(work_units=1, stopped=Event(), deadline=time.monotonic() + 2)
    assert first.operations == second.operations == 2048
    assert first.checksum == second.checksum
    assert len(first.checksum) == 8 and first.duration_ms >= 0
    with pytest.raises(expected_exception=WorkStopped, match="WORK_TIMEOUT"):
        execute(work_units=1_000_000_000, stopped=Event(), deadline=time.monotonic() - 1)
    stopped = Event()
    stopped.set()
    with pytest.raises(expected_exception=WorkStopped, match="WORK_CANCELLED"):
        execute(work_units=1, stopped=stopped, deadline=time.monotonic() + 2)
    for invalid in (0, -1, True, 1_000_000_001):
        with pytest.raises(expected_exception=ValueError):
            execute(work_units=invalid, stopped=Event(), deadline=time.monotonic() + 2)


def test_backend_independently_caps_memory_and_inflight_and_holds_cancelled_cpu_reservations() -> (
    None
):
    async def scenario() -> None:
        released = Event()

        def waiting(_units: int, stopped: Event, _deadline: float) -> CpuResult:
            if not released.wait(timeout=3):
                raise WorkStopped(code="WORK_TIMEOUT")
            if stopped.is_set():
                raise WorkStopped(code="WORK_CANCELLED")
            return CpuResult(operations=2048, checksum="00000000", duration_ms=1.0)

        backend = Backend(
            settings=Settings(backend_id="backend-a", logical_admission_mem_mb=8, max_inflight=2),
            work=waiting,
        )
        first = asyncio.create_task(coro=backend.run(payload=payload(identifier="first", memory=1)))
        second = asyncio.create_task(
            coro=backend.run(payload=payload(identifier="second", memory=1))
        )
        try:
            await asyncio.sleep(delay=0.01)
            assert backend.inflight == 2 and backend.reserved == 2
            with pytest.raises(expected_exception=WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload=payload(identifier="excess"))
            first.cancel()
            with pytest.raises(expected_exception=asyncio.CancelledError):
                await first
            assert backend.inflight == 2 and backend.reserved == 2
            with pytest.raises(expected_exception=WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload=payload(identifier="still-excess"))
            released.set()
            await second
            for _ in range(100):
                if not backend.active:
                    break
                await asyncio.sleep(delay=0.001)
            assert backend.inflight == backend.reserved == 0
            with pytest.raises(expected_exception=WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload=payload(identifier="too-large", memory=9))
            released.clear()
            memory_owner = asyncio.create_task(
                coro=backend.run(payload=payload(identifier="memory-owner", memory=5))
            )
            await asyncio.sleep(delay=0.01)
            assert backend.inflight == 1 and backend.reserved == 5
            with pytest.raises(expected_exception=WorkStopped, match="BACKEND_BUSY"):
                await backend.run(payload=payload(identifier="aggregate-memory-excess", memory=4))
            released.set()
            await memory_owner
        finally:
            released.set()
            await backend.close()

    asyncio.run(main=scenario())


def test_backend_http_validates_streamed_bodies_and_exposes_real_health_without_echoing_input() -> (
    None
):
    async def scenario() -> None:
        settings = Settings(backend_id="backend-a", logical_admission_mem_mb=1_000_000_000)
        backend = Backend(settings=settings)
        app = create_app(settings=settings, backend=backend)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://backend-a"
            ) as client:
                value = BackendHealth.model_validate(obj=(await client.get(url="/health")).json())
                assert value.backend_id == "backend-a" and value.inflight == 0
                # This reserves a billion logical MiB but executes only 2048 integer operations.
                actual = await client.post(
                    url="/work", json=payload(identifier="real", memory=1_000_000_000).model_dump()
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
                    invalid = await client.post(url="/work", json=body)
                    assert (
                        invalid.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                        and "sensitive-row" not in invalid.text
                    )
                assert (
                    await client.post(
                        url="/work", content=b"{}", headers={"content-type": "text/plain"}
                    )
                ).status_code == HTTPStatus.UNSUPPORTED_MEDIA_TYPE.value

                async def large() -> AsyncIterator[bytes]:
                    for _ in range(17):
                        yield b"x" * 4096

                too_large = await client.post(
                    url="/work", content=large(), headers={"content-type": "application/json"}
                )
                assert too_large.status_code == HTTPStatus.CONTENT_TOO_LARGE.value
                assert backend.inflight == backend.reserved == 0
        finally:
            await backend.close()

    asyncio.run(main=scenario())


def test_backend_timeout_is_an_explicit_error_and_shutdown_stops_admission() -> None:
    async def scenario() -> None:
        def expired(_units: int, _stopped: Event, _deadline: float) -> CpuResult:
            raise WorkStopped(code="WORK_TIMEOUT")

        settings = Settings(backend_id="backend-b")
        backend = Backend(settings=settings, work=expired)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(settings=settings, backend=backend)),
            base_url="http://backend-b",
        ) as client:
            timed_out = await client.post(url="/work", json=payload().model_dump())
            assert timed_out.status_code == HTTPStatus.GATEWAY_TIMEOUT.value
            assert timed_out.json() == {"error": {"code": "WORK_TIMEOUT"}}
            await backend.close()
            assert not (await client.get(url="/health")).json()["ready"]
            assert (
                await client.post(url="/work", json=payload().model_dump())
            ).status_code == HTTPStatus.TOO_MANY_REQUESTS.value

    asyncio.run(main=scenario())


@pytest.mark.parametrize(
    argnames="strategy,expected",
    argvalues=[
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
                specs=(
                    BackendSpec(id="backend-b", logical_admission_mem_mb=16),
                    BackendSpec(id="backend-a", logical_admission_mem_mb=8),
                ),
                strategy=strategy,
                http=http,
            )
            for state in router.states.values():
                state.healthy = True
            reservations = [
                router.reserve(request=payload(identifier=f"r-{index}", memory=4))
                for index in range(3)
            ]
            assert [item.backend.spec.id for item in reservations] == expected
            for item in reservations:
                item.release()
                item.release()
            assert all(
                state.inflight == state.reserved_mem_mb == 0 for state in router.states.values()
            )
        finally:
            await http.close()

    asyncio.run(main=scenario())


def test_router_bounds_global_rate_health_drain_and_memory_independently() -> None:
    async def scenario() -> None:
        now = [10.0]
        http = BackendHttp()
        try:
            router = Router(
                specs=(
                    BackendSpec(
                        id="backend-a",
                        logical_admission_mem_mb=8,
                        max_inflight=1,
                        starts_per_second=1,
                    ),
                    BackendSpec(
                        id="backend-b",
                        logical_admission_mem_mb=8,
                        max_inflight=1,
                        starts_per_second=1,
                    ),
                ),
                strategy="first_fit",
                http=http,
                max_inflight=1,
                clock=lambda: now[0],
            )
            with pytest.raises(expected_exception=TrafficError, match="NO_ELIGIBLE_BACKEND"):
                router.reserve(request=payload())
            for state in router.states.values():
                state.healthy = True
            first = router.reserve(request=payload())
            with pytest.raises(expected_exception=TrafficError, match="GLOBAL_INFLIGHT_LIMIT"):
                router.reserve(request=payload())
            first.release()
            router.states["backend-b"].draining = True
            with pytest.raises(expected_exception=TrafficError, match="NO_ELIGIBLE_BACKEND"):
                router.reserve(request=payload())
            now[0] += 1
            next_one = router.reserve(request=payload())
            assert next_one.backend.spec.id == "backend-a"
            next_one.release()
            with pytest.raises(expected_exception=TrafficError, match="UNSCHEDULABLE_MEMORY"):
                router.reserve(request=payload(memory=9))
            now[0] = float("nan")
            with pytest.raises(expected_exception=TrafficError, match="CLOCK_UNAVAILABLE"):
                router.reserve(request=payload())
        finally:
            await http.close()

    asyncio.run(main=scenario())


@pytest.mark.parametrize(
    argnames="kind",
    argvalues=[
        "success",
        "status",
        "redirect",
        "identity",
        "operations",
        "oversized",
        "timeout",
        "cancel",
    ],
)
def test_real_transport_contracts_release_every_reservation_without_retries(kind: str) -> None:
    async def scenario() -> None:
        entered = asyncio.Event()
        calls = 0

        async def serve(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            if request.url.path == "/health":
                return response(value=health().model_dump())
            calls += 1
            entered.set()
            if kind == "timeout":
                raise httpx.ReadTimeout(message="sensitive-network-data")
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
                    status_code=HTTPStatus.OK.value,
                    headers={"content-type": "application/json"},
                    stream=Body(value=b"x" * 65537),
                )
            return response(
                value=result,
                status=HTTPStatus.SERVICE_UNAVAILABLE.value
                if kind == "status"
                else HTTPStatus.FOUND.value
                if kind == "redirect"
                else HTTPStatus.OK.value,
            )

        http = BackendHttp(transport=httpx.MockTransport(handler=serve))
        router = Router(specs=(BackendSpec(id="backend-a"),), strategy="first_fit", http=http)
        try:
            await router.refresh()
            assert router.states["backend-a"].healthy
            task = asyncio.create_task(coro=router.send(request=payload()))
            if kind == "cancel":
                await entered.wait()
                task.cancel()
                with pytest.raises(expected_exception=asyncio.CancelledError):
                    await task
            elif kind == "success":
                assert (await task).backend_id == "backend-a"
            else:
                with pytest.raises(expected_exception=TrafficError) as error:
                    await task
                assert "sensitive" not in str(object=error.value)
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

    asyncio.run(main=scenario())


def test_inventory_rejects_arbitrary_endpoints_and_contracts_reject_bad_units() -> None:
    for identifier in (
        "localhost",
        "api",
        "169.254.169.254",
        "backend-a.evil.test",
        "backend-a:8080/path",
    ):
        with pytest.raises(expected_exception=ValueError):
            BackendSpec(id=identifier)
    for value in ("1", True, 0, 1_000_000_001):
        with pytest.raises(expected_exception=ValidationError):
            WorkRequest.model_validate(obj={"request_id": "a", "work_units": value, "mem_mb": 1})
    with pytest.raises(expected_exception=ValidationError):
        WorkRequest(request_id="é" * 65, work_units=1, mem_mb=1)


@pytest.mark.parametrize(
    argnames="service",
    argvalues=["backend-a", "backend-b", "container-controller", "docker-proxy", "resource-guard"],
)
def test_deployment_never_publishes_private_work_or_control_ports(service: str) -> None:
    with pytest.raises(expected_exception=ValueError, match="must not publish"):
        check(
            payload=json.dumps(
                obj={"services": {service: {"ports": [{"host_ip": "127.0.0.1"}]}}}
            ).encode()
        )
