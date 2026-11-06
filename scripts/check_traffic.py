"""Prove actual private-container routing and bounded CPU work, without daemon access."""

import asyncio
import json
import time
from dataclasses import dataclass, field

from mslb_contracts.docker_runs import DockerSettings
from mslb_contracts.docker_runtime import HttpEvent
from mslb_contracts.inputs import RequestInput
from mslb_contracts.traffic import WorkRequest
from mslb_controller.routing import STRATEGIES, BackendHttp, BackendSpec, Router, TrafficError
from mslb_controller.traffic import TrafficDriver


def require(*, condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


@dataclass(slots=True, kw_only=True)
class SmokeSink:
    events: list[HttpEvent] = field(default_factory=list)

    async def append(self, *, event: HttpEvent) -> None:
        self.events.append(event)
        require(condition=len(self.events) <= 6, message="Traffic smoke event bound exceeded")


async def check_driver(*, http: BackendHttp) -> int:
    completed = 0
    for strategy in STRATEGIES:
        sink = SmokeSink()
        driver = TrafficDriver(
            configuration=DockerSettings(
                strategy=strategy,
                duration_seconds=3,
                backend_inflight=1,
                logical_admission_mem_mb=8,
            ),
            specs=(BackendSpec(id="backend-a"), BackendSpec(id="backend-b")),
            http=http,
            sink=sink,
        )
        summary = await driver.execute(
            requests=(
                RequestInput(t=0, request_id="first", work_units=1, mem_mb=4),
                RequestInput(t=1, request_id="second", work_units=1, mem_mb=4),
            )
        )
        require(
            condition=summary.completed == summary.total == 2
            and summary.failed == 0
            and summary.dropped == summary.unexecuted == 0,
            message="Actual arrival driver failed",
        )
        require(
            condition=any(
                event.kind == "admitted"
                and event.request_id == "second"
                and event.elapsed_seconds >= 1
                for event in sink.events
            ),
            message="Future arrival sent early",
        )
        require(
            condition=all(
                event.result is not None
                and event.result.operations == 2048
                and event.result.checksum != "00000000"
                for event in sink.events
                if event.kind == "completed"
            ),
            message="Actual traffic driver work result is invalid",
        )
        require(
            condition=all(
                state.inflight == state.reserved_mem_mb == 0
                for state in driver.router.states.values()
            ),
            message="Arrival driver reservation leaked",
        )
        completed += summary.completed
    return completed


async def check() -> None:
    http = BackendHttp()
    completed = 0
    try:
        for strategy in sorted(STRATEGIES):
            router = Router(
                specs=(BackendSpec(id="backend-a"), BackendSpec(id="backend-b")),
                strategy=strategy,
                http=http,
            )
            await router.refresh()
            require(
                condition=all(state.healthy for state in router.states.values()),
                message="Private backend health/configuration failed",
            )
            for state in router.states.values():
                measured = state.observed
                require(
                    condition=measured is not None
                    and measured.rss_bytes is not None
                    and measured.cgroup_memory_bytes is not None,
                    message="Backend memory measurements are unavailable",
                )
                if measured is not None and measured.cgroup_memory_bytes is not None:
                    require(
                        condition=measured.cgroup_memory_bytes < 128 * 1024**2,
                        message="Backend exceeded its memory envelope",
                    )
            if strategy in {"first_fit", "round_robin"}:
                destinations = []
                for index in range(4):
                    value = await router.send(
                        request=WorkRequest(
                            request_id=f"{strategy}-{index}", work_units=1, mem_mb=1
                        )
                    )
                    require(
                        condition=value.operations == 2048 and value.checksum != "00000000",
                        message="Backend did not return its CPU-work result",
                    )
                    destinations.append(value.backend_id)
                    completed += 1
                expected = (
                    ["backend-a"] * 4 if strategy == "first_fit" else ["backend-a", "backend-b"] * 2
                )
                require(
                    condition=destinations == expected,
                    message="Actual HTTP routing strategy differs from selection",
                )
            else:
                reservation = router.reserve(
                    request=WorkRequest(request_id=f"{strategy}-busy", work_units=1000, mem_mb=512)
                )
                pending = asyncio.create_task(coro=http.work(reservation=reservation))
                try:
                    observed = False
                    deadline = time.monotonic() + 1
                    while time.monotonic() < deadline:
                        sample = await http.health(spec=reservation.backend.spec)
                        if sample.inflight > 0:
                            observed = True
                            break
                        if pending.done():
                            break
                        await asyncio.sleep(delay=0.005)
                    require(
                        condition=observed, message="Did not observe actual concurrent backend work"
                    )
                    short = await router.send(
                        request=WorkRequest(request_id=f"{strategy}-other", work_units=1, mem_mb=1)
                    )
                    require(
                        condition=short.backend_id == "backend-b",
                        message="Live load did not route to the eligible peer",
                    )
                    long = await pending
                    require(
                        condition=long.operations == 2_000_000,
                        message="Heavy real work did not execute its capped operation count",
                    )
                    completed += 2
                finally:
                    if not pending.done():
                        pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    reservation.release()
            require(
                condition=all(
                    state.inflight == state.reserved_mem_mb == 0 for state in router.states.values()
                ),
                message="Routing reservation leaked",
            )
        driver_completed = await check_driver(http=http)
        print(
            json.dumps(
                obj={
                    "status": "Private Docker HTTP routing verified",
                    "strategies": sorted(STRATEGIES),
                    "actual_completed_calls": completed,
                    "arrival_driver_completed_calls": driver_completed,
                }
            )
        )
    except TrafficError as error:
        raise RuntimeError("Real traffic verification failed: " + error.code) from None
    finally:
        await http.close()


if __name__ == "__main__":
    asyncio.run(main=check())
