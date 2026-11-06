"""Prove actual private-container routing and bounded CPU work, without daemon access."""

import asyncio
import json
import time

from mslb_contracts.traffic import WorkRequest
from mslb_controller.routing import STRATEGIES, BackendHttp, BackendSpec, Router, TrafficError


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


async def check() -> None:
    http = BackendHttp()
    completed = 0
    try:
        for strategy in sorted(STRATEGIES):
            router = Router((BackendSpec("backend-a"), BackendSpec("backend-b")), strategy, http)
            await router.refresh()
            require(
                all(state.healthy for state in router.states.values()),
                "Private backend health/configuration failed",
            )
            for state in router.states.values():
                measured = state.observed
                require(
                    measured is not None
                    and measured.rss_bytes is not None
                    and measured.cgroup_memory_bytes is not None,
                    "Backend memory measurements are unavailable",
                )
                if measured is not None and measured.cgroup_memory_bytes is not None:
                    require(
                        measured.cgroup_memory_bytes < 128 * 1024**2,
                        "Backend exceeded its memory envelope",
                    )
            if strategy in {"first_fit", "round_robin"}:
                destinations = []
                for index in range(4):
                    value = await router.send(
                        WorkRequest(request_id=f"{strategy}-{index}", work_units=1, mem_mb=1)
                    )
                    require(
                        value.operations == 2048 and value.checksum != "00000000",
                        "Backend did not return its CPU-work result",
                    )
                    destinations.append(value.backend_id)
                    completed += 1
                expected = (
                    ["backend-a"] * 4 if strategy == "first_fit" else ["backend-a", "backend-b"] * 2
                )
                require(
                    destinations == expected, "Actual HTTP routing strategy differs from selection"
                )
            else:
                reservation = router.reserve(
                    WorkRequest(request_id=f"{strategy}-busy", work_units=1000, mem_mb=512)
                )
                pending = asyncio.create_task(http.work(reservation))
                try:
                    observed = False
                    deadline = time.monotonic() + 1
                    while time.monotonic() < deadline:
                        sample = await http.health(reservation.backend.spec)
                        if sample.inflight > 0:
                            observed = True
                            break
                        if pending.done():
                            break
                        await asyncio.sleep(0.005)
                    require(observed, "Did not observe actual concurrent backend work")
                    short = await router.send(
                        WorkRequest(request_id=f"{strategy}-other", work_units=1, mem_mb=1)
                    )
                    require(
                        short.backend_id == "backend-b",
                        "Live load did not route to the eligible peer",
                    )
                    long = await pending
                    require(
                        long.operations == 2_000_000,
                        "Heavy real work did not execute its capped operation count",
                    )
                    completed += 2
                finally:
                    if not pending.done():
                        pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    reservation.release()
            require(
                all(
                    state.inflight == state.reserved_mem_mb == 0 for state in router.states.values()
                ),
                "Routing reservation leaked",
            )
        print(
            json.dumps(
                {
                    "status": "Private Docker HTTP routing verified",
                    "strategies": sorted(STRATEGIES),
                    "actual_completed_calls": completed,
                }
            )
        )
    except TrafficError as error:
        raise RuntimeError("Real traffic verification failed: " + error.code) from None
    finally:
        await http.close()


if __name__ == "__main__":
    asyncio.run(check())
