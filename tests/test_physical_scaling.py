"""Real-second hysteresis, reservation-safe membership and owned idle scale-in."""

import asyncio

import pytest
from mslb_contracts.docker_settings import DockerSettings
from mslb_contracts.traffic import WorkRequest
from mslb_controller.errors import GrowthHeld, TrafficError
from mslb_controller.physical_resources import initial_backends
from mslb_controller.physical_scaling import PhysicalScaling, ReadyBackend
from mslb_controller.router import Router
from mslb_controller.routing_models import BackendSpec

from tests.test_traffic_driver import FakeClock, Transport


def test_distinct_samples_cooldown_and_idle_dynamic_retirement_preserve_reservations() -> None:
    async def scenario() -> None:
        clock, http = FakeClock(), Transport()
        settings = DockerSettings.model_validate(
            obj={"initial_replicas": 1, "max_replicas": 2, "physical_scaling": {"enabled": True}}
        )
        router = Router(
            specs=(BackendSpec(id="backend-a"),), strategy="round_robin", http=http, clock=clock.now
        )
        provisioned, retired = [], []

        async def provision() -> ReadyBackend:
            spec = BackendSpec(id="mslb-" + "a" * 32 + "-0")
            provisioned.append(spec.id)
            return ReadyBackend(spec=spec, observed=await http.health(spec=spec))

        async def retire(*, backend_id: str) -> None:
            assert router.states[backend_id].draining
            assert router.states[backend_id].inflight == 0
            retired.append(backend_id)

        policy = PhysicalScaling(
            settings=settings, provision=provision, retire=retire, clock=clock.now
        )
        try:
            await router.refresh()
            old_state = router.states["backend-a"]
            reservation = router.reserve(
                request=WorkRequest(request_id="old", work_units=1, mem_mb=1)
            )
            await policy.evaluate(router=router, pending=2)
            for _ in range(10):
                await policy.evaluate(router=router, pending=2)
            assert not provisioned
            clock.current += 1
            await policy.evaluate(router=router, pending=2)
            assert len(provisioned) == 1 and len(router.states) == 2
            assert router.states["backend-a"] is old_state and old_state.inflight == 1
            reservation.release()
            dynamic = router.states[provisioned[0]]
            dynamic.inflight = 1
            for _ in range(6):
                clock.current += 1
                await policy.evaluate(router=router, pending=0)
            assert not retired
            dynamic.inflight = 0
            for index in range(5):
                clock.current += 1
                await policy.evaluate(router=router, pending=0)
                assert bool(retired) is (index == 4)
            assert retired == provisioned and tuple(router.states) == ("backend-a",)
        finally:
            await http.close()

    asyncio.run(main=scenario())


def test_failed_retirement_keeps_member_excluded_and_holds_capacity() -> None:
    async def scenario() -> None:
        clock, http = FakeClock(), Transport()
        dynamic = BackendSpec(id="mslb-" + "b" * 32 + "-0")
        router = Router(
            specs=(BackendSpec(id="backend-a"), dynamic),
            strategy="first_fit",
            http=http,
            clock=clock.now,
        )

        async def provision() -> ReadyBackend:
            raise RuntimeError("Unexpected growth")

        async def retire(*, backend_id: str) -> None:
            raise RuntimeError("Cleanup unresolved")

        policy = PhysicalScaling(
            settings=DockerSettings.model_validate(obj={"physical_scaling": {"enabled": True}}),
            provision=provision,
            retire=retire,
            clock=clock.now,
        )
        try:
            await router.refresh()
            for _ in range(4):
                await policy.evaluate(router=router, pending=0)
                clock.current += 1
            with pytest.raises(expected_exception=RuntimeError, match="Cleanup unresolved"):
                await policy.evaluate(router=router, pending=0)
            assert len(router.states) == 2 and router.states[dynamic.id].draining
            reservation = router.reserve(
                request=WorkRequest(request_id="next", work_units=1, mem_mb=1)
            )
            assert reservation.backend.spec.id == "backend-a"
            reservation.release()
        finally:
            await http.close()

    asyncio.run(main=scenario())


def test_growth_hold_preserves_existing_traffic_and_hard_stop_still_propagates() -> None:
    async def scenario() -> None:
        clock, http = FakeClock(), Transport()
        router = Router(
            specs=(BackendSpec(id="backend-a"),), strategy="first_fit", http=http, clock=clock.now
        )
        hard = False

        async def provision() -> ReadyBackend:
            if hard:
                raise TrafficError(code="RESOURCE_PRESSURE")
            raise GrowthHeld

        async def retire(*, backend_id: str) -> None:
            raise RuntimeError("Unexpected retirement")

        policy = PhysicalScaling(
            settings=DockerSettings.model_validate(
                obj={"initial_replicas": 1, "physical_scaling": {"enabled": True}}
            ),
            provision=provision,
            retire=retire,
            clock=clock.now,
        )
        try:
            await router.refresh()
            reservation = router.reserve(
                request=WorkRequest(request_id="active", work_units=1, mem_mb=1)
            )
            for _ in range(5):
                await policy.evaluate(router=router, pending=2)
                clock.current += 1
            assert len(router.states) == 1 and reservation.backend.inflight == 1
            reservation.release()
            next_request = router.reserve(
                request=WorkRequest(request_id="later", work_units=1, mem_mb=1)
            )
            next_request.release()
            hard = True
            with pytest.raises(expected_exception=TrafficError):
                await policy.evaluate(router=router, pending=2)
        finally:
            await http.close()

    asyncio.run(main=scenario())


def test_initial_membership_waits_for_soft_holds_and_never_swallows_hard_stop() -> None:
    async def scenario() -> None:
        http = Transport()
        calls = 0
        spec = BackendSpec(id="backend-a")

        async def provision() -> ReadyBackend:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise GrowthHeld
            return ReadyBackend(spec=spec, observed=await http.health(spec=spec))

        def check() -> None:
            if calls > 3:
                raise TrafficError(code="RESOURCE_PRESSURE")

        try:
            result = await initial_backends(count=1, provision=provision, check=check)
            assert len(result) == 1 and calls == 3 and result[0].spec == spec
            calls = 4
            with pytest.raises(expected_exception=TrafficError):
                await initial_backends(count=1, provision=provision, check=check)
            assert calls == 4
        finally:
            await http.close()

    asyncio.run(main=scenario())
