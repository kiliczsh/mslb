"""Bounded real-time membership policy over persisted, inspected backend ownership."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from mslb_contracts.docker_settings import DockerSettings
from mslb_contracts.traffic import BackendHealth

from mslb_controller.errors import GrowthHeld
from mslb_controller.router import Router
from mslb_controller.routing_models import BackendSpec
from mslb_controller.traffic_protocols import RetireBackend


@dataclass(frozen=True, slots=True, kw_only=True)
class ReadyBackend:
    """Carry a provisioned backend only after its actual matching health observation."""

    spec: BackendSpec
    observed: BackendHealth


@dataclass(slots=True, kw_only=True)
class PhysicalScaling:
    """Sample real seconds without overlapping lifecycle calls or logical tick semantics."""

    settings: DockerSettings
    provision: Callable[[], Awaitable[ReadyBackend]]
    retire: RetireBackend
    clock: Callable[[], float]
    last_sample: float | None = field(default=None, init=False)
    changed_at: float | None = field(default=None, init=False)
    pending_samples: int = field(default=0, init=False)
    idle_samples: dict[str, int] = field(default_factory=dict, init=False)

    async def evaluate(self, *, router: Router, pending: int) -> None:
        """Grow after sustained pending work; retire only an owned idle dynamic member."""
        policy = self.settings.physical_scaling
        now = self.clock()
        if not policy.enabled or (
            self.last_sample is not None and now - self.last_sample < policy.evaluation_seconds
        ):
            return
        self.last_sample = now
        self.pending_samples = (
            min(self.pending_samples + 1, policy.pending_samples) if pending else 0
        )
        self.idle_samples = {
            name: min(self.idle_samples.get(name, 0) + 1, policy.idle_samples)
            if not pending
            and state.inflight == 0
            and state.healthy
            and state.observed is not None
            and state.observed.inflight == 0
            else 0
            for name, state in router.states.items()
        }
        if self.changed_at is not None and now - self.changed_at < policy.cooldown_seconds:
            return
        if (
            self.pending_samples >= policy.pending_samples
            and len(router.states) < self.settings.max_replicas
        ):
            try:
                ready = await self.provision()
            except GrowthHeld:
                # Keep the sample cadence and all existing reservations. Hard pressure,
                # stale ownership and uncertain cleanup are different failures.
                return
            router.add(spec=ready.spec, observed=ready.observed)
            self.changed_at, self.pending_samples = self.clock(), 0
            return
        if len(router.states) <= self.settings.min_replicas:
            return
        candidates = [
            name
            for name, count in self.idle_samples.items()
            if name.startswith("mslb-") and count >= policy.idle_samples
        ]
        if candidates:
            name = max(candidates)
            state = router.states[name]
            # No await can admit a request between the idle test and routing exclusion.
            state.draining = True
            await self.retire(backend_id=name)
            router.remove(backend_id=name)
            self.idle_samples.pop(name)
            self.changed_at = self.clock()
