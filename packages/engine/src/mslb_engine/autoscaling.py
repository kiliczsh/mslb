"""Deterministic scaling policy evaluated only from completed exact samples."""

from collections import deque
from dataclasses import dataclass
from fractions import Fraction

from mslb_engine.errors import SimulationError
from mslb_engine.event_models import ScaleDecision
from mslb_engine.input_models import Server
from mslb_engine.run_config import AutoscalingPolicy


@dataclass(frozen=True, slots=True)
class Observation:
    ending_tick: int
    cpu_utilization: Fraction
    drops: int
    drop_ratio: Fraction


class Autoscaler:
    def __init__(
        self, policy: AutoscalingPolicy, initial_ids: frozenset[str], bit_limit: int
    ) -> None:
        self.policy = policy
        self.initial_ids = initial_ids
        self.bit_limit = bit_limit
        self.history: deque[Observation] = deque(maxlen=policy.window_ticks)
        self.next_eligible_tick = 0
        self.counter = 0
        self.pending: ScaleDecision | None = None

    def _bounded(self, value: Fraction) -> Fraction:
        if max(value.numerator.bit_length(), value.denominator.bit_length()) > self.bit_limit:
            raise SimulationError(
                "RESOURCE_LIMIT_EXCEEDED", "Scaling arithmetic complexity exceeded"
            )
        return value

    def observe(
        self, ending_tick: int, consumed: Fraction, capacity: int, drops: int, arrivals: int
    ) -> None:
        if self.policy.enabled:
            self.history.append(
                Observation(
                    ending_tick,
                    self._bounded(consumed / capacity),
                    drops,
                    self._bounded(Fraction(drops, max(1, arrivals))),
                )
            )

    def decide(
        self, tick: int, servers: tuple[Server, ...], idle_ids: frozenset[str], queue_depth: int
    ) -> ScaleDecision | None:
        """Select at most one action; activation is deferred to the next boundary."""
        if not self.policy.enabled or tick < self.next_eligible_tick:
            return None
        samples = len(self.history)
        cpu_sum, drop_sum = Fraction(), Fraction()
        for sample in self.history:
            cpu_sum = self._bounded(cpu_sum + sample.cpu_utilization)
            drop_sum = self._bounded(drop_sum + sample.drop_ratio)
        cpu_mean = self._bounded(cpu_sum / samples) if samples else Fraction()
        drop_mean = self._bounded(drop_sum / samples) if samples else Fraction()
        triggers: list[str] = []
        if cpu_mean >= self.policy.scale_out_cpu_threshold:
            triggers.append("cpu_mean")
        if queue_depth and queue_depth >= self.policy.scale_out_queue_threshold:
            triggers.append("queue_depth")
        if drop_mean >= self.policy.scale_out_drop_rate_threshold:
            triggers.append("drop_rate_mean")
        maximum, minimum = self.policy.max_servers, self.policy.min_servers
        if maximum is None or minimum is None:
            raise SimulationError("INVALID_STATE", "Scaling policy defaults must be resolved")
        added: tuple[str, ...] = ()
        removed: tuple[str, ...] = ()
        action, reason = "none", "not_eligible"
        if triggers:
            count = min(self.policy.scale_out_step, maximum - len(servers))
            if count:
                added = tuple(
                    f"autoscale-{index:06d}"
                    for index in range(self.counter + 1, self.counter + count + 1)
                )
                self.counter += count
                action, reason = "scale_out", None
            else:
                reason = "bounds"
        else:
            idle_dynamic = sorted(idle_ids - self.initial_ids, reverse=True)
            if (
                cpu_mean <= self.policy.scale_in_cpu_threshold
                and queue_depth == 0
                and not any(sample.drops for sample in self.history)
                and idle_dynamic
            ):
                triggers = [
                    "cpu_mean_low",
                    "queue_empty",
                    "no_recent_drops",
                    "removable_idle_server",
                ]
                count = min(
                    self.policy.scale_in_step,
                    len(servers) - max(minimum, len(self.initial_ids)),
                    len(idle_dynamic),
                )
                if count:
                    removed = tuple(idle_dynamic[:count])
                    action, reason = "scale_in", None
                else:
                    reason = "bounds"
        decision = ScaleDecision(
            tick,
            action,
            tuple(triggers),
            tick + 1 if added or removed else None,
            added,
            removed,
            reason,
        )
        if added or removed:
            self.next_eligible_tick = tick + self.policy.cooldown_ticks + 1
            self.pending = decision
        return decision

    def activate(
        self, tick: int, servers: tuple[Server, ...], idle_ids: frozenset[str]
    ) -> tuple[Server, ...]:
        if self.pending is None:
            return servers
        change = self.pending
        if change.applied_at != tick:
            raise SimulationError("INVALID_STATE", "Scaling activation boundary mismatch")
        if not set(change.server_ids_removed).issubset(idle_ids - self.initial_ids):
            raise SimulationError("INVALID_STATE", "Attempted to remove a busy or original server")
        members = {server.id: server for server in servers}
        for sid in change.server_ids_removed:
            del members[sid]
        template = self.policy.server_template
        for sid in change.server_ids_added:
            if sid in members or template is None:
                raise SimulationError(
                    "INVALID_STATE", "Scaling identity collision or missing template"
                )
            members[sid] = Server(
                sid, template.cpu_units_per_tick, template.mem_mb, template.rate_limit_per_sec
            )
        self.pending = None
        return tuple(sorted(members.values(), key=lambda server: server.id))
