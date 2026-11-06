"""Independent policy reconstruction; does not import the runtime autoscaler."""

import json
import re
from collections import deque
from fractions import Fraction
from typing import BinaryIO

from mslb_engine.input_models import Server
from mslb_engine.replay_errors import ReplayError
from mslb_engine.run_config import AutoscalingPolicy


class ScalingReplay:
    """Compute expected sidecar records and membership from policy and trace state."""

    def __init__(
        self,
        stream: BinaryIO | None,
        policy: AutoscalingPolicy,
        initial_servers: tuple[Server, ...],
        bit_limit: int,
        max_series: int,
    ) -> None:
        self.stream = stream
        self.policy = policy
        self.bit_limit = bit_limit
        self.max_series = max_series
        self.original = frozenset(server.id for server in initial_servers)
        if policy.enabled:
            if stream is None:
                raise ReplayError("Enabled scaling requires its decision sidecar")
            if any(
                re.fullmatch(pattern=r"autoscale-[0-9]{6,}", string=sid) for sid in self.original
            ):
                raise ReplayError("Original server collides with generated namespace")
        self.observations: deque[tuple[Fraction, Fraction, int]] = deque(maxlen=policy.window_ticks)
        self.available_at = 0
        self.serial = 0
        self.pending_added: tuple[str, ...] = ()
        self.pending_removed: tuple[str, ...] = ()
        self.pending_at: int | None = None

    def _check_size(self, value: Fraction) -> Fraction:
        if max(value.numerator.bit_length(), value.denominator.bit_length()) > self.bit_limit:
            raise ReplayError("Scaling arithmetic exceeds its configured complexity")
        return value

    def activate(
        self,
        tick: int,
        members: dict[str, Server],
        running: dict[str, dict[str, Fraction]],
        memory: dict[str, int],
    ) -> None:
        """Apply only the independently checked membership change at its boundary."""
        if self.pending_at is None:
            return
        if self.pending_at != tick:
            raise ReplayError("Pending membership activation has the wrong tick")
        for sid in self.pending_removed:
            if sid in self.original or running[sid] or memory[sid]:
                raise ReplayError("Policy removes original or busy capacity")
            del members[sid], running[sid], memory[sid]
        template = self.policy.server_template
        for sid in self.pending_added:
            if template is None or sid in members:
                raise ReplayError("Invalid scale template or generated identity collision")
            members[sid] = Server(
                id=sid,
                cpu_units_per_tick=template.cpu_units_per_tick,
                mem_mb=template.mem_mb,
                rate_limit_per_sec=template.rate_limit_per_sec,
            )
            running[sid], memory[sid] = {}, 0
        self.pending_added, self.pending_removed, self.pending_at = (), (), None

    def check_decision(
        self,
        tick: int,
        members: dict[str, Server],
        running: dict[str, dict[str, Fraction]],
        pending_count: int,
    ) -> None:
        """Recompute exact policy evidence and reject mismatched scaling decisions."""
        if not self.policy.enabled or tick < self.available_at:
            return
        cpu_sum, error_sum = Fraction(), Fraction()
        for cpu, ratio, _ in self.observations:
            cpu_sum = self._check_size(value=cpu_sum + cpu)
            error_sum = self._check_size(value=error_sum + ratio)
        divisor = len(self.observations)
        cpu = self._check_size(value=cpu_sum / divisor) if divisor else Fraction()
        ratio = self._check_size(value=error_sum / divisor) if divisor else Fraction()
        causes = []
        if cpu >= self.policy.scale_out_cpu_threshold:
            causes.append("cpu_mean")
        if pending_count > 0 and pending_count >= self.policy.scale_out_queue_threshold:
            causes.append("queue_depth")
        if ratio >= self.policy.scale_out_drop_rate_threshold:
            causes.append("drop_rate_mean")
        upper, lower = self.policy.max_servers, self.policy.min_servers
        if upper is None or lower is None:
            raise ReplayError("Unresolved scaling policy")
        added, removed = (), ()
        action, reason = "none", "not_eligible"
        if causes:
            size = min(self.policy.scale_out_step, upper - len(members))
            if size > 0:
                added = tuple(
                    f"autoscale-{number:06d}"
                    for number in range(self.serial + 1, self.serial + size + 1)
                )
                self.serial += size
                if self.serial + len(self.original) > self.max_series:
                    raise ReplayError("Logical server history exceeds its configured budget")
                action, reason = "scale_out", None
            else:
                reason = "bounds"
        elif (
            cpu <= self.policy.scale_in_cpu_threshold
            and pending_count == 0
            and sum(drops for _, _, drops in self.observations) == 0
        ):
            candidates = sorted(
                (sid for sid in members if sid not in self.original and not running[sid]),
                reverse=True,
            )
            if candidates:
                causes = ["cpu_mean_low", "queue_empty", "no_recent_drops", "removable_idle_server"]
                size = min(
                    len(candidates),
                    self.policy.scale_in_step,
                    len(members) - max(lower, len(self.original)),
                )
                if size > 0:
                    removed = tuple(candidates[:size])
                    action, reason = "scale_in", None
                else:
                    reason = "bounds"
        applied_at = tick + 1 if added or removed else None
        expected = {
            "t": tick,
            "action": action,
            "trigger": causes,
            "applied_at": applied_at,
            "server_ids_added": list(added),
            "server_ids_removed": list(removed),
            "reason": reason,
        }
        if self.stream is None:
            raise ReplayError("Missing scaling sidecar")
        raw = self.stream.readline(16385)
        canonical = (
            json.dumps(obj=expected, ensure_ascii=False, separators=(",", ":")) + "\n"
        ).encode()
        if raw != canonical:
            raise ReplayError(
                f"Missing, noncanonical, or policy-invalid scaling decision at tick {tick}"
            )
        if applied_at is not None:
            self.pending_added, self.pending_removed, self.pending_at = added, removed, applied_at
            self.available_at = tick + self.policy.cooldown_ticks + 1

    def observe(self, consumed: Fraction, capacity: int, drops: int, arrivals: int) -> None:
        if self.policy.enabled:
            self.observations.append(
                (
                    self._check_size(value=consumed / capacity),
                    self._check_size(value=Fraction(numerator=drops, denominator=max(1, arrivals))),
                    drops,
                )
            )

    def finish(self) -> None:
        if self.pending_at is not None or (self.stream is not None and self.stream.read(1)):
            raise ReplayError("Unapplied activation or trailing scaling sidecar data")
