"""Memory headroom and hysteresis, independent from logical scheduling and telemetry."""

from dataclasses import dataclass
from datetime import datetime

from mslb_contracts.resources import (
    ResourceDecision,
    ResourceProbe,
    host_memory_fresh,
    host_memory_headroom,
    memory_bounds,
)


@dataclass(slots=True, kw_only=True)
class ResourcePolicy:
    """Evaluate conservative headroom with pressure stopping and recovery hysteresis."""

    held: bool = True
    cool_samples: int = 0
    last_measurement: datetime | None = None
    last_host_measurement: datetime | None = None

    def evaluate(self, *, probe: ResourceProbe | None, now: datetime) -> ResourceDecision:
        """Refuse stale/unsafe measurements and apply recovery hysteresis before admission."""
        if now.utcoffset() is None:
            raise ValueError("Resource evaluation requires an aware server clock")
        caps, reserve, headroom = memory_bounds(probe=probe)
        reason = None
        stop_running = False
        if probe is None:
            reason, stop_running = "MEASUREMENTS_UNAVAILABLE", True
        elif not 0 <= (now - probe.measured_at).total_seconds() <= 3 or (
            self.last_measurement is not None and probe.measured_at < self.last_measurement
        ):
            reason, stop_running = "MEASUREMENTS_STALE", True
        elif probe.engine_memory_bytes < 8 * 1024**3 or caps + reserve > probe.engine_memory_bytes:
            reason, stop_running = "ENGINE_CAPACITY", True
        elif any(
            item.service != "redis" and (not item.healthy or item.container_id is None)
            for item in probe.services
        ):
            reason, stop_running = "MISSING_SERVICE", True
        elif any(
            item.oom_killed or item.working_set_bytes * 100 >= item.limit_bytes * 85
            for item in probe.services
        ):
            reason, stop_running = "RESOURCE_PRESSURE", True
        elif probe.available_memory_bytes < headroom:
            reason, stop_running = "MEMORY_HEADROOM", True
        elif probe.desktop_engine and probe.host_memory is None:
            reason, stop_running = "HOST_MEMORY_UNAVAILABLE", True
        elif not host_memory_fresh(probe=probe, now=now) or (
            probe.desktop_engine
            and probe.host_memory is not None
            and self.last_host_measurement is not None
            and probe.host_memory.measured_at < self.last_host_measurement
        ):
            reason, stop_running = "HOST_MEMORY_STALE", True
        elif (
            probe.desktop_engine
            and probe.host_memory is not None
            and probe.host_memory.available_memory_bytes < host_memory_headroom(probe=probe)
        ):
            reason, stop_running = "HOST_MEMORY_HEADROOM", True
        elif any(item.working_set_bytes * 100 >= item.limit_bytes * 70 for item in probe.services):
            reason = "RESOURCE_PRESSURE"
        if reason is not None:
            self.held, self.cool_samples = True, 0
        elif self.held:
            if (
                probe is not None
                and probe.measured_at != self.last_measurement
                and (
                    not probe.desktop_engine
                    or (
                        probe.host_memory is not None
                        and probe.host_memory.measured_at != self.last_host_measurement
                    )
                )
            ):
                self.cool_samples = (
                    self.cool_samples + 1
                    if all(
                        item.working_set_bytes * 100 < item.limit_bytes * 60
                        for item in probe.services
                    )
                    else 0
                )
            if self.cool_samples >= 3:
                self.held = False
            else:
                reason = "WARMING_UP"
        if (
            probe is not None
            and reason not in {"MEASUREMENTS_STALE", "HOST_MEMORY_STALE"}
            and (self.last_measurement is None or probe.measured_at > self.last_measurement)
        ):
            self.last_measurement = probe.measured_at
        if (
            probe is not None
            and probe.desktop_engine
            and probe.host_memory is not None
            and host_memory_fresh(probe=probe, now=now)
            and reason not in {"MEASUREMENTS_STALE", "HOST_MEMORY_STALE"}
            and (
                self.last_host_measurement is None
                or probe.host_memory.measured_at > self.last_host_measurement
            )
        ):
            self.last_host_measurement = probe.host_memory.measured_at
        return ResourceDecision(
            admit=not self.held,
            stop_running=stop_running,
            reason=reason,
            cap_sum_bytes=caps,
            required_headroom_bytes=headroom,
        )
