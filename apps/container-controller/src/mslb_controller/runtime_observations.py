"""Bounded actual health samples, sampled peaks and explicit CPU counter windows."""

from datetime import UTC, datetime
from uuid import UUID

from mslb_contracts.docker_artifacts import DockerProgress
from mslb_contracts.docker_observations import BackendMeasurements, BackendSample
from mslb_contracts.traffic import BackendHealth

from mslb_controller.errors import TrafficError
from mslb_controller.router import Router
from mslb_controller.routing_models import BackendSpec


def peak(*, first: int | None, second: int | None) -> int | None:
    values = [value for value in (first, second) if value is not None]
    return max(values) if values else None


def difference(*, first: int | None, last: int | None, reset: bool) -> int | None:
    return None if reset or first is None or last is None else last - first


class RuntimeObservations:
    """Keep at most64 immutable resource windows and the latest routing observations."""

    def __init__(self) -> None:
        self.windows: dict[UUID, BackendMeasurements] = {}
        self.latest: dict[str, BackendSample | None] = {}

    def sample(self, *, spec: BackendSpec, health: BackendHealth | None) -> None:
        """Record actual counters only; never infer missing observations or reuse an identity."""
        if spec.resource_id is None or spec.container_id is None:
            return
        if spec.id not in self.latest and len(self.latest) >= 64:
            raise TrafficError(code="RESOURCE_LIMIT_EXCEEDED")
        if health is None:
            self.latest[spec.id] = None
            return
        if health.backend_id != spec.id:
            raise TrafficError(code="INVALID_BACKEND")
        sample = BackendSample(
            resource_id=spec.resource_id,
            container_id=spec.container_id,
            observed_at=datetime.now(tz=UTC),
            health=health,
        )
        previous = self.windows.get(spec.resource_id)
        if previous is None:
            if len(self.windows) >= 64:
                raise TrafficError(code="RESOURCE_LIMIT_EXCEEDED")
            current = BackendMeasurements(
                first=sample,
                latest=sample,
                samples=1,
                sampled_peak_rss_bytes=health.rss_bytes,
                sampled_peak_cgroup_memory_bytes=health.cgroup_memory_bytes,
                cpu_counter_reset=False,
                throttling_counter_reset=False,
                cpu_delta_usec=None,
                throttled_delta_usec=None,
            )
        else:
            if previous.samples >= 1000:
                raise TrafficError(code="RESOURCE_LIMIT_EXCEEDED")
            if (
                previous.first.container_id != spec.container_id
                or previous.first.health.backend_id != spec.id
            ):
                raise TrafficError(code="INVALID_BACKEND")
            old = previous.latest.health
            cpu_reset = previous.cpu_counter_reset or (
                old.cpu_usage_usec is not None
                and health.cpu_usage_usec is not None
                and health.cpu_usage_usec < old.cpu_usage_usec
            )
            throttle_reset = previous.throttling_counter_reset or (
                old.throttled_usec is not None
                and health.throttled_usec is not None
                and health.throttled_usec < old.throttled_usec
            )
            current = BackendMeasurements(
                first=previous.first,
                latest=sample,
                samples=previous.samples + 1,
                sampled_peak_rss_bytes=peak(
                    first=previous.sampled_peak_rss_bytes, second=health.rss_bytes
                ),
                sampled_peak_cgroup_memory_bytes=peak(
                    first=previous.sampled_peak_cgroup_memory_bytes,
                    second=health.cgroup_memory_bytes,
                ),
                cpu_counter_reset=cpu_reset,
                throttling_counter_reset=throttle_reset,
                cpu_delta_usec=difference(
                    first=previous.first.health.cpu_usage_usec,
                    last=health.cpu_usage_usec,
                    reset=cpu_reset,
                ),
                throttled_delta_usec=difference(
                    first=previous.first.health.throttled_usec,
                    last=health.throttled_usec,
                    reset=throttle_reset,
                ),
            )
        self.windows[spec.resource_id], self.latest[spec.id] = current, sample

    def measurements(self) -> tuple[BackendMeasurements, ...]:
        """Return deterministic resource ordering without claiming deterministic runtime values."""
        return tuple(self.windows[key] for key in sorted(self.windows))

    def progress(
        self,
        *,
        router: Router,
        pending: int,
        elapsed: float,
        run_id: UUID,
        fence: int,
        snapshot_digest: str,
    ) -> DockerProgress:
        """Report current owned reservations and only fresh actual backend observations."""
        now = datetime.now(tz=UTC)
        samples: list[BackendSample] = []
        unavailable: list[str] = []
        for name in router.states:
            latest = self.latest.get(name)
            if latest is None or not 0 <= (now - latest.observed_at).total_seconds() <= 3:
                unavailable.append(name)
            else:
                samples.append(latest)
        return DockerProgress(
            run_id=run_id,
            fence=fence,
            snapshot_digest=snapshot_digest,
            observed_at=now,
            elapsed_seconds=elapsed,
            pending=pending,
            inflight=sum(state.inflight for state in router.states.values()),
            backends=tuple(samples),
            unavailable_backends=tuple(unavailable),
        )
