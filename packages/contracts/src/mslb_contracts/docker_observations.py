"""Bounded measured backend samples and explicit counter-window summaries."""

from typing import Annotated, Self
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from mslb_contracts.docker_base import DockerId, DockerModel
from mslb_contracts.traffic import BackendHealth


class BackendSample(DockerModel):
    """Bind one actual HTTP health observation to its owned immutable container."""

    resource_id: UUID
    container_id: DockerId
    observed_at: AwareDatetime
    health: BackendHealth

    @model_validator(mode="after")
    def utc(self) -> Self:
        offset = self.observed_at.utcoffset()
        if offset is None or offset.total_seconds() != 0:
            raise ValueError("Backend observations require UTC")
        return self


class BackendMeasurements(DockerModel):
    """Report sampled peaks and measured counter deltas, never unsampled runtime maxima."""

    first: BackendSample
    latest: BackendSample
    samples: Annotated[int, Field(ge=1, le=1000)]
    sampled_peak_rss_bytes: Annotated[int, Field(ge=0)] | None
    sampled_peak_cgroup_memory_bytes: Annotated[int, Field(ge=0)] | None
    cpu_counter_reset: bool
    throttling_counter_reset: bool
    cpu_delta_usec: Annotated[int, Field(ge=0)] | None
    throttled_delta_usec: Annotated[int, Field(ge=0)] | None

    @model_validator(mode="after")
    def accounting(self) -> Self:
        first, latest = self.first, self.latest
        if (first.resource_id, first.container_id, first.health.backend_id) != (
            latest.resource_id,
            latest.container_id,
            latest.health.backend_id,
        ) or first.observed_at > latest.observed_at:
            raise ValueError("Observation window changes backend identity or time order")
        if self.samples == 1 and (
            first != latest or self.cpu_counter_reset or self.throttling_counter_reset
        ):
            raise ValueError("A single observation cannot contain another sample or counter reset")
        for actual, start, end, reset in (
            (
                self.cpu_delta_usec,
                first.health.cpu_usage_usec,
                latest.health.cpu_usage_usec,
                self.cpu_counter_reset,
            ),
            (
                self.throttled_delta_usec,
                first.health.throttled_usec,
                latest.health.throttled_usec,
                self.throttling_counter_reset,
            ),
        ):
            expected = (
                None if self.samples < 2 or reset or start is None or end is None else end - start
            )
            if expected is not None and expected < 0:
                raise ValueError("Counter regression requires an explicit reset")
            if actual != expected:
                raise ValueError("Counter delta differs from its observed window")
        for peak, values in (
            (self.sampled_peak_rss_bytes, (first.health.rss_bytes, latest.health.rss_bytes)),
            (
                self.sampled_peak_cgroup_memory_bytes,
                (first.health.cgroup_memory_bytes, latest.health.cgroup_memory_bytes),
            ),
        ):
            known = [value for value in values if value is not None]
            if self.samples <= 2 and peak != (max(known) if known else None):
                raise ValueError("Sampled peak differs from the complete short window")
            if any(value is not None and (peak is None or value > peak) for value in values):
                raise ValueError("Sampled memory peak is below a measured value")
        return self
