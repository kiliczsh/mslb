"""Measured windows, missing/reset counters and recreated immutable ownership."""

from dataclasses import replace
from uuid import uuid4

import pytest
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import RuntimeRecord
from mslb_contracts.docker_observations import BackendMeasurements
from mslb_contracts.traffic import BackendHealth
from mslb_controller.routing_models import BackendSpec
from mslb_controller.runtime_observations import RuntimeObservations
from pydantic import ValidationError


def health(
    *, cpu: int | None = 100, throttled: int | None = 10, rss: int | None = 20
) -> BackendHealth:
    return BackendHealth(
        backend_id="backend-a",
        ready=True,
        logical_admission_mem_mb=1024,
        max_inflight=4,
        inflight=1,
        reserved_mem_mb=50,
        rss_bytes=rss,
        cgroup_memory_bytes=100,
        cpu_usage_usec=cpu,
        throttled_usec=throttled,
    )


def test_actual_windows_keep_peaks_reset_and_recreation_distinct() -> None:
    recorder = RuntimeObservations()
    spec = BackendSpec(id="backend-a", resource_id=uuid4(), container_id="a" * 64)
    recorder.sample(spec=spec, health=health())
    assert recorder.measurements()[0].cpu_delta_usec is None
    recorder.sample(spec=spec, health=health(cpu=350, throttled=30, rss=80))
    value = recorder.measurements()[0]
    assert value.cpu_delta_usec == 250 and value.throttled_delta_usec == 20
    assert value.sampled_peak_rss_bytes == 80 and value.latest.health.reserved_mem_mb == 50
    forged = value.model_dump(mode="json")
    forged["cpu_delta_usec"] = 251
    with pytest.raises(expected_exception=ValidationError):
        BackendMeasurements.model_validate_json(json_data=canonical_bytes(value=forged))
    recorder.sample(spec=spec, health=health(cpu=5, throttled=1, rss=10))
    value = recorder.measurements()[0]
    assert value.cpu_counter_reset and value.throttling_counter_reset
    assert value.cpu_delta_usec is None and value.throttled_delta_usec is None
    recreated = replace(spec, resource_id=uuid4(), container_id="b" * 64)
    recorder.sample(spec=recreated, health=health(cpu=7))
    recorder.sample(spec=recreated, health=health(cpu=20))
    assert recreated.resource_id is not None and spec.resource_id is not None
    windows = {item.first.resource_id: item for item in recorder.measurements()}
    assert len(windows) == 2 and windows[recreated.resource_id].cpu_delta_usec == 13
    assert windows[spec.resource_id].cpu_delta_usec is None


def test_missing_counters_stay_unavailable_and_old_runtime_bytes_remain_canonical() -> None:
    recorder = RuntimeObservations()
    spec = BackendSpec(id="backend-a", resource_id=uuid4(), container_id="a" * 64)
    recorder.sample(spec=spec, health=health(cpu=None, throttled=None, rss=None))
    recorder.sample(spec=spec, health=health(cpu=9, throttled=2, rss=12))
    value = recorder.measurements()[0]
    assert value.cpu_delta_usec is None and value.throttled_delta_usec is None
    from datetime import UTC, datetime

    from mslb_contracts.docker_runtime import TrafficSummary

    runtime = RuntimeRecord(
        run_id=uuid4(),
        fence=1,
        snapshot_digest="c" * 64,
        observed_at=datetime.now(tz=UTC),
        lifecycle_events=1,
        traffic=TrafficSummary(
            status="completed",
            stop_reason=None,
            total=1,
            completed=1,
            failed=0,
            dropped=0,
            unexecuted=0,
            elapsed_seconds=1.0,
            peak_pending=1,
            peak_inflight=1,
        ),
    )
    saved = canonical_bytes(value=runtime.model_dump(mode="json"))
    assert b'"observations"' not in saved and b'"schema_version":1' in saved
    assert (
        canonical_bytes(
            value=RuntimeRecord.model_validate_json(json_data=saved).model_dump(mode="json")
        )
        == saved
    )
