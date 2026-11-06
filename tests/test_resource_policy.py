"""Fail-closed resource admission, conservative headroom and non-flapping thresholds."""

from datetime import UTC, datetime, timedelta

import pytest
from mslb_application.resource_policy import ResourcePolicy
from mslb_contracts.resources import MIB, SERVICE_LIMITS_MB, ResourceProbe, ServiceMemory
from pydantic import ValidationError

EPOCH = datetime(year=2026, month=11, day=6, tzinfo=UTC)


def probe(*, second: int = 0, percent: int = 10) -> ResourceProbe:
    return ResourceProbe(
        measured_at=EPOCH + timedelta(seconds=second),
        engine_memory_bytes=8 * 1024**3,
        available_memory_bytes=7 * 1024**3,
        desktop_engine=False,
        services=tuple(
            ServiceMemory(
                service=name,
                container_id=f"{index + 1:064x}",
                limit_bytes=limit * MIB,
                usage_bytes=(limit * MIB * percent + 99) // 100,
                working_set_bytes=(limit * MIB * percent + 99) // 100,
                healthy=True,
                oom_killed=False,
                restart_count=0,
            )
            for index, (name, limit) in enumerate(SERVICE_LIMITS_MB.items())
        ),
    )


def warm(*, policy: ResourcePolicy) -> None:
    for second in range(3):
        value = probe(second=second)
        result = policy.evaluate(probe=value, now=value.measured_at)
        assert result.admit is (second == 2)


def test_admission_needs_three_distinct_cool_samples_and_counts_future_growth() -> None:
    policy = ResourcePolicy()
    initial = probe()
    first = policy.evaluate(probe=initial, now=EPOCH)
    for _ in range(10):
        assert not policy.evaluate(probe=initial, now=EPOCH).admit
    assert first.reason == "WARMING_UP" and first.cap_sum_bytes == 3584 * MIB
    warm(policy=ResourcePolicy())
    for second in (1, 2):
        value = probe(second=second)
        decision = policy.evaluate(probe=value, now=value.measured_at)
    assert decision.admit and decision.required_headroom_bytes > 2 * 1024**3


def test_pressure_hysteresis_refuses_at_seventy_stops_at_eighty_five_and_resumes_below_sixty() -> (
    None
):
    policy = ResourcePolicy()
    warm(policy=policy)
    for second, percent, reason, stop in (
        (3, 70, "RESOURCE_PRESSURE", False),
        (4, 85, "RESOURCE_PRESSURE", True),
        (5, 60, "WARMING_UP", False),
    ):
        value = probe(second=second, percent=percent)
        decision = policy.evaluate(probe=value, now=value.measured_at)
        assert not decision.admit and decision.reason == reason and decision.stop_running is stop
    for second in (6, 7, 8):
        value = probe(second=second, percent=59)
        decision = policy.evaluate(probe=value, now=value.measured_at)
        assert decision.admit is (second == 8)


@pytest.mark.parametrize(
    argnames="fault",
    argvalues=["missing", "stale", "future", "capacity", "headroom", "service", "oom"],
)
def test_missing_stale_unsafe_or_incomplete_measurements_fail_closed(fault: str) -> None:
    policy = ResourcePolicy()
    warm(policy=policy)
    value: ResourceProbe | None = probe(second=3)
    now = EPOCH + timedelta(seconds=3)
    if fault == "missing":
        value = None
    elif fault == "stale":
        now += timedelta(seconds=4)
    elif fault == "future":
        value = value.model_copy(update={"measured_at": now + timedelta(seconds=1)})
    elif fault == "capacity":
        value = value.model_copy(
            update={"engine_memory_bytes": 4 * 1024**3, "available_memory_bytes": 3 * 1024**3}
        )
    elif fault == "headroom":
        value = value.model_copy(update={"available_memory_bytes": 1024**3})
    elif fault == "service":
        value = value.model_copy(
            update={
                "services": (
                    value.services[0].model_copy(update={"healthy": False}),
                    *value.services[1:],
                )
            }
        )
    else:
        value = value.model_copy(
            update={
                "services": (
                    value.services[0].model_copy(update={"oom_killed": True}),
                    *value.services[1:],
                )
            }
        )
    decision = policy.evaluate(probe=value, now=now)
    assert not decision.admit and decision.stop_running and decision.reason != "WARMING_UP"


def test_model_rejects_inconsistent_memory_identity_and_unbounded_topology() -> None:
    value = probe()
    for changes in (
        {"services": value.services[:-1]},
        {"services": (value.services[0], *value.services[1:-1], value.services[0])},
        {"available_memory_bytes": value.engine_memory_bytes + 1},
        {"measured_at": datetime(year=2026, month=11, day=6)},
    ):
        with pytest.raises(expected_exception=ValidationError):
            ResourceProbe.model_validate(obj=value.model_dump() | changes)


def test_future_measurement_cannot_poison_later_valid_recovery() -> None:
    policy = ResourcePolicy()
    warm(policy=policy)
    forged = probe(second=1000)
    assert (
        policy.evaluate(probe=forged, now=EPOCH + timedelta(seconds=3)).reason
        == "MEASUREMENTS_STALE"
    )
    for second in (3, 4, 5):
        current = probe(second=second)
        decision = policy.evaluate(probe=current, now=current.measured_at)
        assert decision.admit is (second == 5)
