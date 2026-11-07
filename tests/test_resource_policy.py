"""Fail-closed resource admission, conservative headroom and non-flapping thresholds."""

from datetime import UTC, datetime, timedelta

import pytest
from mslb_application.resource_policy import ResourcePolicy
from mslb_contracts.resources import (
    MIB,
    SERVICE_LIMITS_MB,
    HostMemoryEvidence,
    ResourceProbe,
    ServiceMemory,
    host_memory_headroom,
    memory_bounds,
    parse_resource_guard,
    safe_to_admit,
)
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
            for index, (name, limit) in enumerate(iterable=SERVICE_LIMITS_MB.items())
        ),
    )


def warm(*, policy: ResourcePolicy) -> None:
    for second in range(3):
        value = probe(second=second)
        result = policy.evaluate(probe=value, now=value.measured_at)
        assert result.admit is (second == 2)


def desktop_probe(*, second: int = 0, host_second: int | None = None) -> ResourceProbe:
    return probe(second=second).model_copy(
        update={
            "desktop_engine": True,
            "host_memory": HostMemoryEvidence(
                measured_at=EPOCH
                + timedelta(seconds=second if host_second is None else host_second),
                total_memory_bytes=32 * 1024**3,
                available_memory_bytes=10 * 1024**3,
            ),
        }
    )


def test_desktop_requires_distinct_host_samples_and_independent_host_headroom() -> None:
    policy = ResourcePolicy()
    for second in range(3):
        value = desktop_probe(second=second, host_second=0)
        assert not policy.evaluate(probe=value, now=value.measured_at).admit
    for second in (3, 4):
        value = desktop_probe(second=second)
        decision = policy.evaluate(probe=value, now=value.measured_at)
    assert decision.admit and decision.cap_sum_bytes == 3712 * MIB
    assert safe_to_admit(probe=value)
    assert value.host_memory is not None
    required = host_memory_headroom(probe=value)
    for offset, expected in ((-1, "HOST_MEMORY_HEADROOM"), (0, "WARMING_UP")):
        current = value.model_copy(
            update={
                "host_memory": value.host_memory.model_copy(
                    update={"available_memory_bytes": required + offset}
                )
            }
        )
        decision = ResourcePolicy().evaluate(probe=current, now=current.measured_at)
        assert decision.reason == expected
        assert safe_to_admit(probe=current) is (offset == 0)


@pytest.mark.parametrize(argnames="host_second", argvalues=[-4, 1])
def test_host_age_is_independent_from_fresh_vm_evidence(host_second: int) -> None:
    value = desktop_probe(host_second=host_second)
    decision = ResourcePolicy().evaluate(probe=value, now=value.measured_at)
    assert decision.reason == "HOST_MEMORY_STALE" and decision.stop_running
    assert not safe_to_admit(probe=value)


def test_future_host_clock_cannot_poison_valid_host_recovery() -> None:
    policy = ResourcePolicy()
    forged = desktop_probe(host_second=1000)
    assert policy.evaluate(probe=forged, now=EPOCH).reason == "HOST_MEMORY_STALE"
    assert policy.last_measurement is None and policy.last_host_measurement is None
    for second in range(3):
        value = desktop_probe(second=second)
        assert policy.evaluate(probe=value, now=value.measured_at).admit is (second == 2)


def test_host_sampler_may_finish_after_vm_sampler_but_not_after_reader_clock() -> None:
    value = desktop_probe(host_second=1)
    assert not safe_to_admit(probe=value)
    assert safe_to_admit(probe=value, now=EPOCH + timedelta(seconds=1))


def test_desktop_vm_evidence_cannot_replace_host_headroom() -> None:
    policy = ResourcePolicy()
    for second in range(4):
        value = probe(second=second).model_copy(update={"desktop_engine": True})
        result = policy.evaluate(probe=value, now=value.measured_at)
        assert not result.admit and result.stop_running
        assert result.reason == "HOST_MEMORY_UNAVAILABLE"


def test_admission_needs_three_distinct_cool_samples_and_counts_future_growth() -> None:
    policy = ResourcePolicy()
    initial = probe()
    first = policy.evaluate(probe=initial, now=EPOCH)
    for _ in range(10):
        assert not policy.evaluate(probe=initial, now=EPOCH).admit
    assert first.reason == "WARMING_UP" and first.cap_sum_bytes == 3648 * MIB
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
        {"measured_at": datetime(year=2026, month=11, day=6, tzinfo=UTC).replace(tzinfo=None)},
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


@pytest.mark.parametrize(argnames="absent", argvalues=(False, True))
def test_optional_redis_outage_keeps_full_budget_and_durable_admission(absent: bool) -> None:
    policy = ResourcePolicy()
    baseline = memory_bounds(probe=probe())[0]
    value = probe()
    for second in range(3):
        value = probe(second=second)
        services = tuple(
            item.model_copy(
                update={
                    "healthy": False,
                    "container_id": None,
                    "usage_bytes": 0,
                    "working_set_bytes": 0,
                }
            )
            if absent and item.service == "redis"
            else item.model_copy(update={"healthy": False})
            if item.service == "redis"
            else item
            for item in value.services
        )
        value = value.model_copy(update={"services": services})
        decision = policy.evaluate(probe=value, now=value.measured_at)
        assert decision.admit is (second == 2) and not decision.stop_running
        assert decision.cap_sum_bytes == baseline
        assert safe_to_admit(probe=value)
    pressured = value.model_copy(
        update={
            "services": tuple(
                item.model_copy(update={"oom_killed": True}) if item.service == "redis" else item
                for item in value.services
            )
        }
    )
    assert policy.evaluate(probe=pressured, now=pressured.measured_at).stop_running
    assert not safe_to_admit(probe=pressured)


@pytest.mark.parametrize(argnames="value", argvalues=("", "TRUE", "1", "false "))
def test_resource_consumer_flags_reject_ambiguous_configuration(value: str) -> None:
    with pytest.raises(expected_exception=ValueError, match="RESOURCE_GUARD_ENABLED"):
        parse_resource_guard(value=value)
    assert parse_resource_guard(value="true")
    assert not parse_resource_guard(value="false")
