"""Explicit stress capacity preserves default rejection, frozen identity and recovery."""

import io
from pathlib import Path
from typing import BinaryIO

import pytest
from mslb_api.settings import Settings
from mslb_application.artifacts import execute_bundle, verify_bundle
from mslb_application.checkpoints import PREFIX_NAMES, read_checkpoint, write_checkpoint
from mslb_application.snapshots import domain_config, domain_inputs, make_snapshot
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.inputs import InputError, RequestInput, ServerInput, read_requests
from mslb_contracts.runs import (
    RunSettings,
    RunSnapshot,
    StressRunSettings,
    settings_for_profile,
)
from mslb_engine.errors import SimulationError
from mslb_engine.metric_accumulator import MetricAccumulator
from mslb_engine.metric_values import distribution
from mslb_engine.run_config import RunConfig
from mslb_engine.simulator import Simulation
from pydantic import ValidationError

from tests.unit.application.test_checkpoints import append, contents


def large_snapshot(
    *, count: int = 10_001, identifier_prefix: str = "r", server_id: str = "s"
) -> RunSnapshot:
    """Exercise one oversized boundary, exact metrics and pending completions cheaply."""
    return make_snapshot(
        servers=(
            ServerInput(
                id=server_id, cpu_units_per_tick=100_000, mem_mb=100_000, rate_limit_per_sec=100_000
            ),
        ),
        requests=tuple(
            RequestInput(t=0, request_id=f"{identifier_prefix}{index:05d}", work_units=1, mem_mb=1)
            for index in range(count)
        ),
        settings=settings_for_profile(settings=RunSettings(mode="concurrent"), profile="stress"),
    )


@pytest.mark.parametrize(
    argnames=("folder", "count"),
    argvalues=[("20_sustained_12k", 12_000), ("21_stress_50k", 50_000)],
)
def test_expanded_upload_populations_require_explicit_capacity(*, folder: str, count: int) -> None:
    path = Path("/fixtures/expanded/scenarios") / folder / "requests.csv"
    with (
        path.open(mode="rb") as source,
        pytest.raises(expected_exception=InputError, match="request_count_exceeded"),
    ):
        read_requests(stream=source)
    with path.open(mode="rb") as source:
        assert len(read_requests(stream=source, max_requests=50_000)) == count


def test_clients_cannot_grant_stress_bounds_or_reinterpret_standard_snapshots() -> None:
    for value in ({"max_requests": 50_000}, {"capacity_profile": "stress"}):
        with pytest.raises(expected_exception=ValidationError):
            RunSettings.model_validate(obj=value)
    with pytest.raises(expected_exception=ValueError, match="isolated installation"):
        Settings(capacity_profile="stress")
    with pytest.raises(expected_exception=SimulationError, match="capacity"):
        RunConfig(max_requests=10_001)
    saved = large_snapshot()
    assert isinstance(saved.configuration, StressRunSettings)
    raw = saved.model_dump(mode="json")
    raw["configuration"] = RunSettings(mode="concurrent").model_dump(mode="json")
    with pytest.raises(expected_exception=ValidationError, match="frozen capacity"):
        RunSnapshot.model_validate_json(json_data=canonical_bytes(value=raw))
    standard = make_snapshot(
        servers=saved.servers, requests=saved.requests[:1], settings=RunSettings()
    )
    assert "max_requests" not in standard.configuration.model_dump()
    assert "capacity_profile" not in standard.configuration.model_dump()
    with pytest.raises(expected_exception=SimulationError, match="latency"):
        distribution(values=[1] * 10_001)
    assert distribution(values=[1] * 10_001, max_values=50_000).count == 10_001


def test_stress_checkpoint_restores_more_than_ten_thousand_pending_completions(
    *, tmp_path: Path
) -> None:
    saved = large_snapshot()
    servers, requests = domain_inputs(snapshot=saved)
    config = domain_config(settings=saved.configuration)
    simulation = Simulation(servers=servers, requests=requests, config=config)
    metrics = MetricAccumulator(
        requests=requests, servers=servers, max_requests=config.max_requests
    )
    streams: dict[str, BinaryIO] = {name: io.BytesIO() for name in PREFIX_NAMES}
    append(frame=next(simulation.iter_ticks()), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    with artifact.path.open(mode="rb") as source:
        restored = read_checkpoint(
            stream=source, expected_digest=artifact.sha256, snapshot=saved, prefixes=streams
        )
    assert restored.metrics.max_requests == 50_000
    for frame in restored.simulation.iter_ticks():
        append(frame=frame, metrics=restored.metrics, streams=streams)
    assert restored.metrics.summary()["finished"] == 10_001
    assert len(contents(streams=streams)["run.jsonl"].splitlines()) == 30_003


def test_stress_bundle_verifies_inputs_replay_metrics_and_repeatability(*, tmp_path: Path) -> None:
    saved = large_snapshot()
    destination = tmp_path / "stress-bundle"
    first = execute_bundle(snapshot=saved, destination=destination)
    assert first.event_count == 30_003 and first.original_validator == "not_applicable"
    assert verify_bundle(directory=destination) == first
    assert (
        execute_bundle(snapshot=saved, destination=destination).bundle_digest == first.bundle_digest
    )


def test_full_stress_checkpoint_uses_serialized_collections_within_unchanged_byte_cap(
    *, tmp_path: Path
) -> None:
    saved = large_snapshot(count=50_000, identifier_prefix="stress-0", server_id="stress-00")
    servers, requests = domain_inputs(snapshot=saved)
    config = domain_config(settings=saved.configuration)
    simulation = Simulation(servers=servers, requests=requests, config=config)
    metrics = MetricAccumulator(
        requests=requests, servers=servers, max_requests=config.max_requests
    )
    streams: dict[str, BinaryIO] = {name: io.BytesIO() for name in PREFIX_NAMES}
    append(frame=next(simulation.iter_ticks()), metrics=metrics, streams=streams)
    artifact = write_checkpoint(
        directory=tmp_path, snapshot=saved, simulation=simulation, metrics=metrics, prefixes=streams
    )
    assert artifact.size_bytes < 32 * 1024**2
    with artifact.path.open(mode="rb") as source:
        restored = read_checkpoint(
            stream=source, expected_digest=artifact.sha256, snapshot=saved, prefixes=streams
        )
    assert len(restored.simulation.active["stress-00"]) == 50_000
