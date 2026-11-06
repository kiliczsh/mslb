"""Execution defaults and immutable pre-rename records cross the same boundaries."""

import io
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from mslb_application.artifacts import execute_bundle, verify_bundle
from mslb_application.snapshots import domain_config, make_snapshot, read_snapshot
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.http import SimulationCreate
from mslb_contracts.inputs import read_requests, read_servers
from mslb_contracts.mcp import CompareStrategiesArguments, RunCommand
from mslb_contracts.runs import LegacyRunSettings, RunSettings, RunSnapshot, public_settings
from mslb_engine.models import Request, RunConfig, Server
from mslb_engine.simulator import Simulation
from mslb_persistence.models import Job, Run
from mslb_persistence.reads import run_view


def legacy_snapshot() -> RunSnapshot:
    with Path("/fixtures/core/servers.json").open(mode="rb") as stream:
        servers = read_servers(stream=stream)
    with Path("/fixtures/core/requests.csv").open(mode="rb") as stream:
        requests = read_requests(stream=stream)
    return make_snapshot(
        servers=servers, requests=requests, settings=LegacyRunSettings(mode="sequential_compat")
    )


def test_omitted_mode_is_sequential_in_engine_api_and_mcp() -> None:
    dataset_id = uuid4()
    assert RunConfig().mode == RunSettings().mode == "sequential"
    assert SimulationCreate(dataset_id=dataset_id).mode == "sequential"
    assert (
        RunCommand(simulation=SimulationCreate(dataset_id=dataset_id)).simulation.mode
        == "sequential"
    )
    assert CompareStrategiesArguments(dataset_id=dataset_id).mode == "sequential"
    frames = tuple(
        Simulation(
            servers=(Server(id="s", cpu_units_per_tick=10, mem_mb=100, rate_limit_per_sec=10),),
            requests=tuple(
                Request(t=0, request_id=rid, work_units=10, mem_mb=1) for rid in ("a", "b")
            ),
            config=RunConfig(),
        ).iter_ticks()
    )
    starts = [
        (event.request_id, event.t)
        for frame in frames
        for event in frame.events
        if event.event == "REQUEST_STARTED"
    ]
    assert starts == [("a", 0), ("b", 1)]


def test_archived_snapshot_keeps_its_original_digest_and_bundle_bytes(tmp_path: Path) -> None:
    saved = legacy_snapshot()
    # Identity of the package generated before the public mode was renamed.
    assert (
        saved.snapshot_digest == "fcfee0aabf32462cf99d19f8986f6063b84a2e0158f47f59be2d2392f807e584"
    )
    original = canonical_bytes(value=saved.model_dump(mode="json"))
    assert read_snapshot(stream=io.BytesIO(initial_bytes=original)) == saved
    assert domain_config(settings=saved.configuration).mode == "sequential"
    destination = tmp_path / "legacy"
    manifest = execute_bundle(snapshot=saved, destination=destination)
    assert manifest.original_validator == "passed"
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    assert verify_bundle(directory=destination) == manifest
    assert {path.name: path.read_bytes() for path in destination.iterdir()} == before
    current = make_snapshot(servers=saved.servers, requests=saved.requests, settings=RunSettings())
    assert current.snapshot_digest != saved.snapshot_digest
    execute_bundle(snapshot=current, destination=tmp_path / "current")
    assert (tmp_path / "current/run.jsonl").read_bytes() == before["run.jsonl"]


def test_legacy_history_projects_current_mode_without_mutating_storage() -> None:
    saved = legacy_snapshot()
    row = Run(
        id=uuid4(),
        dataset_id=uuid4(),
        snapshot_digest=saved.snapshot_digest,
        dataset_digest=saved.dataset_digest,
        server_snapshot_digest=saved.server_snapshot_digest,
        engine_version=saved.engine_version,
        configuration=saved.configuration.model_dump(mode="json"),
        created_at=datetime.now(tz=UTC),
    )
    job = Job(status="queued", attempts=0, completed_at=None, error_code=None)
    before = canonical_bytes(value=row.configuration)
    view = run_view(run=row, job=job, progress=None)
    assert view.configuration == public_settings(settings=saved.configuration)
    assert view.configuration.mode == "sequential" and view.snapshot_digest == saved.snapshot_digest
    assert canonical_bytes(value=row.configuration) == before


@pytest.mark.parametrize(argnames="mode", argvalues=[None, "sequential", "concurrent"])
def test_cli_defaults_and_explicit_modes_generate_and_replay(
    tmp_path: Path, mode: str | None
) -> None:
    output = tmp_path / "run.jsonl"
    arguments = [
        "--servers",
        "/fixtures/core/servers.json",
        "--requests",
        "/fixtures/core/requests.csv",
    ]
    mode_arguments = ["--mode", mode] if mode is not None else []
    generated = subprocess.run(  # noqa: S603 -- fixed interpreter, bounded trusted fixture inputs
        args=[
            sys.executable,
            "scripts/generate_example.py",
            *arguments,
            "--output",
            str(object=output),
            *mode_arguments,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert generated.stderr == ""
    validated = subprocess.run(  # noqa: S603 -- fixed interpreter and the generated local trace
        args=[
            sys.executable,
            "scripts/validate_concurrent.py",
            *arguments,
            "--run",
            str(object=output),
            *mode_arguments,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert validated.stderr == ""
    assert json.loads(s=validated.stdout)["terminal_tick"] == (6 if mode == "concurrent" else 4)


def test_bundle_cli_without_configuration_uses_sequential(tmp_path: Path) -> None:
    destination = tmp_path / "bundle"
    result = subprocess.run(  # noqa: S603 -- fixed interpreter and trusted fixture paths
        args=[
            sys.executable,
            "scripts/run_bundle.py",
            "run",
            "--servers",
            "/fixtures/core/servers.json",
            "--requests",
            "/fixtures/core/requests.csv",
            "--output",
            str(object=destination),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stderr == ""
    assert json.loads(s=result.stdout)["original_validator"] == "passed"
    with (destination / "snapshot.json").open(mode="rb") as stream:
        assert read_snapshot(stream=stream).configuration.mode == "sequential"
