import hashlib
import io
import json
import subprocess
import sys
from importlib.metadata import version
from pathlib import Path

import pytest
from mslb_application.artifacts import ARTIFACT_NAMES, BundleLimits, execute_bundle, verify_bundle
from mslb_application.errors import OperationError
from mslb_application.snapshots import (
    canonical_bytes,
    domain_config,
    make_snapshot,
    read_snapshot,
    verify_snapshot,
)
from mslb_contracts.inputs import InputError, read_requests, read_servers
from mslb_contracts.runs import RunSettings, RunSnapshot, ScalingSettings, TemplateSettings
from mslb_engine import __version__
from mslb_engine.models import Event, SimulationError
from pydantic import ValidationError


def snapshot(settings: RunSettings | None = None, scaling_example: bool = False) -> RunSnapshot:
    with Path("/fixtures/core/servers.json").open(mode="rb") as stream:
        servers = read_servers(stream=stream)
    requests_path = (
        Path("data/examples/scaling-requests.csv")
        if scaling_example
        else Path("/fixtures/core/requests.csv")
    )
    with requests_path.open(mode="rb") as stream:
        requests = read_requests(stream=stream)
    return make_snapshot(servers=servers, requests=requests, settings=settings or RunSettings())


def test_snapshot_defaults_are_frozen_normalized_and_round_trip() -> None:
    saved = snapshot()
    assert saved.configuration.autoscaling.min_servers == 2
    assert saved.configuration.autoscaling.max_servers == 5
    assert (
        make_snapshot(
            servers=tuple(reversed(saved.servers)),
            requests=tuple(reversed(saved.requests)),
            settings=RunSettings(),
        )
        == saved
    )
    assert (
        read_snapshot(
            stream=io.BytesIO(initial_bytes=canonical_bytes(value=saved.model_dump(mode="json")))
        )
        == saved
    )
    assert domain_config(settings=saved.configuration).mode == "sequential"
    assert saved.engine_version == __version__ == version(distribution_name="mslb-engine")


@pytest.mark.parametrize(
    argnames="body",
    argvalues=[
        '{"max_samples":true}',
        '{"max_simulation_ticks":"100"}',
        '{"queue_timeout_ticks":0}',
        '{"strategy":"random"}',
        '{"mode":"docker"}',
        '{"mode":"sequential_compat"}',
        '{"unexpected":1}',
        '{"autoscaling":{"enabled":"true"}}',
        '{"autoscaling":{"window_ticks":10001}}',
        '{"autoscaling":{"scale_out_cpu_threshold":{"numerator":4,"denominator":"5"}}}',
    ],
)
def test_configuration_rejects_ambiguous_or_unsupported_values(body: str) -> None:
    with pytest.raises(expected_exception=ValidationError):
        RunSettings.model_validate_json(json_data=body)


def test_cross_field_scaling_validation_is_shared_with_domain() -> None:
    with pytest.raises(expected_exception=SimulationError, match="template"):
        snapshot(settings=RunSettings(autoscaling=ScalingSettings(enabled=True)))
    with pytest.raises(expected_exception=SimulationError, match="bounds"):
        snapshot(settings=RunSettings(autoscaling=ScalingSettings(max_servers=1)))


def test_snapshot_identity_rejects_tampering_and_unknown_versions() -> None:
    saved = snapshot()
    with pytest.raises(expected_exception=InputError):
        verify_snapshot(snapshot=saved.model_copy(update={"dataset_digest": "0" * 64}))
    with pytest.raises(expected_exception=InputError, match="version"):
        verify_snapshot(snapshot=saved.model_copy(update={"engine_version": "unknown"}))
    raw = saved.model_dump(mode="json")
    raw["schema_version"] = True
    with pytest.raises(expected_exception=InputError):
        read_snapshot(stream=io.BytesIO(initial_bytes=canonical_bytes(value=raw)))


@pytest.mark.parametrize(argnames="mode", argvalues=["concurrent", "sequential"])
def test_complete_bundle_replays_and_repeated_execution_is_identical(
    tmp_path: Path, mode: str
) -> None:
    settings = RunSettings.model_validate_json(json_data=json.dumps(obj={"mode": mode}))
    saved = snapshot(settings=settings)
    destination = tmp_path / "result"
    result = execute_bundle(snapshot=saved, destination=destination)
    assert set(result.artifacts) == ARTIFACT_NAMES
    assert result.independent_replay == "passed"
    assert result.original_validator == ("passed" if mode == "sequential" else "not_applicable")
    assert verify_bundle(directory=destination) == result
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    assert execute_bundle(snapshot=saved, destination=destination) == result
    assert {path.name: path.read_bytes() for path in destination.iterdir()} == before
    assert not list(tmp_path.glob(pattern=".mslb-attempt-*"))


def test_scaled_bundle_has_independently_validated_decisions_and_metrics(tmp_path: Path) -> None:
    settings = RunSettings(
        autoscaling=ScalingSettings(
            enabled=True,
            max_servers=3,
            server_template=TemplateSettings(
                cpu_units_per_tick=10, mem_mb=1024, rate_limit_per_sec=2
            ),
        )
    )
    saved = snapshot(settings=settings, scaling_example=True)
    destination = tmp_path / "scaled"
    result = execute_bundle(snapshot=saved, destination=destination)
    assert result.decision_count > 0 and result.original_validator == "not_applicable"
    assert verify_bundle(directory=destination) == result
    decisions = [
        json.loads(s=line)
        for line in (destination / "autoscaling_decisions.jsonl").read_text().splitlines()
    ]
    assert any(decision["action"] == "scale_out" for decision in decisions)
    metrics = json.loads(s=(destination / "metrics.json").read_text())
    assert metrics["finished"] == 3
    assert "autoscale-000001" in metrics["servers"]


def test_storage_pressure_and_cancellation_never_publish_partial_bundles(tmp_path: Path) -> None:
    for limits in (BundleLimits(artifact_bytes=8), BundleLimits(total_bytes=64)):
        with pytest.raises(expected_exception=OperationError) as failure:
            execute_bundle(snapshot=snapshot(), destination=tmp_path / "failed", limits=limits)
        assert failure.value.code == "STORAGE_LIMIT_EXCEEDED"
        assert not (tmp_path / "failed").exists()
        assert not list(tmp_path.glob(pattern=".mslb-attempt-*"))
    calls = 0

    def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    with pytest.raises(expected_exception=OperationError) as failure:
        execute_bundle(snapshot=snapshot(), destination=tmp_path / "cancelled", cancelled=cancelled)
    assert failure.value.code == "INTERRUPTED" and not (tmp_path / "cancelled").exists()
    assert not list(tmp_path.glob(pattern=".mslb-attempt-*"))


def test_corrupt_producer_fails_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Event.canonical_line

    def corrupt(self: Event) -> str:
        return original(self=self).replace('"server_id":"s1"', '"server_id":"unknown"')

    monkeypatch.setattr(target=Event, name="canonical_line", value=corrupt)
    with pytest.raises(expected_exception=ValueError):
        execute_bundle(snapshot=snapshot(), destination=tmp_path / "corrupt")
    assert list(tmp_path.iterdir()) == []


def test_existing_directory_or_different_snapshot_is_never_overwritten(tmp_path: Path) -> None:
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "user.txt").write_text(data="keep this")
    with pytest.raises(expected_exception=OperationError) as failure:
        execute_bundle(snapshot=snapshot(), destination=unrelated)
    assert failure.value.code == "CONFLICT" and (unrelated / "user.txt").read_text() == "keep this"
    destination = tmp_path / "saved"
    result = execute_bundle(snapshot=snapshot(), destination=destination)
    with pytest.raises(expected_exception=OperationError):
        execute_bundle(
            snapshot=snapshot(settings=RunSettings(strategy="round_robin")), destination=destination
        )
    assert verify_bundle(directory=destination) == result


def test_missing_corrupt_extra_and_traversal_bundle_artifacts_fail(tmp_path: Path) -> None:
    for corruption in ("changed", "missing", "extra", "traversal"):
        destination = tmp_path / corruption
        execute_bundle(snapshot=snapshot(), destination=destination)
        if corruption == "changed":
            (destination / "run.jsonl").write_text(data="{}\n")
        elif corruption == "missing":
            (destination / "metrics.json").unlink()
        elif corruption == "extra":
            (destination / "unexpected.txt").write_text(data="extra")
        else:
            manifest_path = destination / "manifest.json"
            manifest = json.loads(s=manifest_path.read_text())
            manifest["artifacts"]["../outside"] = manifest["artifacts"].pop("run.jsonl")
            manifest_path.write_text(data=json.dumps(obj=manifest))
        with pytest.raises(expected_exception=OperationError):
            verify_bundle(directory=destination)


def test_packaged_original_validator_is_byte_identical() -> None:
    assert (
        hashlib.sha256(data=Path("scripts/validate_run.py").read_bytes()).digest()
        == hashlib.sha256(data=Path("/fixtures/core/validate_run.py").read_bytes()).digest()
    )


def test_evaluator_timeout_is_controlled_and_never_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def timed_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired(cmd="verified evaluator", timeout=60)

    monkeypatch.setattr(target=subprocess, name="run", value=timed_out)
    with pytest.raises(expected_exception=OperationError, match="timed out") as failure:
        execute_bundle(
            snapshot=snapshot(settings=RunSettings(mode="sequential")),
            destination=tmp_path / "timeout",
        )
    assert failure.value.code == "VALIDATION_FAILED"
    assert list(tmp_path.iterdir()) == []


def test_bundle_cli_run_verify_and_safe_error(tmp_path: Path) -> None:
    base = [sys.executable, "scripts/run_bundle.py"]
    destination = tmp_path / "cli"
    run = subprocess.run(  # noqa: S603 -- fixed local CLI and trusted fixture paths
        args=[
            *base,
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
    verify = subprocess.run(  # noqa: S603 -- fixed local CLI and test-owned directory
        args=[*base, "verify", str(object=destination)], capture_output=True, text=True, check=True
    )
    assert json.loads(s=run.stdout) == json.loads(s=verify.stdout)
    failed = subprocess.run(  # noqa: S603 -- intentional missing test-owned artifact
        args=[*base, "verify", str(object=tmp_path / "missing")],
        capture_output=True,
        text=True,
        check=False,
    )
    assert failed.returncode == 2 and "Traceback" not in failed.stderr
    assert json.loads(s=failed.stderr)["error"]["code"] == "VALIDATION_FAILED"
