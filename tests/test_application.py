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
    with Path("/fixtures/core/servers.json").open("rb") as stream:
        servers = read_servers(stream)
    requests_path = (
        Path("data/examples/scaling-requests.csv")
        if scaling_example
        else Path("/fixtures/core/requests.csv")
    )
    with requests_path.open("rb") as stream:
        requests = read_requests(stream)
    return make_snapshot(servers, requests, settings or RunSettings())


def test_snapshot_defaults_are_frozen_normalized_and_round_trip() -> None:
    saved = snapshot()
    assert saved.configuration.autoscaling.min_servers == 2
    assert saved.configuration.autoscaling.max_servers == 5
    assert (
        make_snapshot(
            tuple(reversed(saved.servers)), tuple(reversed(saved.requests)), RunSettings()
        )
        == saved
    )
    assert read_snapshot(io.BytesIO(canonical_bytes(saved.model_dump(mode="json")))) == saved
    assert domain_config(settings=saved.configuration).mode == "sequential"
    assert saved.engine_version == __version__ == version("mslb-engine")


@pytest.mark.parametrize(
    "body",
    [
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
    with pytest.raises(ValidationError):
        RunSettings.model_validate_json(body)


def test_cross_field_scaling_validation_is_shared_with_domain() -> None:
    with pytest.raises(SimulationError, match="template"):
        snapshot(RunSettings(autoscaling=ScalingSettings(enabled=True)))
    with pytest.raises(SimulationError, match="bounds"):
        snapshot(RunSettings(autoscaling=ScalingSettings(max_servers=1)))


def test_snapshot_identity_rejects_tampering_and_unknown_versions() -> None:
    saved = snapshot()
    with pytest.raises(InputError):
        verify_snapshot(saved.model_copy(update={"dataset_digest": "0" * 64}))
    with pytest.raises(InputError, match="version"):
        verify_snapshot(saved.model_copy(update={"engine_version": "unknown"}))
    raw = saved.model_dump(mode="json")
    raw["schema_version"] = True
    with pytest.raises(InputError):
        read_snapshot(io.BytesIO(canonical_bytes(raw)))


@pytest.mark.parametrize("mode", ["concurrent", "sequential"])
def test_complete_bundle_replays_and_repeated_execution_is_identical(
    tmp_path: Path, mode: str
) -> None:
    settings = RunSettings.model_validate_json(json.dumps({"mode": mode}))
    saved = snapshot(settings)
    destination = tmp_path / "result"
    result = execute_bundle(saved, destination)
    assert set(result.artifacts) == ARTIFACT_NAMES
    assert result.independent_replay == "passed"
    assert result.original_validator == ("passed" if mode == "sequential" else "not_applicable")
    assert verify_bundle(destination) == result
    before = {path.name: path.read_bytes() for path in destination.iterdir()}
    assert execute_bundle(saved, destination) == result
    assert {path.name: path.read_bytes() for path in destination.iterdir()} == before
    assert not list(tmp_path.glob(".mslb-attempt-*"))


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
    saved = snapshot(settings, scaling_example=True)
    destination = tmp_path / "scaled"
    result = execute_bundle(saved, destination)
    assert result.decision_count > 0 and result.original_validator == "not_applicable"
    assert verify_bundle(destination) == result
    decisions = [
        json.loads(line)
        for line in (destination / "autoscaling_decisions.jsonl").read_text().splitlines()
    ]
    assert any(decision["action"] == "scale_out" for decision in decisions)
    metrics = json.loads((destination / "metrics.json").read_text())
    assert metrics["finished"] == 3
    assert "autoscale-000001" in metrics["servers"]


def test_storage_pressure_and_cancellation_never_publish_partial_bundles(tmp_path: Path) -> None:
    for limits in (BundleLimits(artifact_bytes=8), BundleLimits(total_bytes=64)):
        with pytest.raises(OperationError) as failure:
            execute_bundle(snapshot(), tmp_path / "failed", limits)
        assert failure.value.code == "STORAGE_LIMIT_EXCEEDED"
        assert not (tmp_path / "failed").exists()
        assert not list(tmp_path.glob(".mslb-attempt-*"))
    calls = 0

    def cancelled() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    with pytest.raises(OperationError) as failure:
        execute_bundle(snapshot(), tmp_path / "cancelled", cancelled=cancelled)
    assert failure.value.code == "INTERRUPTED" and not (tmp_path / "cancelled").exists()
    assert not list(tmp_path.glob(".mslb-attempt-*"))


def test_corrupt_producer_fails_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original = Event.canonical_line

    def corrupt(self: Event) -> str:
        return original(self).replace('"server_id":"s1"', '"server_id":"unknown"')

    monkeypatch.setattr(Event, "canonical_line", corrupt)
    with pytest.raises(ValueError):
        execute_bundle(snapshot(), tmp_path / "corrupt")
    assert list(tmp_path.iterdir()) == []


def test_existing_directory_or_different_snapshot_is_never_overwritten(tmp_path: Path) -> None:
    unrelated = tmp_path / "unrelated"
    unrelated.mkdir()
    (unrelated / "user.txt").write_text("keep this")
    with pytest.raises(OperationError) as failure:
        execute_bundle(snapshot(), unrelated)
    assert failure.value.code == "CONFLICT" and (unrelated / "user.txt").read_text() == "keep this"
    destination = tmp_path / "saved"
    result = execute_bundle(snapshot(), destination)
    with pytest.raises(OperationError):
        execute_bundle(snapshot(RunSettings(strategy="round_robin")), destination)
    assert verify_bundle(destination) == result


def test_missing_corrupt_extra_and_traversal_bundle_artifacts_fail(tmp_path: Path) -> None:
    for corruption in ("changed", "missing", "extra", "traversal"):
        destination = tmp_path / corruption
        execute_bundle(snapshot(), destination)
        if corruption == "changed":
            (destination / "run.jsonl").write_text("{}\n")
        elif corruption == "missing":
            (destination / "metrics.json").unlink()
        elif corruption == "extra":
            (destination / "unexpected.txt").write_text("extra")
        else:
            manifest_path = destination / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["artifacts"]["../outside"] = manifest["artifacts"].pop("run.jsonl")
            manifest_path.write_text(json.dumps(manifest))
        with pytest.raises(OperationError):
            verify_bundle(destination)


def test_packaged_original_validator_is_byte_identical() -> None:
    assert (
        hashlib.sha256(Path("scripts/validate_run.py").read_bytes()).digest()
        == hashlib.sha256(Path("/fixtures/core/validate_run.py").read_bytes()).digest()
    )


def test_evaluator_timeout_is_controlled_and_never_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def timed_out(*args: object, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
        raise subprocess.TimeoutExpired("verified evaluator", 60)

    monkeypatch.setattr(subprocess, "run", timed_out)
    with pytest.raises(OperationError, match="timed out") as failure:
        execute_bundle(snapshot(RunSettings(mode="sequential")), tmp_path / "timeout")
    assert failure.value.code == "VALIDATION_FAILED"
    assert list(tmp_path.iterdir()) == []


def test_bundle_cli_run_verify_and_safe_error(tmp_path: Path) -> None:
    base = [sys.executable, "scripts/run_bundle.py"]
    destination = tmp_path / "cli"
    run = subprocess.run(  # noqa: S603 -- fixed local CLI and trusted fixture paths
        [
            *base,
            "run",
            "--servers",
            "/fixtures/core/servers.json",
            "--requests",
            "/fixtures/core/requests.csv",
            "--output",
            str(destination),
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    verify = subprocess.run(  # noqa: S603 -- fixed local CLI and test-owned directory
        [*base, "verify", str(destination)], capture_output=True, text=True, check=True
    )
    assert json.loads(run.stdout) == json.loads(verify.stdout)
    failed = subprocess.run(  # noqa: S603 -- intentional missing test-owned artifact
        [*base, "verify", str(tmp_path / "missing")], capture_output=True, text=True, check=False
    )
    assert failed.returncode == 2 and "Traceback" not in failed.stderr
    assert json.loads(failed.stderr)["error"]["code"] == "VALIDATION_FAILED"
