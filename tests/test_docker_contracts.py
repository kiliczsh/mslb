import json
from uuid import uuid4

import pytest
from mslb_contracts.docker_runs import (
    CreationIntent,
    DeploymentSpec,
    DockerManifest,
    DockerSettings,
    DockerSnapshot,
    PhysicalScaling,
    StaticBackend,
)
from mslb_contracts.traffic import BackendHealth
from pydantic import ValidationError


def test_docker_settings_are_distinct_strict_and_bounded() -> None:
    settings = DockerSettings()
    assert settings.mode == "docker" and settings.initial_replicas == 2
    assert settings.physical_scaling.cooldown_seconds == 3
    for field, value in (
        ("mode", "concurrent"),
        ("initial_replicas", 5),
        ("max_replicas", 1),
        ("global_inflight", 17),
        ("duration_seconds", 121),
        ("backend_memory_mb", 129),
        ("backend_nano_cpus", 500_000_001),
        ("backend_template", "arbitrary-image"),
        ("max_pending", 257),
        ("starts_per_backend_second", 33),
        ("backend_inflight", "4"),
        ("autoscaling", {"enabled": True}),
        ("image", "attacker/image"),
    ):
        with pytest.raises(ValidationError):
            DockerSettings.model_validate({field: value})


def test_snapshot_rejects_arrivals_outside_real_runtime_and_changes_identity_with_provenance() -> (
    None
):
    body = dict(
        dataset_id=uuid4(),
        dataset_digest="a" * 64,
        request_count=2,
        latest_arrival=1,
        deployment_id=uuid4(),
        deployment_digest="b" * 64,
        configuration=DockerSettings(duration_seconds=2),
    )
    snapshot = DockerSnapshot.model_validate(body)
    assert (
        DockerSnapshot.model_validate_json(snapshot.model_dump_json()).digest() == snapshot.digest()
    )
    with pytest.raises(ValidationError):
        DockerSnapshot.model_validate({**body, "latest_arrival": 2})
    for invalid in (True, 1.0, "1"):
        with pytest.raises(ValidationError):
            DockerSnapshot.model_validate(snapshot.model_dump() | {"schema_version": invalid})
    assert snapshot.model_copy(update={"deployment_id": uuid4()}).digest() != snapshot.digest()


def test_creation_body_has_only_approved_security_resources_network_and_identity() -> None:
    intent = CreationIntent(
        deployment_id=uuid4(),
        run_id=uuid4(),
        intent_id=uuid4(),
        creator_fence=7,
        slot=2,
        image_id="sha256:" + "a" * 64,
        network_id="b" * 64,
        logical_admission_mem_mb=1024,
        max_inflight=4,
        memory_mb=128,
        nano_cpus=500_000_000,
    )
    body = intent.daemon_body()
    assert body["Image"] == intent.image_id and body["User"] == "1000:1000"
    assert body["Entrypoint"] == ["python", "-m", "mslb_mock_server"]
    assert body["HostConfig"] == {
        "Memory": 128 * 1024**2,
        "MemorySwap": 128 * 1024**2,
        "NanoCpus": 500_000_000,
        "PidsLimit": 32,
        "ReadonlyRootfs": True,
        "CapDrop": ["ALL"],
        "SecurityOpt": ["no-new-privileges:true"],
        "NetworkMode": "b" * 64,
        "PublishAllPorts": False,
        "PortBindings": {},
        "Tmpfs": {"/run/mslb-tmp": "rw,noexec,nosuid,nodev,size=16777216,uid=1000,gid=1000"},
        "LogConfig": {
            "Type": "json-file",
            "Config": {
                "max-size": "5m",
                "max-file": "2",
                "mode": "non-blocking",
                "max-buffer-size": "1m",
            },
        },
    }
    assert "Privileged" not in body and "Binds" not in body
    assert intent.name == f"mslb-{intent.intent_id.hex}-2"
    raw = intent.model_dump(mode="json")
    for field in ("command", "host_mount", "privileged", "labels", "devices"):
        with pytest.raises(ValidationError):
            CreationIntent.model_validate_json(json.dumps(raw | {field: "arbitrary"}))


def test_deployment_and_artifact_namespaces_cannot_alias_logical_output() -> None:
    a = StaticBackend(name="backend-a", container_id="a" * 64)
    b = StaticBackend(name="backend-b", container_id="b" * 64)
    assert DeploymentSpec(
        image_id="sha256:" + "c" * 64, network_id="d" * 64, static=(a, b)
    ).digest()
    with pytest.raises(expected_exception=ValidationError, match="project"):
        DeploymentSpec.model_validate(
            obj={
                "project": "medsien-load-balancer",
                "image_id": "sha256:" + "c" * 64,
                "network_id": "d" * 64,
                "static": [a.model_dump(), b.model_dump()],
            }
        )
    with pytest.raises(ValidationError):
        DeploymentSpec(image_id="sha256:" + "c" * 64, network_id="d" * 64, static=(b, a))
    identifier = uuid4()
    files = {
        name: {"key": f"docker/{identifier.hex}/3/{name}", "sha256": "e" * 64, "size_bytes": 1}
        for name in ("http_events.jsonl", "runtime_summary.json", "container_lifecycle.jsonl")
    }
    data = dict(
        run_id=str(identifier),
        publication_fence=3,
        snapshot_digest="f" * 64,
        total=2,
        completed=1,
        failed=1,
        dropped=0,
        unexecuted=0,
        artifacts=files,
    )
    valid = DockerManifest.model_validate_json(json.dumps(data))
    assert valid.total == 2
    for invalid in (True, 1.0, "1"):
        with pytest.raises(ValidationError):
            DockerManifest.model_validate_json(json.dumps(data | {"schema_version": invalid}))
    for filename in ("run.jsonl", "metrics.json", "../runtime_summary.json"):
        invalid = {name: value for name, value in files.items() if name != "runtime_summary.json"}
        invalid[filename] = {
            **files["runtime_summary.json"],
            "key": f"docker/{identifier.hex}/3/{filename}",
        }
        with pytest.raises(ValidationError):
            DockerManifest.model_validate_json(json.dumps(data | {"artifacts": invalid}))
    files["http_events.jsonl"]["key"] = "runs/logical/run.jsonl"
    with pytest.raises(ValidationError):
        DockerManifest.model_validate_json(json.dumps(data))


def test_fixed_numeric_fields_reject_literal_equality_coercion() -> None:
    base = BackendHealth(
        backend_id="backend-a",
        ready=True,
        logical_admission_mem_mb=1024,
        max_inflight=4,
        inflight=0,
        reserved_mem_mb=0,
        rss_bytes=None,
        cgroup_memory_bytes=None,
        cpu_usage_usec=None,
        throttled_usec=None,
    )
    for field, expected in (
        ("operations_per_unit", 2048),
        ("max_operations", 2_000_000),
        ("work_timeout_ms", 2000),
    ):
        for invalid in (float(expected), str(expected), True):
            with pytest.raises(ValidationError):
                BackendHealth.model_validate(base.model_dump() | {field: invalid})
    for invalid in (True, 1.0, "1"):
        with pytest.raises(ValidationError):
            PhysicalScaling.model_validate({"evaluation_seconds": invalid})
