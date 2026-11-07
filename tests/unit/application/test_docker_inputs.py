"""Bounded immutable operational snapshots cannot alias logical input files."""

import hashlib
from pathlib import Path
from uuid import uuid4

import pytest
from mslb_application.docker_inputs import load_docker_snapshot, save_docker_snapshot
from mslb_application.errors import OperationError
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_contracts.docker_runs import DockerSettings, DockerSnapshot


def snapshot() -> DockerSnapshot:
    return DockerSnapshot(
        dataset_id=uuid4(),
        dataset_digest="a" * 64,
        request_count=2,
        latest_arrival=1,
        deployment_id=uuid4(),
        deployment_digest="b" * 64,
        configuration=DockerSettings(duration_seconds=2),
    )


def files(*, root: Path) -> ManagedFiles:
    return ManagedFiles(root=root, limits=StorageLimits(min_free_bytes=0))


def test_snapshot_bytes_are_canonical_immutable_and_separate_from_logical_files(
    tmp_path: Path,
) -> None:
    storage, value = files(root=tmp_path), snapshot()
    saved = save_docker_snapshot(files=storage, snapshot=value)
    assert saved.key == f"docker/snapshots/{value.digest()}.json" and saved.sha256 == value.digest()
    assert save_docker_snapshot(files=storage, snapshot=value) == saved
    assert (
        load_docker_snapshot(files=storage, key=saved.key, expected_digest=value.digest()) == value
    )
    assert not storage.path(key=f"snapshots/{value.digest()}.json").exists()
    storage.path(key=saved.key).write_bytes(data=b"corrupted")
    with pytest.raises(expected_exception=OperationError, match="differs"):
        save_docker_snapshot(files=storage, snapshot=value)
    assert storage.path(key=saved.key).read_bytes() == b"corrupted"


def test_snapshot_loader_rejects_missing_wrong_namespace_oversized_and_noncanonical_bytes(
    tmp_path: Path,
) -> None:
    storage, value = files(root=tmp_path), snapshot()
    key = f"docker/snapshots/{value.digest()}.json"
    with pytest.raises(expected_exception=OperationError, match="unavailable"):
        load_docker_snapshot(files=storage, key=key, expected_digest=value.digest())
    with pytest.raises(expected_exception=OperationError, match="namespace"):
        load_docker_snapshot(
            files=storage, key=f"snapshots/{value.digest()}.json", expected_digest=value.digest()
        )
    save_docker_snapshot(files=storage, snapshot=value)
    for raw in (b"x" * (16 * 1024 + 1), value.model_dump_json().encode()):
        expected = hashlib.sha256(string=raw).hexdigest()
        altered = storage.path(key=f"docker/snapshots/{expected}.json")
        altered.write_bytes(data=raw)
        with pytest.raises(expected_exception=OperationError):
            load_docker_snapshot(
                files=storage, key=f"docker/snapshots/{expected}.json", expected_digest=expected
            )


def test_snapshot_storage_refuses_symlinks_and_exhausted_quota(tmp_path: Path) -> None:
    value = snapshot()
    storage = files(root=tmp_path / "store")
    target = tmp_path / "outside"
    target.write_bytes(data=b"outside")
    directory = storage.path(key="docker")
    directory.symlink_to(target=tmp_path)
    with pytest.raises(expected_exception=OperationError, match="unsafe"):
        save_docker_snapshot(files=storage, snapshot=value)
    assert target.read_bytes() == b"outside"
    limited = ManagedFiles(
        root=tmp_path / "limited", limits=StorageLimits(quota_bytes=1, min_free_bytes=0)
    )
    with pytest.raises(expected_exception=OperationError, match="quota"):
        save_docker_snapshot(files=limited, snapshot=value)
