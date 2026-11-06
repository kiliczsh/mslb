import json
import subprocess
import sys
from pathlib import Path

import pytest
from mslb_worker import health


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"updated": 95}, True),
        ({"updated": 100}, True),
        ({"updated": 90}, False),
        ({"updated": 101}, False),
        ({"updated": "95"}, False),
        ({}, False),
        ([], False),
        (None, False),
    ],
)
def test_heartbeat_freshness_and_invalid_frames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, payload: object, expected: bool
) -> None:
    target = tmp_path / "health.json"
    monkeypatch.setattr(target=health, name="HEALTH", value=target)
    monkeypatch.setattr(target=health.time, name="monotonic", value=lambda: 100)
    target.write_text(data=json.dumps(obj=payload), encoding="utf-8")
    assert health.healthy() is expected
    target.write_bytes(data=b"not-json")
    assert not health.healthy()
    target.unlink()
    assert not health.healthy()


def test_heartbeat_writer_keeps_private_atomic_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "private" / "health.json"
    monkeypatch.setattr(target=health, name="HEALTH", value=target)
    health.write_health()
    assert health.healthy()
    assert target.parent.stat().st_mode & 0o777 == 0o700
    assert not target.with_suffix(suffix=".tmp").exists()
    target.parent.chmod(mode=0o755)
    with pytest.raises(expected_exception=ValueError, match="private"):
        health.write_health()


def test_docker_health_import_does_not_load_worker_or_database_stack() -> None:
    code = (
        "import sys; from mslb_worker import health; "
        "assert not {'sqlalchemy','redis','mslb_application.worker','mslb_worker.__main__'} "
        "& sys.modules.keys()"
    )
    subprocess.run(  # noqa: S603 -- fixed local interpreter and source, no user command.
        args=[sys.executable, "-c", code], check=True, capture_output=True, timeout=5
    )
