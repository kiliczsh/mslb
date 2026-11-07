"""Publication liveness is bounded, symlink-safe and independent of workload readiness."""

import json
from pathlib import Path

import pytest
from mslb_controller import guard_health


def test_guard_liveness_writer_and_unsafe_frames(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "health.json"
    monkeypatch.setattr(target=guard_health, name="HEALTH", value=path)
    monkeypatch.setattr(target=guard_health.time, name="monotonic", value=lambda: 100.0)
    assert not guard_health.healthy()
    guard_health.write_health()
    assert guard_health.healthy()
    for value in (True, None, "100", 101, 91, float("nan"), float("inf"), 10**900):
        path.write_text(data=json.dumps(obj={"updated": value}))
        assert not guard_health.healthy()
    path.write_bytes(data=b"x" * 1025)
    assert not guard_health.healthy()
    path.unlink()
    target = tmp_path / "target.json"
    target.write_text(data='{"updated":100}')
    path.symlink_to(target=target)
    assert not guard_health.healthy()
