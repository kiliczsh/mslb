"""Unsupported runtime platforms cannot silently weaken private Linux storage."""

import sys
from collections.abc import Callable
from pathlib import Path

import pytest
from mslb_api.settings import cursor_secret
from mslb_application.files import ManagedFiles
from mslb_controller import credentials, guard_health, host_memory
from mslb_worker import health


def test_unsupported_platform_refuses_private_writes_before_touching_storage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "untouched"
    operations: tuple[Callable[[], object], ...] = (
        lambda: cursor_secret(root=target),
        lambda: ManagedFiles(root=target),
        lambda: credentials.initialize(),
        lambda: credentials.read_token(),
        lambda: guard_health.write_health(),
        lambda: health.write_health(),
        lambda: host_memory.write_frame(raw=b"invalid\n", directory=target, previous=None),
        lambda: host_memory.main(),
    )
    with monkeypatch.context() as patch:
        patch.setattr(target=sys, name="platform", value="win32")
        for operation in operations:
            with pytest.raises(expected_exception=RuntimeError, match="Linux containers"):
                operation()
        assert guard_health.healthy() is False
        assert host_memory.read_host_memory() is None
    assert not target.exists()
