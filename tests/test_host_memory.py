"""Bounded numeric host bridge, atomic replacement and untrusted file rejection."""

import io
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from mslb_contracts.resources import HostMemoryEvidence
from mslb_controller import host_memory
from pydantic import ValidationError

from scripts.check_deployment import check


def frame(*, second: int = 0) -> HostMemoryEvidence:
    return HostMemoryEvidence(
        measured_at=datetime.now(tz=UTC) + timedelta(seconds=second),
        total_memory_bytes=32 * 1024**3,
        available_memory_bytes=10 * 1024**3,
    )


def test_writer_replaces_one_bounded_frame_and_rejects_replay(tmp_path: Path) -> None:
    value = frame()
    raw = value.model_dump_json().encode() + b"\n"
    host_memory.consume(stream=io.BytesIO(initial_bytes=raw), directory=tmp_path)
    assert (tmp_path / "memory.json").read_bytes() == raw.rstrip(b"\n")
    assert not (tmp_path / "memory.tmp").exists()
    with pytest.raises(expected_exception=ValueError, match="stale or repeated"):
        host_memory.write_frame(raw=raw, directory=tmp_path, previous=value.measured_at)
    assert (tmp_path / "memory.json").read_bytes() == raw.rstrip(b"\n")


@pytest.mark.parametrize(
    argnames="raw",
    argvalues=[b"x" * 513, b"{}", b"{}\n", b'{"command":"anything"}\n'],
)
def test_invalid_frame_never_replaces_good_evidence(tmp_path: Path, raw: bytes) -> None:
    target = tmp_path / "memory.json"
    target.write_bytes(data=b"previous")
    with pytest.raises(expected_exception=(ValueError, ValidationError)):
        host_memory.consume(stream=io.BytesIO(initial_bytes=raw), directory=tmp_path)
    assert target.read_bytes() == b"previous"


@pytest.mark.parametrize(argnames="second", argvalues=[-4, 1])
def test_writer_rejects_stale_and_future_host_clock(tmp_path: Path, second: int) -> None:
    with pytest.raises(expected_exception=ValueError, match="stale or repeated"):
        host_memory.consume(
            stream=io.BytesIO(
                initial_bytes=frame(second=second).model_dump_json().encode() + b"\n"
            ),
            directory=tmp_path,
        )


def test_host_contract_refuses_bool_oversized_inconsistent_and_non_utc_counters() -> None:
    original = frame().model_dump(mode="json")
    for changes in (
        {"available_memory_bytes": True},
        {"available_memory_bytes": 33 * 1024**3},
        {"total_memory_bytes": 2**63},
        {"measured_at": "2026-11-06T10:00:00+03:00"},
        {"extra": 0},
    ):
        with pytest.raises(expected_exception=ValidationError):
            HostMemoryEvidence.model_validate_json(json_data=json.dumps(obj=original | changes))


def test_reader_checks_owner_mode_type_links_and_length(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(target=host_memory, name="DIRECTORY", value=tmp_path)
    assert host_memory.read_host_memory() is None
    value = frame()
    target = tmp_path / "memory.json"
    host_memory.consume(
        stream=io.BytesIO(initial_bytes=value.model_dump_json().encode() + b"\n"),
        directory=tmp_path,
    )
    original_fstat = os.fstat
    evidence_owner = 1000

    def root_stat(fd: int) -> os.stat_result:
        fields = list(original_fstat(fd=fd))
        fields[4] = evidence_owner
        return os.stat_result(fields)

    monkeypatch.setattr(target=host_memory.os, name="fstat", value=root_stat)
    assert host_memory.read_host_memory() is None
    evidence_owner = 0
    assert host_memory.read_host_memory() == value
    target.chmod(mode=0o666)
    assert host_memory.read_host_memory() is None
    target.chmod(mode=0o644)
    hardlink = tmp_path / "hardlink"
    hardlink.hardlink_to(target=target)
    assert host_memory.read_host_memory() is None
    hardlink.unlink()
    target.write_bytes(data=b"x" * 513)
    assert host_memory.read_host_memory() is None
    target.unlink()
    target.symlink_to(target=tmp_path / "missing")
    assert host_memory.read_host_memory() is None
    target.unlink()
    if sys.platform != "linux":
        pytest.skip(reason="FIFO security checks require Linux containers")
    os.mkfifo(path=target, mode=0o644)
    assert host_memory.read_host_memory() is None


@pytest.mark.parametrize(
    argnames=("service", "read_only", "allowed"),
    argvalues=[
        ("resource-guard", True, True),
        ("host-memory-writer", False, True),
        ("resource-guard", False, False),
        ("api", True, False),
        ("host-memory-writer", True, False),
    ],
)
def test_deployment_confines_host_evidence_mount(
    service: str, read_only: bool, allowed: bool
) -> None:
    payload = json.dumps(
        obj={
            "services": {
                service: {
                    "volumes": [
                        {
                            "type": "volume",
                            "source": "mslb_host-memory",
                            "target": "/run/host-memory",
                            "read_only": read_only,
                        }
                    ]
                }
            }
        }
    ).encode()
    if allowed:
        check(payload=payload)
    else:
        with pytest.raises(expected_exception=ValueError, match="one isolated writer"):
            check(payload=payload)
