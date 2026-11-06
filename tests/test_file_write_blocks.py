"""Fragmented producers retain streaming, quota and immutable-publication guarantees."""

import hashlib
from collections.abc import Generator
from pathlib import Path
from typing import override

import pytest
from mslb_application.errors import OperationError
from mslb_application.files import ManagedFiles, StorageLimits


class MeasuredFiles(ManagedFiles):
    def __init__(self, *, root: Path, quota: int = 1024**2) -> None:
        super().__init__(root=root, limits=StorageLimits(min_free_bytes=0, quota_bytes=quota))
        self.admissions: list[tuple[int, int]] = []

    @override
    def _admit(
        self, additional: int, scope: str | None = None, scope_additional: int | None = None
    ) -> None:
        staged = tuple((self.root / ".staging").iterdir())
        assert len(staged) == 1
        # Previous writes must already be visible during the next quota check.
        self.admissions.append((additional, staged[0].stat().st_size))
        super()._admit(additional=additional, scope=scope, scope_additional=scope_additional)


def test_fragmented_publication_flushes_before_reading_ahead_and_preserves_bytes(
    tmp_path: Path,
) -> None:
    files = MeasuredFiles(root=tmp_path)
    prefix = bytes(range(256)) * 256
    large = b"x" * (2 * 64 * 1024 + 19)
    suffix = "Türkçe".encode()
    expected = prefix + large + suffix

    def producer() -> Generator[bytes]:
        yield b""
        for value in prefix:
            yield bytes((value,))
        assert len(files.admissions) == 1, (
            "Tiny fragments triggered extra scans or were buffered whole"
        )
        yield large
        yield b""
        yield suffix

    reference = files.publish(key="runs/owned/data.bin", chunks=producer(), maximum=len(expected))
    assert files.path(key=reference.key).read_bytes() == expected
    assert reference.sha256 == hashlib.sha256(data=expected).hexdigest()
    assert reference.size_bytes == len(expected)
    assert files.admissions == [
        (65536, 0),
        (65536, 65536),
        (65536, 131072),
        (len(expected) - 196608, 196608),
    ]
    assert not tuple((tmp_path / ".staging").iterdir())


@pytest.mark.parametrize(argnames="maximum", argvalues=[0, 3, 65536])
def test_fragmented_overflow_is_rejected_and_staging_is_removed(
    tmp_path: Path, maximum: int
) -> None:
    files = ManagedFiles(root=tmp_path, limits=StorageLimits(min_free_bytes=0))
    produced = 0

    def producer() -> Generator[bytes]:
        nonlocal produced
        for _ in range(maximum + 2):
            produced += 1
            yield b"x"

    with pytest.raises(expected_exception=OperationError) as error:
        files.publish(key="runs/owned/overflow.bin", chunks=producer(), maximum=maximum)
    assert error.value.code == "PAYLOAD_TOO_LARGE"
    assert produced == maximum + 1, "File limit did not stop producer consumption promptly"
    assert not files.path(key="runs/owned/overflow.bin").exists()
    assert not tuple((tmp_path / ".staging").iterdir())


def test_each_coalesced_write_still_checks_quota_and_preserves_existing_files(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "retained.bin"
    marker.write_bytes(data=b"retained")
    files = MeasuredFiles(root=tmp_path, quota=65536 + 8)
    with pytest.raises(expected_exception=OperationError) as error:
        files.publish(key="runs/owned/quota.bin", chunks=(b"x" * 65536, b"y"), maximum=65537)
    assert error.value.code == "STORAGE_LIMIT_EXCEEDED"
    assert files.admissions == [(65536, 0), (1, 65536)]
    assert marker.read_bytes() == b"retained"
    assert not files.path(key="runs/owned/quota.bin").exists()
    assert not tuple((tmp_path / ".staging").iterdir())


def test_producer_failure_discards_both_written_and_buffered_bytes(tmp_path: Path) -> None:
    files = ManagedFiles(root=tmp_path, limits=StorageLimits(min_free_bytes=0))

    def producer() -> Generator[bytes]:
        yield b"x" * 65536
        yield b"buffered"
        raise OSError("Injected producer failure")

    with pytest.raises(expected_exception=OSError, match="Injected producer failure"):
        files.publish(key="runs/owned/failed.bin", chunks=producer(), maximum=1024**2)
    assert not files.path(key="runs/owned/failed.bin").exists()
    assert not tuple((tmp_path / ".staging").iterdir())
