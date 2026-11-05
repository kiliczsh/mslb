import hashlib
import io
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from typing import override

import pytest
from mslb_application.errors import OperationError
from mslb_application.files import FileReference, ManagedFiles, StorageLimits
from mslb_application.snapshots import make_snapshot
from mslb_contracts.inputs import InputError, ServerInput
from mslb_contracts.runs import RunSettings

SOURCE = (
    b'\xef\xbb\xbfrequest_id,t,work_units,mem_mb,extra\r\n" r2 ",1,3,2,ignored\r\n'
    b"r1,0,4,1,also ignored\r\n"
)


def storage(root: Path, limits: StorageLimits | None = None) -> ManagedFiles:
    return ManagedFiles(root, limits or StorageLimits(min_free_bytes=0))


class BoundedSource(io.BytesIO):
    @override
    def read(self, size: int | None = -1) -> bytes:
        assert size is not None and 0 < size <= 64 * 1024
        return super().read(size)


def test_ingest_preserves_original_bytes_and_canonicalizes_normalized_inputs(
    tmp_path: Path,
) -> None:
    files = storage(tmp_path)
    result = files.ingest(BoundedSource(SOURCE), "../../display-only.csv")
    assert result.original_digest == hashlib.sha256(SOURCE).hexdigest()
    assert result.source_size_bytes == len(SOURCE)
    assert result.original_digest is not None
    original = FileReference(
        f"uploads/{result.original_digest}.csv", result.original_digest, len(SOURCE)
    )
    with files.verified_stream(original) as stream:
        assert stream.read() == SOURCE
    requests = files.load_requests(result.digest, result.size_bytes)
    assert [(r.request_id, r.t) for r in requests] == [("r1", 0), ("r2", 1)]
    expected = b"t,request_id,work_units,mem_mb\n0,r1,4,1\n1,r2,3,2\n"
    assert files.path(f"datasets/{result.digest}.csv").read_bytes() == expected
    assert files.ingest(BoundedSource(SOURCE), "../../display-only.csv") == result
    assert not list((tmp_path / ".staging").iterdir())
    assert len(list((tmp_path / "uploads").iterdir())) == 1
    assert len(list((tmp_path / "datasets").iterdir())) == 1


@pytest.mark.parametrize(
    "payload",
    [b"", b"not,csv\n", b"t,request_id,work_units,mem_mb\n0,bad,0,1\n", b" " * (10 * 1024**2 + 1)],
)
def test_invalid_or_oversized_upload_leaves_no_published_input(
    payload: bytes, tmp_path: Path
) -> None:
    files = storage(tmp_path)
    with pytest.raises((OperationError, InputError)):
        files.ingest(BoundedSource(payload), "invalid.csv")
    assert not list((tmp_path / ".staging").iterdir())
    assert not list((tmp_path / "uploads").iterdir())
    assert not list((tmp_path / "datasets").iterdir())


@pytest.mark.parametrize(
    "limits",
    [
        StorageLimits(quota_bytes=16, min_free_bytes=0),
        StorageLimits(min_free_bytes=2**60),
        StorageLimits(min_free_bytes=0, max_entries=4),
    ],
)
def test_storage_pressure_is_explicit_and_removes_only_its_own_staging(
    limits: StorageLimits, tmp_path: Path
) -> None:
    files = storage(tmp_path, limits)
    with pytest.raises(OperationError) as failure:
        files.ingest(BoundedSource(SOURCE), "bounded.csv")
    assert failure.value.code == "STORAGE_LIMIT_EXCEEDED"
    assert not list((tmp_path / ".staging").iterdir())
    assert not list((tmp_path / "uploads").iterdir())


def test_immutable_publication_and_verified_read_reject_corruption(tmp_path: Path) -> None:
    files = storage(tmp_path)
    first = files.publish("examples/value.bin", (b"one", b"two"), 6)
    assert files.publish(first.key, (b"onetwo",), 6) == first
    with pytest.raises(OperationError, match="differs"):
        files.publish(first.key, (b"change",), 6)
    assert files.path(first.key).read_bytes() == b"onetwo"
    files.path(first.key).write_bytes(b"damage")
    with pytest.raises(OperationError, match="identity"):
        with files.verified_stream(first):
            pytest.fail("Corrupt data became visible")
    assert not list((tmp_path / ".staging").iterdir())


def test_normalized_semantic_and_canonical_byte_tampering_both_fail(tmp_path: Path) -> None:
    files = storage(tmp_path)
    result = files.ingest(BoundedSource(SOURCE), "source.csv")
    path = files.path(f"datasets/{result.digest}.csv")
    original = path.read_bytes()
    path.write_bytes(original.replace(b"r1", b"x1"))
    with pytest.raises(OperationError, match="identity"):
        files.load_requests(result.digest, result.size_bytes)
    path.write_bytes(b"request_id,t,work_units,mem_mb\nr1,0,4,1\nr2,1,3,2\n")
    assert path.stat().st_size == result.size_bytes
    with pytest.raises(OperationError, match="identity"):
        files.load_requests(result.digest, result.size_bytes)


def test_snapshots_round_trip_by_identity_and_never_follow_arbitrary_keys(tmp_path: Path) -> None:
    files = storage(tmp_path)
    result = files.ingest(BoundedSource(SOURCE), "source.csv")
    requests = files.load_requests(result.digest, result.size_bytes)
    snapshot = make_snapshot(
        (ServerInput(id="s", cpu_units_per_tick=2, mem_mb=4, rate_limit_per_sec=2),),
        requests,
        RunSettings(),
    )
    reference = files.save_snapshot(snapshot)
    assert files.load_snapshot(reference.key, snapshot.snapshot_digest) == snapshot
    with pytest.raises(OperationError):
        files.load_snapshot(reference.key, "0" * 64)
    files.path(reference.key).write_bytes(b"{}\n")
    with pytest.raises(InputError):
        files.load_snapshot(reference.key, snapshot.snapshot_digest)


@pytest.mark.parametrize(
    "key", ["../outside", "/absolute", "a/../../outside", "a//b", "a/./b", "C:\\file", "a/../b"]
)
def test_storage_keys_cannot_escape_the_managed_root(tmp_path: Path, key: str) -> None:
    with pytest.raises(OperationError):
        storage(tmp_path).publish(key, (b"not written",), 100)


def test_symlinked_directories_and_nonregular_snapshot_targets_fail(tmp_path: Path) -> None:
    root, outside = tmp_path / "managed", tmp_path / "outside"
    outside.mkdir()
    files = storage(root)
    (root / "datasets").rmdir()
    (root / "datasets").symlink_to(outside, target_is_directory=True)
    with pytest.raises(OperationError):
        files.publish("datasets/untrusted.csv", (b"never written",), 100)
    assert not list(outside.iterdir())
    fifo = root / "snapshots" / ("0" * 64 + ".json")
    os.mkfifo(fifo)
    with pytest.raises(OperationError):
        files.load_snapshot(f"snapshots/{'0' * 64}.json", "0" * 64)


def test_only_one_active_parser_is_admitted(tmp_path: Path) -> None:
    files, entered, release = storage(tmp_path), Event(), Event()

    class PausedSource(BoundedSource):
        @override
        def read(self, size: int | None = -1) -> bytes:
            entered.set()
            assert release.wait(5)
            return super().read(size)

    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(files.ingest, PausedSource(SOURCE), "first.csv")
        assert entered.wait(5)
        try:
            with pytest.raises(OperationError) as error:
                files.ingest(BoundedSource(SOURCE), "second.csv")
            assert error.value.code == "RATE_LIMITED"
        finally:
            release.set()
        assert first.result(timeout=5).request_count == 2
