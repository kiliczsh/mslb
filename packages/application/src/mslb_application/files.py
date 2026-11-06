"""Bounded managed local files; only server-derived keys can select storage."""

import csv
import fcntl
import hashlib
import io
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.inputs import MAX_UPLOAD_BYTES, RequestInput, input_digest, read_requests
from mslb_contracts.runs import RunSnapshot

from mslb_application.errors import OperationError
from mslb_application.snapshots import read_snapshot, verify_snapshot


@dataclass(frozen=True, slots=True)
class StorageLimits:
    quota_bytes: int = 5 * 1024**3
    min_free_bytes: int = 2 * 1024**3
    max_entries: int = 100_000

    def __post_init__(self) -> None:
        if any(
            type(value) is not int or value < 0
            for value in (self.quota_bytes, self.min_free_bytes, self.max_entries)
        ) or not (1 <= self.quota_bytes <= 5 * 1024**3 and 1 <= self.max_entries <= 100_000):
            raise OperationError("VALIDATION_ERROR", "Invalid managed storage limits")


@dataclass(frozen=True, slots=True)
class FileReference:
    key: str
    sha256: str
    size_bytes: int

    def __post_init__(self) -> None:
        if (
            not re.fullmatch(r"[0-9a-f]{64}", self.sha256)
            or type(self.size_bytes) is not int
            or not 0 <= self.size_bytes <= 256 * 1024**2
        ):
            raise OperationError("VALIDATION_ERROR", "Invalid managed file reference")


@dataclass(frozen=True, slots=True)
class _Staged:
    path: Path
    sha256: str
    size_bytes: int


def request_bytes(requests: tuple[RequestInput, ...]) -> Generator[bytes]:
    """Canonical LF CSV without retaining a second whole dataset buffer."""
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(("t", "request_id", "work_units", "mem_mb"))
    yield buffer.getvalue().encode()
    for request in requests:
        buffer.seek(0)
        buffer.truncate()
        writer.writerow((request.t, request.request_id, request.work_units, request.mem_mb))
        yield buffer.getvalue().encode()


def _read_chunks(stream: BinaryIO) -> Generator[bytes]:
    while block := stream.read(64 * 1024):
        yield block


def _write_blocks(*, chunks: Iterable[bytes], maximum: int) -> Generator[bytes]:
    """Coalesce encoder fragments without buffering more than one 64 KiB block."""
    remaining_budget = maximum
    pending = bytearray()
    for chunk in chunks:
        remaining = memoryview(chunk)
        while remaining:
            amount = min(len(remaining), 64 * 1024 - len(pending), remaining_budget + 1)
            pending.extend(remaining[:amount])
            remaining = remaining[amount:]
            remaining_budget -= amount
            if remaining_budget < 0:
                raise OperationError(
                    code="PAYLOAD_TOO_LARGE", message="Managed file exceeds its byte limit"
                )
            if len(pending) == 64 * 1024:
                yield bytes(pending)
                pending.clear()
    if pending:
        yield bytes(pending)


class ManagedFiles:
    """Linux-container storage with immutable publication and scoped admission locks."""

    def __init__(
        self, root: Path, limits: StorageLimits | None = None, read_only: bool = False
    ) -> None:
        if root.is_symlink():
            raise OperationError("VALIDATION_FAILED", "Managed storage root is unsafe")
        if not read_only:
            root.mkdir(parents=True, exist_ok=True)
        elif not root.is_dir():
            raise OperationError("VALIDATION_FAILED", "Read-only storage root is missing")
        self.root = root.resolve()
        self.limits = limits or StorageLimits()
        self.read_only = read_only
        for name in (".staging", "uploads", "datasets", "snapshots"):
            path = self.root / name
            if path.is_symlink():
                raise OperationError("VALIDATION_FAILED", "Managed storage directory is unsafe")
            if not read_only:
                path.mkdir(exist_ok=True)
        if not read_only:
            self._sync_directory(self.root)

    @staticmethod
    def _sync_directory(directory: Path) -> None:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    def path(self, key: str) -> Path:
        if not re.fullmatch(r"[a-z0-9][a-z0-9._/-]{0,255}", key) or any(
            part in {"", ".", ".."} for part in key.split("/")
        ):
            raise OperationError("VALIDATION_ERROR", "Invalid managed storage key")
        path = self.root / key
        for candidate in (path, *path.parents):
            if candidate == self.root:
                break
            if candidate.is_symlink():
                raise OperationError("VALIDATION_FAILED", "Managed storage path is unsafe")
        if not path.resolve().is_relative_to(self.root):
            raise OperationError("VALIDATION_FAILED", "Managed storage path escapes its root")
        return path

    @contextmanager
    def _lock(self, name: str, wait: bool = True) -> Generator[None]:
        if self.read_only:
            raise OperationError("FORBIDDEN", "Storage is read-only")
        descriptor = os.open(self.root / name, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        deadline = time.monotonic() + 5
        try:
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if not wait:
                        raise OperationError(
                            "RATE_LIMITED", "Another upload is being parsed"
                        ) from None
                    if time.monotonic() >= deadline:
                        raise OperationError(
                            "RESOURCE_PRESSURE", "Storage writer is busy"
                        ) from None
                    time.sleep(0.01)
            yield
        finally:
            os.close(descriptor)

    def _usage(self, root: Path | None = None) -> int:
        entries = 0

        def measure(directory: Path, depth: int = 0) -> int:
            nonlocal entries
            if depth > 8:
                raise OperationError("STORAGE_LIMIT_EXCEEDED", "Managed directory depth exceeded")
            total = 0
            with os.scandir(directory) as children:
                for entry in children:
                    entries += 1
                    if entries > self.limits.max_entries:
                        raise OperationError(
                            "STORAGE_LIMIT_EXCEEDED", "Managed file count exceeded"
                        )
                    try:
                        if entry.is_symlink():
                            raise OperationError(
                                "VALIDATION_FAILED", "Managed storage contains unsafe links"
                            )
                        if entry.is_dir(follow_symlinks=False):
                            total += measure(Path(entry.path), depth + 1)
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                        else:
                            raise OperationError(
                                "VALIDATION_FAILED", "Managed storage contains unsafe entries"
                            )
                    except FileNotFoundError:
                        # Another managed writer may remove its own staging file
                        # while the scan advances. No staged bytes then remain.
                        continue
            return total

        return measure(root or self.root)

    def _admit(
        self, additional: int, scope: str | None = None, scope_additional: int | None = None
    ) -> None:
        if self._usage() + additional > self.limits.quota_bytes:
            raise OperationError("STORAGE_LIMIT_EXCEEDED", "Managed storage quota exceeded")
        if shutil.disk_usage(self.root).free - additional < self.limits.min_free_bytes:
            raise OperationError(
                "STORAGE_LIMIT_EXCEEDED", "Managed storage free-space reserve reached"
            )
        if scope is not None:
            directory = self.path(scope)
            pending = additional if scope_additional is None else scope_additional
            if directory.is_dir() and self._usage(directory) + pending > 512 * 1024**2:
                raise OperationError(
                    "STORAGE_LIMIT_EXCEEDED", "Run physical storage ceiling reached"
                )

    @staticmethod
    def run_scope(key: str) -> str | None:
        parts = key.split("/")
        if len(parts) >= 3 and (
            parts[0] == "runs"
            or parts[0] == "docker"
            and re.fullmatch(pattern=r"[0-9a-f]{32}", string=parts[1])
        ):
            return "/".join(parts[:2])
        return None

    @contextmanager
    def _stage(
        self, chunks: Iterable[bytes], maximum: int, scope: str | None = None
    ) -> Generator[_Staged]:
        if self.read_only:
            raise OperationError("FORBIDDEN", "Storage is read-only")
        # Unbuffered writes make in-flight file bytes visible to other writers'
        # quota scans before the cross-process admission lock is released.
        with tempfile.NamedTemporaryFile(
            dir=self.root / ".staging", buffering=0, delete=False
        ) as stream:
            path, digest, size = Path(stream.name), hashlib.sha256(), 0
            try:
                for block in _write_blocks(chunks=chunks, maximum=maximum):
                    if size + len(block) > maximum:
                        raise OperationError(
                            code="PAYLOAD_TOO_LARGE", message="Managed file exceeds its byte limit"
                        )
                    with self._lock(name=".write-lock"):
                        self._admit(
                            additional=len(block), scope=scope, scope_additional=size + len(block)
                        )
                        if stream.write(block) != len(block):
                            raise OSError("Short managed file write")
                    digest.update(block)
                    size += len(block)
                os.fsync(stream.fileno())
                yield _Staged(path, digest.hexdigest(), size)
            finally:
                stream.close()
                path.unlink(missing_ok=True)

    @staticmethod
    def _hash(path: Path, maximum: int) -> tuple[int, str]:
        size, digest = 0, hashlib.sha256()
        with path.open("rb") as stream:
            for block in _read_chunks(stream):
                size += len(block)
                if size > maximum:
                    raise OperationError("VALIDATION_FAILED", "Managed file length mismatch")
                digest.update(block)
        return size, digest.hexdigest()

    def _publish(self, staged: _Staged, key: str) -> FileReference:
        path = self.path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(staged.path, path)
        except FileExistsError:
            if (
                not path.is_file()
                or path.is_symlink()
                or self._hash(path, staged.size_bytes) != (staged.size_bytes, staged.sha256)
            ):
                raise OperationError(
                    "VALIDATION_FAILED", "Existing immutable file differs"
                ) from None
        directory = path.parent
        while True:
            self._sync_directory(directory)
            if directory == self.root:
                break
            directory = directory.parent
        return FileReference(key, staged.sha256, staged.size_bytes)

    def publish(self, key: str, chunks: Iterable[bytes], maximum: int) -> FileReference:
        """Write a server-owned candidate without replacing any existing file."""
        self.path(key)
        if type(maximum) is not int or not 0 <= maximum <= 256 * 1024**2:
            raise OperationError("VALIDATION_ERROR", "Invalid file byte limit")
        with self._stage(chunks, maximum, self.run_scope(key)) as staged:
            return self._publish(staged, key)

    @contextmanager
    def work_file(self, key: str) -> Generator["WorkFile"]:
        if self.read_only or self.run_scope(key) is None:
            raise OperationError("FORBIDDEN", "Work files require a writable run namespace")
        path = self.path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x+b", buffering=0) as stream:
            writer = WorkFile(self, key, stream)
            try:
                yield writer
            finally:
                stream.close()
                path.unlink(missing_ok=True)

    def remove(self, key: str) -> None:
        """Remove an exact file only after the caller verifies DB non-reference/ownership."""
        if self.read_only:
            raise OperationError("FORBIDDEN", "Storage is read-only")
        path = self.path(key)
        if path.exists() and not path.is_file():
            raise OperationError("VALIDATION_FAILED", "Cleanup target is not a regular file")
        path.unlink(missing_ok=True)

    def ingest(self, source: BinaryIO, filename: str) -> DatasetRegistration:
        """Preserve source bytes and publish normalized data only after full validation."""
        with self._lock(".upload-lock", wait=False):
            with self._stage(_read_chunks(source), MAX_UPLOAD_BYTES) as original:
                with original.path.open("rb") as stream:
                    requests = read_requests(stream)
                digest = input_digest(requests)
                with self._stage(request_bytes(requests), MAX_UPLOAD_BYTES) as normalized:
                    data = DatasetRegistration(
                        digest=digest,
                        source_filename=filename,
                        request_count=len(requests),
                        earliest_arrival=requests[0].t,
                        latest_arrival=requests[-1].t,
                        size_bytes=normalized.size_bytes,
                        original_digest=original.sha256,
                        source_size_bytes=original.size_bytes,
                    )
                    self._publish(original, f"uploads/{original.sha256}.csv")
                    self._publish(normalized, f"datasets/{digest}.csv")
                return data

    def load_requests(self, digest: str, size_bytes: int) -> tuple[RequestInput, ...]:
        path = self.path(f"datasets/{digest}.csv")
        if not path.is_file() or path.stat().st_size != size_bytes:
            raise OperationError("VALIDATION_FAILED", "Stored dataset length mismatch")
        with path.open("rb") as stream:
            requests = read_requests(stream)
        expected = hashlib.sha256()
        for chunk in request_bytes(requests):
            expected.update(chunk)
        if (
            input_digest(requests) != digest
            or self._hash(path, MAX_UPLOAD_BYTES)[1] != expected.hexdigest()
        ):
            raise OperationError("VALIDATION_FAILED", "Stored dataset identity mismatch")
        return requests

    def save_snapshot(self, snapshot: RunSnapshot) -> FileReference:
        verify_snapshot(snapshot)
        return self.publish(
            f"snapshots/{snapshot.snapshot_digest}.json",
            (canonical_bytes(snapshot.model_dump(mode="json")),),
            32 * 1024**2,
        )

    def load_snapshot(self, key: str, expected_digest: str) -> RunSnapshot:
        if key != f"snapshots/{expected_digest}.json":
            raise OperationError("VALIDATION_FAILED", "Snapshot reference does not match identity")
        path = self.path(key)
        if not path.is_file():
            raise OperationError("VALIDATION_FAILED", "Stored snapshot is missing or unsafe")
        with path.open("rb") as stream:
            saved = read_snapshot(stream)
        if saved.snapshot_digest != expected_digest:
            raise OperationError("VALIDATION_FAILED", "Stored snapshot identity mismatch")
        return saved

    @contextmanager
    def verified_stream(self, reference: FileReference) -> Generator[BinaryIO]:
        path = self.path(reference.key)
        if not path.is_file() or self._hash(path, reference.size_bytes) != (
            reference.size_bytes,
            reference.sha256,
        ):
            raise OperationError("VALIDATION_FAILED", "Stored file identity mismatch")
        with path.open("rb") as stream:
            yield stream


class WorkFile:
    """Private attempt spool with a bounded buffer and accounted physical writes."""

    def __init__(self, files: ManagedFiles, key: str, stream: BinaryIO) -> None:
        self.files, self.key, self.stream = files, key, stream
        self.size = 0
        self.buffer = bytearray()
        self.check: Callable[[], None] | None = None

    @property
    def position(self) -> int:
        return self.size + len(self.buffer)

    def write(self, data: bytes) -> None:
        if self.check is not None:
            self.check()
        if self.position + len(data) > 256 * 1024**2:
            raise OperationError(
                "STORAGE_LIMIT_EXCEEDED", "Attempt stream exceeds its byte ceiling"
            )
        for offset in range(0, len(data), 64 * 1024):
            self.buffer.extend(data[offset : offset + 64 * 1024])
            if len(self.buffer) >= 256 * 1024:
                self.flush()

    def flush(self) -> None:
        if self.buffer:
            with self.files._lock(".write-lock"):
                if self.check is not None:
                    self.check()
                self.files._admit(len(self.buffer), self.files.run_scope(self.key))
                self.stream.seek(self.size)
                if self.stream.write(self.buffer) != len(self.buffer):
                    raise OSError("Short attempt spool write")
                self.size += len(self.buffer)
                self.buffer.clear()
        self.stream.seek(self.size)

    def sync(self) -> None:
        self.flush()
        os.fsync(self.stream.fileno())
