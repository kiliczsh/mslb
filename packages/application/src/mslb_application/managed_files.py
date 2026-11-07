"""Linux managed storage, immutable publication and scoped admission."""

import hashlib
import os
import re
import shutil
import sys
import tempfile
import time
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from pathlib import Path
from typing import BinaryIO

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.capacity_profiles import request_limit
from mslb_contracts.catalog import DatasetRegistration
from mslb_contracts.inputs import MAX_UPLOAD_BYTES, RequestInput, input_digest, read_requests
from mslb_contracts.runs import RunSnapshot

from mslb_application.errors import OperationError
from mslb_application.snapshots import read_snapshot, verify_snapshot

if sys.platform == "linux":
    import fcntl
from mslb_application.storage_encoding import _read_chunks, _write_blocks, request_bytes
from mslb_application.storage_values import FileReference, StorageLimits, _Staged
from mslb_application.work_file import WorkFile


class ManagedFiles:
    """Linux-container storage with immutable publication and scoped admission locks."""

    root: Path
    limits: StorageLimits
    read_only: bool

    def __init__(
        self, root: Path, limits: StorageLimits | None = None, read_only: bool = False
    ) -> None:
        if sys.platform != "linux":
            raise RuntimeError("Managed storage requires Linux containers")
        if root.is_symlink():
            raise OperationError(code="VALIDATION_FAILED", message="Managed storage root is unsafe")
        if not read_only:
            root.mkdir(parents=True, exist_ok=True)
        elif not root.is_dir():
            raise OperationError(
                code="VALIDATION_FAILED", message="Read-only storage root is missing"
            )
        self.root = root.resolve()
        self.limits = limits or StorageLimits()
        self.read_only = read_only
        for name in (".staging", "uploads", "datasets", "snapshots"):
            path = self.root / name
            if path.is_symlink():
                raise OperationError(
                    code="VALIDATION_FAILED", message="Managed storage directory is unsafe"
                )
            if not read_only:
                path.mkdir(exist_ok=True)
        if not read_only:
            self._sync_directory(directory=self.root)

    @staticmethod
    def _sync_directory(directory: Path) -> None:
        if sys.platform != "linux":
            raise RuntimeError("Managed storage synchronization requires Linux containers")
        descriptor = os.open(path=directory, flags=os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd=descriptor)
        finally:
            os.close(fd=descriptor)

    def path(self, key: str) -> Path:
        if not re.fullmatch(pattern=r"[a-z0-9][a-z0-9._/-]{0,255}", string=key) or any(
            part in {"", ".", ".."} for part in key.split(sep="/")
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid managed storage key")
        path = self.root / key
        for candidate in (path, *path.parents):
            if candidate == self.root:
                break
            if candidate.is_symlink():
                raise OperationError(
                    code="VALIDATION_FAILED", message="Managed storage path is unsafe"
                )
        if not path.resolve().is_relative_to(self.root):
            raise OperationError(
                code="VALIDATION_FAILED", message="Managed storage path escapes its root"
            )
        return path

    @contextmanager
    def _lock(self, name: str, wait: bool = True) -> Generator[None]:
        """Retain scoped Linux writer admission with a bounded contention deadline."""
        if sys.platform != "linux":
            raise RuntimeError("Managed storage locking requires Linux containers")
        if self.read_only:
            raise OperationError(code="FORBIDDEN", message="Storage is read-only")
        descriptor = os.open(
            path=self.root / name, flags=os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, mode=0o600
        )
        deadline = time.monotonic() + 5
        try:
            while True:
                try:
                    fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if not wait:
                        raise OperationError(
                            code="RATE_LIMITED", message="Another upload is being parsed"
                        ) from None
                    if time.monotonic() >= deadline:
                        raise OperationError(
                            code="RESOURCE_PRESSURE", message="Storage writer is busy"
                        ) from None
                    time.sleep(0.01)
            yield
        finally:
            os.close(fd=descriptor)

    def _usage(self, root: Path | None = None) -> int:
        """Measure physical bytes while rejecting unsafe or excessive directory entries."""
        entries = 0

        def measure(directory: Path, depth: int = 0) -> int:
            """Walk accounted entries without following links or exceeding count/depth bounds."""
            nonlocal entries
            if depth > 8:
                raise OperationError(
                    code="STORAGE_LIMIT_EXCEEDED", message="Managed directory depth exceeded"
                )
            total = 0
            with os.scandir(path=directory) as children:
                for entry in children:
                    entries += 1
                    if entries > self.limits.max_entries:
                        raise OperationError(
                            code="STORAGE_LIMIT_EXCEEDED", message="Managed file count exceeded"
                        )
                    try:
                        if entry.is_symlink():
                            raise OperationError(
                                code="VALIDATION_FAILED",
                                message="Managed storage contains unsafe links",
                            )
                        if entry.is_dir(follow_symlinks=False):
                            total += measure(directory=Path(entry.path), depth=depth + 1)
                        elif entry.is_file(follow_symlinks=False):
                            total += entry.stat(follow_symlinks=False).st_size
                        else:
                            raise OperationError(
                                code="VALIDATION_FAILED",
                                message="Managed storage contains unsafe entries",
                            )
                    except FileNotFoundError:
                        # Another managed writer may remove its own staging file
                        # while the scan advances. No staged bytes then remain.
                        continue
            return total

        return measure(directory=root or self.root)

    def _admit(
        self, additional: int, scope: str | None = None, scope_additional: int | None = None
    ) -> None:
        if self._usage() + additional > self.limits.quota_bytes:
            raise OperationError(
                code="STORAGE_LIMIT_EXCEEDED", message="Managed storage quota exceeded"
            )
        if shutil.disk_usage(path=self.root).free - additional < self.limits.min_free_bytes:
            raise OperationError(
                code="STORAGE_LIMIT_EXCEEDED", message="Managed storage free-space reserve reached"
            )
        if scope is not None:
            directory = self.path(key=scope)
            pending = additional if scope_additional is None else scope_additional
            if directory.is_dir() and self._usage(root=directory) + pending > 512 * 1024**2:
                raise OperationError(
                    code="STORAGE_LIMIT_EXCEEDED", message="Run physical storage ceiling reached"
                )

    @staticmethod
    def run_scope(key: str) -> str | None:
        parts = key.split(sep="/")
        if len(parts) >= 3 and (
            parts[0] == "runs"
            or (parts[0] == "docker" and re.fullmatch(pattern=r"[0-9a-f]{32}", string=parts[1]))
        ):
            return "/".join(parts[:2])
        return None

    @contextmanager
    def _stage(
        self, chunks: Iterable[bytes], maximum: int, scope: str | None = None
    ) -> Generator[_Staged]:
        """Account bounded physical writes before exposing an immutable candidate."""
        if self.read_only:
            raise OperationError(code="FORBIDDEN", message="Storage is read-only")
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
                os.fsync(fd=stream.fileno())
                yield _Staged(path=path, sha256=digest.hexdigest(), size_bytes=size)
            finally:
                stream.close()
                path.unlink(missing_ok=True)

    @staticmethod
    def _hash(path: Path, maximum: int) -> tuple[int, str]:
        size, digest = 0, hashlib.sha256()
        with path.open(mode="rb") as stream:
            for block in _read_chunks(stream=stream):
                size += len(block)
                if size > maximum:
                    raise OperationError(
                        code="VALIDATION_FAILED", message="Managed file length mismatch"
                    )
                digest.update(block)
        return size, digest.hexdigest()

    def _publish(self, staged: _Staged, key: str) -> FileReference:
        """Link an immutable candidate or verify identical existing bytes before fsync."""
        path = self.path(key=key)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(src=staged.path, dst=path)
        except FileExistsError:
            if (
                not path.is_file()
                or path.is_symlink()
                or self._hash(path=path, maximum=staged.size_bytes)
                != (staged.size_bytes, staged.sha256)
            ):
                raise OperationError(
                    code="VALIDATION_FAILED", message="Existing immutable file differs"
                ) from None
        directory = path.parent
        while True:
            self._sync_directory(directory=directory)
            if directory == self.root:
                break
            directory = directory.parent
        return FileReference(key=key, sha256=staged.sha256, size_bytes=staged.size_bytes)

    def publish(self, key: str, chunks: Iterable[bytes], maximum: int) -> FileReference:
        """Write a server-owned candidate without replacing any existing file."""
        self.path(key=key)
        if type(maximum) is not int or not 0 <= maximum <= 256 * 1024**2:
            raise OperationError(code="VALIDATION_ERROR", message="Invalid file byte limit")
        with self._stage(chunks=chunks, maximum=maximum, scope=self.run_scope(key=key)) as staged:
            return self._publish(staged=staged, key=key)

    @contextmanager
    def work_file(self, key: str) -> Generator["WorkFile"]:
        if self.read_only or self.run_scope(key=key) is None:
            raise OperationError(
                code="FORBIDDEN", message="Work files require a writable run namespace"
            )
        path = self.path(key=key)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open(mode="x+b", buffering=0) as stream:
            writer = WorkFile(files=self, key=key, stream=stream)
            try:
                yield writer
            finally:
                stream.close()
                path.unlink(missing_ok=True)

    def remove(self, key: str) -> None:
        """Remove an exact file only after the caller verifies DB non-reference/ownership."""
        if self.read_only:
            raise OperationError(code="FORBIDDEN", message="Storage is read-only")
        path = self.path(key=key)
        if path.exists() and not path.is_file():
            raise OperationError(
                code="VALIDATION_FAILED", message="Cleanup target is not a regular file"
            )
        path.unlink(missing_ok=True)

    def ingest(self, source: BinaryIO, filename: str) -> DatasetRegistration:
        """Preserve source bytes and publish normalized data only after full validation."""
        with (
            self._lock(name=".upload-lock", wait=False),
            self._stage(chunks=_read_chunks(stream=source), maximum=MAX_UPLOAD_BYTES) as original,
        ):
            with original.path.open(mode="rb") as stream:
                requests = read_requests(
                    stream=stream, max_requests=request_limit(profile=self.limits.capacity_profile)
                )
            digest = input_digest(records=requests)
            with self._stage(
                chunks=request_bytes(requests=requests), maximum=MAX_UPLOAD_BYTES
            ) as normalized:
                data = DatasetRegistration(
                    digest=digest,
                    source_filename=filename,
                    request_count=len(requests),
                    earliest_arrival=requests[0].t,
                    latest_arrival=requests[-1].t,
                    size_bytes=normalized.size_bytes,
                    original_digest=original.sha256,
                    source_size_bytes=original.size_bytes,
                    capacity_profile=self.limits.capacity_profile,
                )
                self._publish(staged=original, key=f"uploads/{original.sha256}.csv")
                self._publish(staged=normalized, key=f"datasets/{digest}.csv")
            return data

    def load_requests(self, digest: str, size_bytes: int) -> tuple[RequestInput, ...]:
        path = self.path(key=f"datasets/{digest}.csv")
        if not path.is_file() or path.stat().st_size != size_bytes:
            raise OperationError(code="VALIDATION_FAILED", message="Stored dataset length mismatch")
        with path.open(mode="rb") as stream:
            requests = read_requests(
                stream=stream, max_requests=request_limit(profile=self.limits.capacity_profile)
            )
        expected = hashlib.sha256()
        for chunk in request_bytes(requests=requests):
            expected.update(chunk)
        if (
            input_digest(records=requests) != digest
            or self._hash(path=path, maximum=MAX_UPLOAD_BYTES)[1] != expected.hexdigest()
        ):
            raise OperationError(
                code="VALIDATION_FAILED", message="Stored dataset identity mismatch"
            )
        return requests

    def save_snapshot(self, snapshot: RunSnapshot) -> FileReference:
        verify_snapshot(snapshot=snapshot)
        return self.publish(
            key=f"snapshots/{snapshot.snapshot_digest}.json",
            chunks=(canonical_bytes(value=snapshot.model_dump(mode="json")),),
            maximum=32 * 1024**2,
        )

    def load_snapshot(self, key: str, expected_digest: str) -> RunSnapshot:
        if key != f"snapshots/{expected_digest}.json":
            raise OperationError(
                code="VALIDATION_FAILED", message="Snapshot reference does not match identity"
            )
        path = self.path(key=key)
        if not path.is_file():
            raise OperationError(
                code="VALIDATION_FAILED", message="Stored snapshot is missing or unsafe"
            )
        with path.open(mode="rb") as stream:
            saved = read_snapshot(stream=stream)
        if saved.snapshot_digest != expected_digest:
            raise OperationError(
                code="VALIDATION_FAILED", message="Stored snapshot identity mismatch"
            )
        return saved

    @contextmanager
    def verified_stream(self, reference: FileReference) -> Generator[BinaryIO]:
        path = self.path(key=reference.key)
        if not path.is_file() or self._hash(path=path, maximum=reference.size_bytes) != (
            reference.size_bytes,
            reference.sha256,
        ):
            raise OperationError(code="VALIDATION_FAILED", message="Stored file identity mismatch")
        with path.open(mode="rb") as stream:
            yield stream
