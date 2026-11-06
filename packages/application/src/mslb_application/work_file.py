"""Bounded private attempt spool with accounted physical writes."""

from __future__ import annotations

import os
from collections.abc import Callable
from typing import TYPE_CHECKING, BinaryIO

from mslb_application.errors import OperationError

if TYPE_CHECKING:
    from mslb_application.managed_files import ManagedFiles


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
                code="STORAGE_LIMIT_EXCEEDED", message="Attempt stream exceeds its byte ceiling"
            )
        for offset in range(0, len(data), 64 * 1024):
            self.buffer.extend(data[offset : offset + 64 * 1024])
            if len(self.buffer) >= 256 * 1024:
                self.flush()

    def flush(self) -> None:
        if self.buffer:
            with self.files._lock(name=".write-lock"):
                if self.check is not None:
                    self.check()
                self.files._admit(
                    additional=len(self.buffer), scope=self.files.run_scope(key=self.key)
                )
                self.stream.seek(self.size)
                if self.stream.write(self.buffer) != len(self.buffer):
                    raise OSError("Short attempt spool write")
                self.size += len(self.buffer)
                self.buffer.clear()
        self.stream.seek(self.size)

    def sync(self) -> None:
        self.flush()
        os.fsync(fd=self.stream.fileno())
