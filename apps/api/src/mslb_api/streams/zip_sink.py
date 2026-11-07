"""Bounded backpressured output for a streaming ZIP writer."""

import io
import queue
import time
from collections.abc import Buffer
from threading import Event
from typing import override

from mslb_application.errors import OperationError


class _ZipSink(io.RawIOBase):
    """Bridge bounded ZIP output to an owned streaming transfer with backpressure."""

    def __init__(self, *, pending: queue.Queue[bytes], stopped: Event) -> None:
        super().__init__()
        self.pending, self.stopped = pending, stopped

    @override
    def writable(self) -> bool:
        """Advertise the write-only sink capability required by zipfile."""
        return True

    @override
    def write(self, b: Buffer, /) -> int:
        """Forward bounded ZIP bytes under backpressure and report the consumed byte count."""
        value = memoryview(b)
        for offset in range(0, len(value), 64 * 1024):
            deadline = time.monotonic() + 5
            while not self.stopped.is_set():
                try:
                    self.pending.put(item=bytes(value[offset : offset + 64 * 1024]), timeout=0.1)
                    break
                except queue.Full:
                    if time.monotonic() > deadline:
                        raise OperationError(
                            code="INTERRUPTED", message="Archive consumer stopped reading"
                        ) from None
            else:
                raise OperationError(code="INTERRUPTED", message="Archive stream closed")
        return len(value)
