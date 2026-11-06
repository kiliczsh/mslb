"""Bounded streaming blocks and canonical request CSV encoding."""

import csv
import io
from collections.abc import Generator, Iterable
from typing import BinaryIO

from mslb_contracts.inputs import RequestInput

from mslb_application.errors import OperationError


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
