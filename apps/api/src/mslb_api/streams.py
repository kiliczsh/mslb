"""Verified bounded file and ZIP streams with deterministic resource cleanup."""

import asyncio
import io
import queue
import time
import zipfile
from collections.abc import AsyncGenerator, Buffer, Callable
from contextlib import ExitStack
from threading import Event
from typing import BinaryIO, override

from mslb_application.errors import OperationError
from mslb_application.queries import Download
from starlette.requests import ClientDisconnect
from starlette.responses import StreamingResponse
from starlette.types import Message, Receive, Scope, Send

SEND_TIMEOUT_SECONDS = 5


async def finish[T](*, operation: Callable[[], T]) -> T:
    """Do not abandon bounded filesystem work or cleanup on repeated cancellation."""
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        drained = asyncio.gather(task, return_exceptions=True)
        while not drained.done():
            try:
                await asyncio.shield(drained)
            except asyncio.CancelledError:
                continue
        raise


class _ZipSink(io.RawIOBase):
    def __init__(self, *, pending: queue.Queue[bytes], stopped: Event) -> None:
        super().__init__()
        self.pending, self.stopped = pending, stopped

    @override
    def writable(self) -> bool:
        return True

    @override
    def write(self, b: Buffer, /) -> int:
        value = memoryview(b)
        for offset in range(0, len(value), 64 * 1024):
            deadline = time.monotonic() + 5
            while not self.stopped.is_set():
                try:
                    self.pending.put(bytes(value[offset : offset + 64 * 1024]), timeout=0.1)
                    break
                except queue.Full:
                    if time.monotonic() > deadline:
                        raise OperationError(
                            "INTERRUPTED", "Archive consumer stopped reading"
                        ) from None
            else:
                raise OperationError("INTERRUPTED", "Archive stream closed")
        return len(value)


class ArtifactResponse(StreamingResponse):
    def __init__(self, *, downloads: tuple[Download, ...], archive: bool = False) -> None:
        self.downloads, self.archive = downloads, archive
        self.stack = ExitStack()
        self.files: list[BinaryIO] = []
        self.stopped = Event()
        filename = "inputs.zip" if archive else downloads[0].filename
        headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
        if not archive:
            headers["Content-Length"] = str(downloads[0].reference.size_bytes)
            headers["ETag"] = '"' + downloads[0].reference.sha256 + '"'
        self.source = self.chunks()
        super().__init__(
            content=self.source,
            media_type="application/zip" if archive else downloads[0].media_type,
            headers=headers,
        )

    def prepare(self) -> None:
        for download in self.downloads:
            self.files.append(self.stack.enter_context(cm=download.open()))

    async def chunks(self) -> AsyncGenerator[bytes]:
        if not self.archive:
            while block := await finish(operation=lambda: self.files[0].read(64 * 1024)):
                yield block
            return
        pending: queue.Queue[bytes] = queue.Queue(maxsize=1)

        def write() -> None:
            with zipfile.ZipFile(
                file=_ZipSink(pending=pending, stopped=self.stopped),
                mode="w",
                compression=zipfile.ZIP_DEFLATED,
                compresslevel=1,
            ) as archive:
                for download, source in zip(self.downloads, self.files, strict=True):
                    with archive.open(name=download.filename, mode="w") as target:
                        while block := source.read(64 * 1024):
                            target.write(block)

        task = asyncio.create_task(asyncio.to_thread(write))
        try:
            while not task.done() or not pending.empty():
                try:
                    yield pending.get_nowait()
                except queue.Empty:
                    await asyncio.sleep(delay=0.01)
            await task
        finally:
            self.stopped.set()
            drained = asyncio.gather(task, return_exceptions=True)
            while not drained.done():
                try:
                    await asyncio.shield(drained)
                except asyncio.CancelledError:
                    continue

    @override
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def bounded_send(message: Message) -> None:
            await asyncio.wait_for(fut=send(message), timeout=SEND_TIMEOUT_SECONDS)

        try:
            await finish(operation=self.prepare)
            await super().__call__(scope, receive, bounded_send)
        except (TimeoutError, ClientDisconnect):
            return
        finally:
            self.stopped.set()
            try:
                await self.source.aclose()
            finally:
                await finish(operation=self.stack.close)


class EventResponse(StreamingResponse):
    def __init__(
        self, *, source: AsyncGenerator[bytes], media_type: str, headers: dict[str, str]
    ) -> None:
        self.source = source
        super().__init__(content=source, media_type=media_type, headers=headers)

    @override
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        async def bounded_send(message: Message) -> None:
            await asyncio.wait_for(fut=send(message), timeout=SEND_TIMEOUT_SECONDS)

        try:
            await super().__call__(scope, receive, bounded_send)
        except (TimeoutError, ClientDisconnect):
            # A full socket must release the stream slot, just like a disconnect.
            return
        finally:
            await self.source.aclose()
