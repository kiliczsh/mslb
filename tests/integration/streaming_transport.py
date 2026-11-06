"""HTTPX transport whose ASGI send waits for the streaming consumer to advance.

Unlike HTTPX's eager ASGI transport, this permits one in-delivery body chunk and
keeps the real middleware/response lifecycle suspended when the client pauses.
"""

import asyncio
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import override

import httpx
from starlette.types import ASGIApp, Message, Scope


class _Body(httpx.AsyncByteStream):
    def __init__(self, *, transport: "SlowHttpTransport") -> None:
        self.transport = transport

    @override
    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            if self.transport.finished.is_set() and self.transport.frames.empty():
                if self.transport.task is not None:
                    await self.transport.task
                return
            block = await self.transport.frames.get()
            if block is None:
                if self.transport.task is not None:
                    await self.transport.task
                return
            try:
                yield block
            finally:
                self.transport.acknowledged.set()

    @override
    async def aclose(self) -> None:
        await self.transport.aclose()


class SlowHttpTransport(httpx.AsyncBaseTransport):
    def __init__(self, *, app: ASGIApp) -> None:
        self.app = app
        self.frames: asyncio.Queue[bytes | None] = asyncio.Queue(maxsize=1)
        self.acknowledged = asyncio.Event()
        self.disconnected = asyncio.Event()
        self.finished = asyncio.Event()
        self.sending = asyncio.Event()
        self.task: asyncio.Task[None] | None = None
        self.peak_frames = 0

    @override
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if self.task is not None:
            raise AssertionError("Use a separate slow transport for each connection")
        started = asyncio.Future[tuple[int, list[tuple[bytes, bytes]]]]()
        scope: Scope = {
            "type": "http",
            "http_version": "1.1",
            "method": request.method,
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(sep=b"?", maxsplit=1)[0],
            "query_string": request.url.query,
            "headers": [(name.lower(), value) for name, value in request.headers.raw],
            "root_path": "",
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
            "asgi": {"version": "3.0", "spec_version": "2.4"},
        }
        requested = False

        async def receive() -> Message:
            nonlocal requested
            if not requested:
                requested = True
                return {"type": "http.request", "body": b"", "more_body": False}
            await self.disconnected.wait()
            return {"type": "http.disconnect"}

        async def send(message: Message) -> None:
            if self.disconnected.is_set():
                raise OSError("HTTP streaming consumer disconnected")
            if message["type"] == "http.response.start":
                started.set_result((message["status"], message.get("headers", [])))
            elif message["type"] == "http.response.body" and message.get("body"):
                self.acknowledged.clear()
                await self.frames.put(item=message["body"])
                self.peak_frames = max(self.peak_frames, self.frames.qsize())
                self.sending.set()
                try:
                    await self.acknowledged.wait()
                finally:
                    self.sending.clear()
                if self.disconnected.is_set():
                    raise OSError("HTTP streaming consumer disconnected")

        async def run() -> None:
            try:
                await self.app(scope, receive, send)
            except BaseException as error:
                if not started.done():
                    started.set_exception(error)
                raise
            finally:
                self.finished.set()
                # A retained data chunk is drained before __aiter__ can finish.
                with suppress(asyncio.QueueFull):
                    self.frames.put_nowait(item=None)

        self.task = asyncio.create_task(coro=run())
        try:
            status, headers = await asyncio.wait_for(fut=started, timeout=5)
        except BaseException:
            await self.aclose()
            raise
        return httpx.Response(
            status_code=status, headers=headers, stream=_Body(transport=self), request=request
        )

    @override
    async def aclose(self) -> None:
        self.disconnected.set()
        self.acknowledged.set()
        if self.task is not None:
            await asyncio.wait_for(fut=self.task, timeout=3)
