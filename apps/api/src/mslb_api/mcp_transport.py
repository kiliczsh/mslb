"""Observe MCP HTTP disconnects without competing for the SDK's receive channel."""

import asyncio
from collections.abc import Awaitable

from anyio import CancelScope
from mslb_application.errors import OperationError
from starlette.types import ASGIApp, Message, Receive, Scope, Send

DISCONNECTED = "mcp_disconnected"


class DisconnectAware:
    def __init__(self, *, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        disconnected = asyncio.Event()
        scope.setdefault("state", {})[DISCONNECTED] = disconnected

        async def observed_receive() -> Message:
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected.set()
            return message

        async def observed_send(message: Message) -> None:
            try:
                await send(message)
            except OSError:
                disconnected.set()
                raise

        try:
            await self.app(scope, observed_receive, observed_send)
        finally:
            # Stateful transports can keep handlers alive after their HTTP stream.
            disconnected.set()


async def while_connected[T](*, operation: Awaitable[T], disconnected: asyncio.Event | None) -> T:
    """Cancel/drain only the read operation when its HTTP request goes away."""
    if disconnected is None:
        return await operation
    task = asyncio.ensure_future(coro_or_future=operation)
    closed = asyncio.create_task(coro=disconnected.wait())
    try:
        done, _ = await asyncio.wait(fs=(task, closed), return_when=asyncio.FIRST_COMPLETED)
        if closed in done:
            # A local HTTP close must not tear down the SDK's retained session
            # (or its other requests) via an unhandled handler cancellation.
            raise OperationError(
                code="INTERRUPTED", message="MCP wait connection ended; simulations continue"
            )
        return await task
    finally:
        task.cancel()
        closed.cancel()
        # The SDK uses level-triggered AnyIO cancellation. Do not repeatedly
        # cancel SQLAlchemy's rollback/connection cleanup while draining it.
        with CancelScope(shield=True):
            drained = asyncio.gather(task, closed, return_exceptions=True)
            while not drained.done():
                try:
                    await asyncio.shield(arg=drained)
                except asyncio.CancelledError:
                    continue
