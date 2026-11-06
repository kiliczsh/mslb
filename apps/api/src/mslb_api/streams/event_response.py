"""Bounded event responses that release their source on disconnect or timeout."""

import asyncio
from collections.abc import AsyncGenerator
from typing import override

from starlette.requests import ClientDisconnect
from starlette.responses import StreamingResponse
from starlette.types import Message, Receive, Scope, Send


class EventResponse(StreamingResponse):
    def __init__(
        self, *, source: AsyncGenerator[bytes], media_type: str, headers: dict[str, str]
    ) -> None:
        self.source = source
        super().__init__(content=source, media_type=media_type, headers=headers)

    @override
    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        # Read the public policy at call time without an import-time dependency cycle.
        from mslb_api import streams

        async def bounded_send(message: Message) -> None:
            await asyncio.wait_for(fut=send(message), timeout=streams.SEND_TIMEOUT_SECONDS)

        try:
            await super().__call__(scope, receive, bounded_send)
        except (TimeoutError, ClientDisconnect):
            # A full socket must release the stream slot, just like a disconnect.
            return
        finally:
            await self.source.aclose()
