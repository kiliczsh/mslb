"""Multipart parsing that closes upload spools on every interrupted parse."""

from typing import override

from starlette.datastructures import FormData
from starlette.formparsers import MultiPartParser

from mslb_api.streams.cleanup import finish


class _Multipart(MultiPartParser):
    @override
    async def parse(self) -> FormData:
        try:
            return await super().parse()
        except BaseException:
            # Starlette closes these spools for its own multipart exceptions.
            # Also close them when our receive budget or client cancellation fires.
            for spool in self._files_to_close_on_error:
                await finish(operation=spool.close)
            raise
