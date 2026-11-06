"""Guard-side read-only measurement RPC, with no controller credential or daemon access."""

import asyncio

import httpx
from mslb_contracts.resources import ResourceProbe
from pydantic import ValidationError

from mslb_controller.credentials import read_token
from mslb_controller.engine import EngineError


class ResourceClient:
    def __init__(
        self, *, token: str | None = None, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.client = httpx.AsyncClient(
            base_url="http://docker-proxy:8081",
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(timeout=4, connect=1, pool=0.1),
            limits=httpx.Limits(max_connections=1, max_keepalive_connections=1),
            headers={
                "Authorization": "Bearer "
                + (token if token is not None else read_token(role="guard")),
                "Accept-Encoding": "identity",
            },
        )

    async def probe(self) -> ResourceProbe:
        try:
            async with (
                asyncio.timeout(delay=5),
                self.client.stream(method="GET", url="/guard/probe") as response,
            ):
                if response.status_code != 200:
                    raise EngineError(code="RESOURCE_UNAVAILABLE", status=response.status_code)
                if (
                    response.headers.get(key="content-encoding", default="identity") != "identity"
                    or response.headers.get(key="content-type", default="")
                    .split(sep=";")[0]
                    .lower()
                    != "application/json"
                ):
                    raise EngineError(code="RESOURCE_RESPONSE")
                raw = bytearray()
                async for chunk in response.aiter_raw(chunk_size=4096):
                    if len(raw) + len(chunk) > 16 * 1024:
                        raise EngineError(code="RESOURCE_RESPONSE")
                    raw.extend(chunk)
                return ResourceProbe.model_validate_json(json_data=raw)
        except (httpx.HTTPError, TimeoutError):
            raise EngineError(code="RESOURCE_UNAVAILABLE") from None
        except ValidationError:
            raise EngineError(code="RESOURCE_RESPONSE") from None

    async def close(self) -> None:
        await self.client.aclose()
