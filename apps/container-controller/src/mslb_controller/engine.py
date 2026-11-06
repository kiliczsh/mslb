"""Private bounded Unix-socket adapter; no general daemon forwarding operation."""

import asyncio
import re
from http import HTTPStatus

import httpx
from mslb_contracts.docker_ownership import CreationIntent
from pydantic import BaseModel, ValidationError

from mslb_controller.engine_models import ContainerInfo as ContainerInfo
from mslb_controller.engine_models import Created as Created
from mslb_controller.engine_models import EngineModel as EngineModel
from mslb_controller.engine_models import ImageInfo as ImageInfo
from mslb_controller.engine_models import NetworkInfo as NetworkInfo
from mslb_controller.errors import EngineError as EngineError

PROJECT = "mslb"
IMAGE = "mslb-python-runtime:local"
NETWORK = PROJECT + "_docker-traffic"
API_VERSION = "/v1.53"


class Engine:
    """Only the proxy owns this adapter/socket. Policy verifies ownership before mutations."""

    def __init__(self, transport: httpx.AsyncBaseTransport | None = None) -> None:
        limits = httpx.Limits(max_connections=2, max_keepalive_connections=1)
        self.client = httpx.AsyncClient(
            transport=transport
            if transport is not None
            else httpx.AsyncHTTPTransport(uds="/var/run/docker.sock", retries=0, limits=limits),
            base_url="http://engine",
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(timeout=5, connect=1, pool=0.1),
            limits=limits,
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    async def _request[T: BaseModel](
        self,
        method: str,
        path: str,
        model: type[T],
        *,
        body: dict[str, object] | None = None,
        expected: tuple[int, ...] = (HTTPStatus.OK.value,),
    ) -> T | None:
        """Bound daemon HTTP bytes/time and return safe status/schema failures."""
        try:
            async with (
                asyncio.timeout(delay=8),
                self.client.stream(method=method, url=API_VERSION + path, json=body) as response,
            ):
                if response.status_code not in expected:
                    raise EngineError(code="ENGINE_STATUS", status=response.status_code)
                if response.status_code in {
                    HTTPStatus.NO_CONTENT.value,
                    HTTPStatus.NOT_MODIFIED.value,
                }:
                    return None
                if response.headers.get(key="content-encoding", default="identity") != "identity":
                    raise EngineError(code="ENGINE_RESPONSE")
                raw = bytearray()
                async for chunk in response.aiter_raw(chunk_size=65536):
                    if len(raw) + len(chunk) > 256 * 1024:
                        raise EngineError(code="ENGINE_RESPONSE")
                    raw.extend(chunk)
                try:
                    return model.model_validate_json(json_data=raw)
                except ValidationError:
                    raise EngineError(code="ENGINE_RESPONSE") from None
        except (TimeoutError, httpx.TimeoutException):
            raise EngineError(code="ENGINE_TIMEOUT") from None
        except httpx.HTTPError:
            raise EngineError(code="ENGINE_UNAVAILABLE") from None

    async def image(self, image_id: str | None = None) -> ImageInfo:
        if image_id is not None and not re.fullmatch(
            pattern=r"sha256:[0-9a-f]{64}", string=image_id
        ):
            raise EngineError(code="INVALID_REFERENCE")
        value = await self._request(
            method="GET", path=f"/images/{image_id or IMAGE}/json", model=ImageInfo
        )
        if value is None:
            raise EngineError(code="ENGINE_RESPONSE")
        return value

    async def network(self) -> NetworkInfo:
        value = await self._request(method="GET", path=f"/networks/{NETWORK}", model=NetworkInfo)
        if value is None:
            raise EngineError(code="ENGINE_RESPONSE")
        return value

    async def inspect(self, reference: str) -> ContainerInfo:
        if not re.fullmatch(
            pattern=r"[0-9a-f]{64}|mslb-backend-[ab]-1|mslb-[0-9a-f]{32}-[0-3]",
            string=reference,
        ):
            raise EngineError(code="INVALID_REFERENCE")
        value = await self._request(
            method="GET", path=f"/containers/{reference}/json", model=ContainerInfo
        )
        if value is None:
            raise EngineError(code="ENGINE_RESPONSE")
        return value

    async def create(self, intent: CreationIntent) -> Created:
        value = await self._request(
            method="POST",
            path=f"/containers/create?name={intent.name}",
            model=Created,
            body=intent.daemon_body(),
            expected=(HTTPStatus.CREATED.value,),
        )
        if value is None:
            raise EngineError(code="ENGINE_RESPONSE")
        # Preserve the ID even if a daemon warning is returned: the reservation still needs cleanup.
        return value

    async def start(self, container_id: str) -> None:
        self._id(value=container_id)
        await self._request(
            method="POST",
            path=f"/containers/{container_id}/start",
            model=Created,
            expected=(HTTPStatus.NO_CONTENT.value, HTTPStatus.NOT_MODIFIED.value),
        )

    async def stop(self, container_id: str) -> None:
        self._id(value=container_id)
        await self._request(
            method="POST",
            path=f"/containers/{container_id}/stop?t=3",
            model=Created,
            expected=(HTTPStatus.NO_CONTENT.value, HTTPStatus.NOT_MODIFIED.value),
        )

    async def remove(self, container_id: str) -> None:
        self._id(value=container_id)
        await self._request(
            method="DELETE",
            path=f"/containers/{container_id}?force=false&v=false",
            model=Created,
            expected=(HTTPStatus.NO_CONTENT.value,),
        )

    @staticmethod
    def _id(value: str) -> None:
        if not re.fullmatch(pattern=r"[0-9a-f]{64}", string=value):
            raise EngineError(code="INVALID_REFERENCE")

    async def close(self) -> None:
        await self.client.aclose()
