"""Private bounded Unix-socket adapter; no general daemon forwarding operation."""

import asyncio
import re
from typing import Annotated

import httpx
from mslb_contracts.docker_runs import CreationIntent, DockerId, ImageId
from pydantic import BaseModel, ConfigDict, Field, ValidationError

PROJECT = "mslb"
IMAGE = "mslb-python-tools:local"
NETWORK = PROJECT + "_docker-traffic"
API_VERSION = "/v1.53"


class EngineError(Exception):
    """Safe status only; daemon/transport error text can contain privileged host details."""

    def __init__(self, code: str, status: int | None = None) -> None:
        self.code, self.status = code, status
        super().__init__(code)


class EngineModel(BaseModel):
    model_config = ConfigDict(strict=True, extra="ignore")


class ImageInfo(EngineModel):
    Id: ImageId
    Config: dict[str, object]


class NetworkInfo(EngineModel):
    Id: DockerId
    Name: Annotated[str, Field(max_length=128)]
    Driver: Annotated[str, Field(max_length=64)]
    Internal: bool
    Scope: Annotated[str, Field(max_length=32)]
    Labels: dict[str, str]


class ContainerInfo(EngineModel):
    Id: DockerId
    Image: ImageId
    Name: Annotated[str, Field(max_length=128)]
    Config: dict[str, object]
    HostConfig: dict[str, object]
    State: dict[str, object]
    NetworkSettings: dict[str, object]
    Mounts: Annotated[list[dict[str, object]], Field(max_length=8)]


class Created(EngineModel):
    Id: DockerId
    Warnings: Annotated[list[str], Field(max_length=16)] | None = None


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
            timeout=httpx.Timeout(5, connect=1, pool=0.1),
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
        expected: tuple[int, ...] = (200,),
    ) -> T | None:
        try:
            async with (
                asyncio.timeout(8),
                self.client.stream(method, API_VERSION + path, json=body) as response,
            ):
                if response.status_code not in expected:
                    raise EngineError("ENGINE_STATUS", response.status_code)
                if response.status_code in {204, 304}:
                    return None
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise EngineError("ENGINE_RESPONSE")
                raw = bytearray()
                async for chunk in response.aiter_raw(65536):
                    if len(raw) + len(chunk) > 256 * 1024:
                        raise EngineError("ENGINE_RESPONSE")
                    raw.extend(chunk)
                try:
                    return model.model_validate_json(raw)
                except ValidationError:
                    raise EngineError("ENGINE_RESPONSE") from None
        except (TimeoutError, httpx.TimeoutException):
            raise EngineError("ENGINE_TIMEOUT") from None
        except httpx.HTTPError:
            raise EngineError("ENGINE_UNAVAILABLE") from None

    async def image(self, image_id: str | None = None) -> ImageInfo:
        if image_id is not None and not re.fullmatch(r"sha256:[0-9a-f]{64}", image_id):
            raise EngineError("INVALID_REFERENCE")
        value = await self._request("GET", f"/images/{image_id or IMAGE}/json", ImageInfo)
        if value is None:
            raise EngineError("ENGINE_RESPONSE")
        return value

    async def network(self) -> NetworkInfo:
        value = await self._request("GET", f"/networks/{NETWORK}", NetworkInfo)
        if value is None:
            raise EngineError("ENGINE_RESPONSE")
        return value

    async def inspect(self, reference: str) -> ContainerInfo:
        if not re.fullmatch(
            r"[0-9a-f]{64}|mslb-backend-[ab]-1|mslb-[0-9a-f]{32}-[0-3]",
            reference,
        ):
            raise EngineError("INVALID_REFERENCE")
        value = await self._request("GET", f"/containers/{reference}/json", ContainerInfo)
        if value is None:
            raise EngineError("ENGINE_RESPONSE")
        return value

    async def create(self, intent: CreationIntent) -> Created:
        value = await self._request(
            "POST",
            f"/containers/create?name={intent.name}",
            Created,
            body=intent.daemon_body(),
            expected=(201,),
        )
        if value is None:
            raise EngineError("ENGINE_RESPONSE")
        # Preserve the ID even if a daemon warning is returned: the reservation still needs cleanup.
        return value

    async def start(self, container_id: str) -> None:
        self._id(container_id)
        await self._request(
            "POST", f"/containers/{container_id}/start", Created, expected=(204, 304)
        )

    async def stop(self, container_id: str) -> None:
        self._id(container_id)
        await self._request(
            "POST", f"/containers/{container_id}/stop?t=3", Created, expected=(204, 304)
        )

    async def remove(self, container_id: str) -> None:
        self._id(container_id)
        await self._request(
            "DELETE", f"/containers/{container_id}?force=false&v=false", Created, expected=(204,)
        )

    @staticmethod
    def _id(value: str) -> None:
        if not re.fullmatch(r"[0-9a-f]{64}", value):
            raise EngineError("INVALID_REFERENCE")

    async def close(self) -> None:
        await self.client.aclose()
