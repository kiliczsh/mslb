"""Scoped controller-side RPC: fixed private URL, no daemon socket or caller URLs."""

import asyncio
from typing import Literal
from uuid import UUID

import httpx
from mslb_persistence.docker_records import ControlLease, DeploymentRecord
from pydantic import BaseModel, ValidationError

from mslb_controller.control_models import EnrollmentReply as EnrollmentReply
from mslb_controller.control_models import ResourceReply as ResourceReply
from mslb_controller.credentials import read_token
from mslb_controller.errors import EngineError
from mslb_controller.proxy_models import LeaseRef


class ControlClient:
    def __init__(
        self, token: str | None = None, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.client = httpx.AsyncClient(
            base_url="http://docker-proxy:8081",
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(50, connect=1, pool=0.1),
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
            headers={
                "Authorization": "Bearer " + (token or read_token()),
                "Accept-Encoding": "identity",
            },
        )

    async def _post[T: BaseModel](self, path: str, payload: str, model: type[T]) -> T:
        try:
            async with (
                asyncio.timeout(52),
                self.client.stream(
                    "POST", path, content=payload, headers={"Content-Type": "application/json"}
                ) as response,
            ):
                if response.status_code != 200:
                    raise EngineError("CONTROL_REFUSED", response.status_code)
                if response.headers.get("content-encoding", "identity") != "identity":
                    raise EngineError("CONTROL_RESPONSE")
                raw = bytearray()
                async for chunk in response.aiter_raw(4096):
                    if len(raw) + len(chunk) > 16384:
                        raise EngineError("CONTROL_RESPONSE")
                    raw.extend(chunk)
                return model.model_validate_json(raw)
        except (httpx.HTTPError, TimeoutError):
            raise EngineError("CONTROL_UNAVAILABLE") from None
        except ValidationError:
            raise EngineError("CONTROL_RESPONSE") from None

    async def enroll(self) -> DeploymentRecord:
        value = await self._post("/control/enroll", "{}", EnrollmentReply)
        return DeploymentRecord(value.id, value.definition)

    async def resource(
        self,
        lease: ControlLease,
        identifier: UUID,
        action: Literal["create", "inspect", "start", "cleanup"],
    ) -> ResourceReply:
        reference = LeaseRef(
            run_id=lease.run_id, fence=lease.fence, owner=lease.owner, phase=lease.phase
        )
        value = await self._post(
            f"/control/resources/{identifier}/{action}", reference.model_dump_json(), ResourceReply
        )
        if value.id != identifier:
            raise EngineError("CONTROL_RESPONSE")
        return value

    async def close(self) -> None:
        await self.client.aclose()
