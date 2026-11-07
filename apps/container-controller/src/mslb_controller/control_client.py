"""Scoped controller-side RPC: fixed private URL, no daemon socket or caller URLs."""

import asyncio
from http import HTTPStatus
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
    """Call only semantic private control operations with bounded authenticated responses."""

    def __init__(
        self, token: str | None = None, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        self.client = httpx.AsyncClient(
            base_url="http://docker-proxy:8081",
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(timeout=50, connect=1, pool=0.1),
            limits=httpx.Limits(max_connections=2, max_keepalive_connections=1),
            headers={
                "Authorization": "Bearer " + (token or read_token()),
                "Accept-Encoding": "identity",
            },
        )

    async def _post[T: BaseModel](self, path: str, payload: str, model: type[T]) -> T:
        """Send a bounded authenticated semantic operation without following redirects."""
        try:
            async with (
                asyncio.timeout(delay=52),
                self.client.stream(
                    method="POST",
                    url=path,
                    content=payload,
                    headers={"Content-Type": "application/json"},
                ) as response,
            ):
                if response.status_code != HTTPStatus.OK.value:
                    raise EngineError(code="CONTROL_REFUSED", status=response.status_code)
                if response.headers.get(key="content-encoding", default="identity") != "identity":
                    raise EngineError(code="CONTROL_RESPONSE")
                raw = bytearray()
                async for chunk in response.aiter_raw(chunk_size=4096):
                    if len(raw) + len(chunk) > 16384:
                        raise EngineError(code="CONTROL_RESPONSE")
                    raw.extend(chunk)
                return model.model_validate_json(json_data=raw)
        except (httpx.HTTPError, TimeoutError):
            raise EngineError(code="CONTROL_UNAVAILABLE") from None
        except ValidationError:
            raise EngineError(code="CONTROL_RESPONSE") from None

    async def enroll(self) -> DeploymentRecord:
        """Enroll approved static backend identities through the restricted control boundary."""
        value = await self._post(path="/control/enroll", payload="{}", model=EnrollmentReply)
        return DeploymentRecord(id=value.id, definition=value.definition)

    async def resource(
        self,
        lease: ControlLease,
        identifier: UUID,
        action: Literal["create", "inspect", "start", "cleanup"],
    ) -> ResourceReply:
        """Validate one bounded resource receipt from the private control API."""
        reference = LeaseRef(
            run_id=lease.run_id, fence=lease.fence, owner=lease.owner, phase=lease.phase
        )
        value = await self._post(
            path=f"/control/resources/{identifier}/{action}",
            payload=reference.model_dump_json(),
            model=ResourceReply,
        )
        if value.id != identifier:
            raise EngineError(code="CONTROL_RESPONSE")
        return value

    async def close(self) -> None:
        """Close the authenticated control HTTP client owned by this instance."""
        await self.client.aclose()
