"""Bounded private HTTP backend transport and response validation."""

import asyncio
from http import HTTPStatus

import httpx
from mslb_contracts.traffic import BackendHealth, WorkCompleted
from pydantic import BaseModel, ValidationError

from mslb_controller.errors import TrafficError
from mslb_controller.routing_models import BackendSpec, Reservation


async def _json(response: httpx.Response, model: type[BaseModel]) -> BaseModel:
    if response.status_code != HTTPStatus.OK.value:
        raise TrafficError(code="HTTP_STATUS", status_code=response.status_code)
    if (
        response.headers.get(key="content-type", default="").split(sep=";")[0].lower()
        != "application/json"
        or response.headers.get(key="content-encoding", default="identity").lower() != "identity"
    ):
        raise TrafficError(code="INVALID_RESPONSE")
    payload = bytearray()
    async for chunk in response.aiter_raw(chunk_size=65536):
        if len(payload) + len(chunk) > 65536:
            raise TrafficError(code="INVALID_RESPONSE")
        payload.extend(chunk)
    try:
        return model.model_validate_json(json_data=payload)
    except ValidationError:
        raise TrafficError(code="INVALID_RESPONSE") from None


class BackendHttp:
    """No redirects, environment proxies, retries or unbounded response/pool state."""

    def __init__(
        self,
        transport: httpx.AsyncBaseTransport | None = None,
        *,
        request_timeout_ms: int = 3000,
        health_timeout_ms: int = 1000,
    ) -> None:
        if (
            type(request_timeout_ms) is not int
            or not 100 <= request_timeout_ms <= 3000
            or type(health_timeout_ms) is not int
            or not 100 <= health_timeout_ms <= 1000
        ):
            raise ValueError("HTTP timeouts exceed the supported envelope")
        self.request_seconds = request_timeout_ms / 1000
        self.health_seconds = health_timeout_ms / 1000
        self.client = httpx.AsyncClient(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(timeout=self.request_seconds, connect=0.5, pool=0.1),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    async def health(self, spec: BackendSpec) -> BackendHealth:
        try:
            async with (
                asyncio.timeout(delay=self.health_seconds),
                self.client.stream(method="GET", url=spec.url + "/health") as response,
            ):
                value = await _json(response=response, model=BackendHealth)
            if not isinstance(value, BackendHealth) or value.backend_id != spec.id:
                raise TrafficError(code="INVALID_RESPONSE", backend_id=spec.id)
            if (
                value.logical_admission_mem_mb != spec.logical_admission_mem_mb
                or value.max_inflight != spec.max_inflight
            ):
                raise TrafficError(code="BACKEND_CONFIGURATION_MISMATCH", backend_id=spec.id)
            return value
        except TrafficError as error:
            raise TrafficError(
                code=error.code, backend_id=spec.id, status_code=error.status_code
            ) from None
        except (TimeoutError, httpx.TimeoutException):
            raise TrafficError(code="HTTP_TIMEOUT", backend_id=spec.id) from None
        except httpx.HTTPError:
            raise TrafficError(code="CONNECTION_FAILED", backend_id=spec.id) from None

    async def work(self, reservation: Reservation) -> WorkCompleted:
        spec, payload = reservation.backend.spec, reservation.request
        try:
            async with (
                asyncio.timeout(delay=self.request_seconds),
                self.client.stream(
                    method="POST", url=spec.url + "/work", json=payload.model_dump()
                ) as response,
            ):
                value = await _json(response=response, model=WorkCompleted)
            if (
                not isinstance(value, WorkCompleted)
                or value.backend_id != spec.id
                or value.request_id != payload.request_id
                or value.operations != min(payload.work_units * 2048, 2_000_000)
            ):
                raise TrafficError(code="INVALID_RESPONSE", backend_id=spec.id)
            return value
        except TrafficError as error:
            raise TrafficError(
                code=error.code, backend_id=spec.id, status_code=error.status_code
            ) from None
        except (TimeoutError, httpx.TimeoutException):
            raise TrafficError(code="HTTP_TIMEOUT", backend_id=spec.id) from None
        except httpx.HTTPError:
            raise TrafficError(code="CONNECTION_FAILED", backend_id=spec.id) from None

    async def close(self) -> None:
        await self.client.aclose()
