"""Diagnostic contention handling is bounded and never retries non-busy failures."""

import asyncio
import secrets
from http import HTTPStatus

import httpx
import pytest
from mslb_controller.engine import EngineError
from mslb_controller.resource_client import ResourceClient

from scripts.check_resource_probe import diagnostic_probe
from tests.unit.platform.test_resource_policy import probe


@pytest.mark.parametrize(
    argnames="statuses",
    argvalues=[
        (HTTPStatus.TOO_MANY_REQUESTS.value, HTTPStatus.OK.value),
        (
            HTTPStatus.TOO_MANY_REQUESTS.value,
            HTTPStatus.TOO_MANY_REQUESTS.value,
            HTTPStatus.TOO_MANY_REQUESTS.value,
            HTTPStatus.TOO_MANY_REQUESTS.value,
        ),
        (HTTPStatus.CONFLICT.value,),
        (HTTPStatus.SERVICE_UNAVAILABLE.value,),
    ],
)
def test_diagnostic_retries_only_busy_with_a_finite_attempt_bound(
    statuses: tuple[int, ...],
) -> None:
    async def run() -> None:
        calls = 0

        def respond(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            assert request.url.path == "/guard/probe"
            status = statuses[calls]
            calls += 1
            return httpx.Response(
                status_code=status,
                stream=httpx.ByteStream(stream=probe().model_dump_json().encode()),
                headers={"Content-Type": "application/json"},
            )

        guard = ResourceClient(
            token=secrets.token_hex(nbytes=32), transport=httpx.MockTransport(handler=respond)
        )
        try:
            if statuses[-1] == HTTPStatus.OK.value:
                assert (await diagnostic_probe(guard=guard)).engine_memory_bytes > 0
            else:
                with pytest.raises(expected_exception=EngineError):
                    await diagnostic_probe(guard=guard)
            assert calls == len(statuses)
        finally:
            await guard.close()

    asyncio.run(main=run())
