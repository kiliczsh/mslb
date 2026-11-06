"""The privileged probe emits bounded numeric evidence and never mutates Docker."""

import asyncio
import json
from http import HTTPStatus

import httpx
import pytest
from mslb_contracts.resources import MIB
from mslb_controller.engine import PROJECT, EngineError
from mslb_controller.resource_probe import ProbeEngine

from tests.test_docker_engine import fixture


@pytest.mark.parametrize(
    argnames="fault",
    argvalues=[
        "none",
        "foreign",
        "unbounded",
        "inconsistent",
        "boolean",
        "oom-string",
        "restart-boolean",
    ],
)
def test_probe_reads_only_fixed_project_identity_and_immutable_id_stats(fault: str) -> None:
    async def run() -> None:
        _intent, _image, _net, template = fixture()
        info = template.model_dump()
        info["Name"] = f"/{PROJECT}-worker-1"
        info["Config"]["Labels"] = {
            "com.docker.compose.project": "other" if fault == "foreign" else PROJECT,
            "com.docker.compose.service": "worker",
            "com.docker.compose.oneoff": "False",
        }
        info["HostConfig"]["Memory"] = 0 if fault == "unbounded" else 1024 * MIB
        info["State"] = {
            "Running": True,
            "Health": {"Status": "healthy"},
            "OOMKilled": "false" if fault == "oom-string" else False,
        }
        info["RestartCount"] = True if fault == "restart-boolean" else 0
        calls: list[tuple[str, str]] = []

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append((request.method, request.url.path))
            if request.url.path.endswith("/json"):
                data = info
            else:
                assert request.url.path == f"/v1.53/containers/{template.Id}/stats"
                assert request.url.query == b"stream=false&one-shot=true"
                data = {
                    "memory_stats": {
                        "limit": 1024 * MIB,
                        "usage": True if fault == "boolean" else 1000,
                        "stats": {"inactive_file": 1001 if fault == "inconsistent" else 200},
                    }
                }
            return httpx.Response(
                status_code=HTTPStatus.OK.value,
                stream=httpx.ByteStream(stream=json.dumps(obj=data).encode()),
            )

        engine = ProbeEngine(transport=httpx.MockTransport(handler=respond))
        try:
            if fault == "none":
                observed = await engine.service(service="worker")
                assert (
                    observed.healthy
                    and observed.working_set_bytes == 800
                    and observed.usage_bytes == 1000
                )
                assert observed.container_id == template.Id
            else:
                with pytest.raises(expected_exception=EngineError):
                    await engine.service(service="worker")
            assert all(method == "GET" for method, _path in calls)
            assert calls[0][1] == f"/v1.53/containers/{PROJECT}-worker-1/json"
        finally:
            await engine.close()

    asyncio.run(main=run())
