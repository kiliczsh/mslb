"""Installed public REST/MCP Docker flow with actual guarded controller execution."""

import asyncio
import hashlib
import io
import json
import os
import zipfile
from http import HTTPStatus
from uuid import uuid4

import httpx
import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextResourceContents
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_artifacts import HttpRecord
from mslb_contracts.docker_views import DockerRunDetail, DockerRunView
from mslb_contracts.http import SimulationAccepted
from mslb_contracts.pagination import Page
from pydantic import BaseModel


async def tool[T: BaseModel](
    *, client: Client, name: str, arguments: dict[str, object], model: type[T]
) -> T:
    reply = await client.call_tool(name=name, arguments=arguments)
    if reply.is_error or reply.structured_content is None:
        raise RuntimeError("Docker MCP operation failed: " + name)
    return model.model_validate_json(json_data=canonical_bytes(value=reply.structured_content))


async def detail(*, client: httpx.AsyncClient, run_id: str) -> DockerRunDetail:
    response = await client.get(url=f"/api/v1/docker/simulations/{run_id}")
    response.raise_for_status()
    return DockerRunDetail.model_validate_json(json_data=response.content)


async def main() -> None:
    """Use the installed API/worker/controller, never a synthetic admission frame."""
    headers = (
        {"Authorization": "Bearer " + os.environ["AUTH_TOKEN"]}
        if os.environ.get(key="AUTH_ENABLED") == "true"
        else {}
    )
    async with (
        asyncio.timeout(delay=60),
        httpx.AsyncClient(
            base_url="http://api:8000",
            trust_env=False,
            timeout=15,
            limits=httpx.Limits(max_connections=2),
            headers=headers,
        ) as client,
    ):
        csv = b"t,request_id,work_units,mem_mb\n" + b"".join(
            f"0,public-{index:02},1,1\n".encode() for index in range(9)
        )
        upload = await client.post(
            url="/api/v1/datasets", files={"file": ("docker-public.csv", csv, "text/csv")}
        )
        upload.raise_for_status()
        body = {
            "mode": "docker",
            "dataset_id": upload.json()["id"],
            "strategy": "round_robin",
            "initial_replicas": 3,
            "duration_seconds": 10,
        }
        key = "public-docker-" + uuid4().hex
        response = await client.post(
            url="/api/v1/simulations", json=body, headers={"Idempotency-Key": key}
        )
        if response.status_code != HTTPStatus.ACCEPTED.value:
            raise RuntimeError(
                f"Public Docker admission failed: {response.status_code}/{response.text[:512]}"
            )
        run_id = response.json()["run_id"]
        saved = await detail(client=client, run_id=run_id)
        for _ in range(120):
            if saved.run.status in {"completed", "failed", "cancelled"}:
                break
            await asyncio.sleep(delay=0.25)
            saved = await detail(client=client, run_id=run_id)
        if saved.run.status != "completed" or saved.cleanup_pending or saved.summary is None:
            raise RuntimeError(
                f"Installed Docker controller did not complete: "
                f"{saved.run.status}/{saved.run.error_code}"
            )
        if saved.summary.completed != 9 or len(saved.resources) != 3 or len(saved.artifacts) != 5:
            raise RuntimeError("Public operational result or initial membership differs")
        if not any(
            resource.kind == "dynamic" and resource.state == "removed"
            for resource in saved.resources
        ):
            raise RuntimeError("Public run did not clean its actual dynamic backend")
        events: list[HttpRecord] = []
        cursor = None
        for _ in range(8):
            page = await client.get(
                url=f"/api/v1/docker/simulations/{run_id}/events",
                params={"limit": 4} | ({"cursor": cursor} if cursor is not None else {}),
            )
            page.raise_for_status()
            events.extend(
                HttpRecord.model_validate_json(json_data=json.dumps(obj=row))
                for row in page.json()["items"]
            )
            cursor = page.json()["next_cursor"]
            if cursor is None:
                break
        else:
            raise RuntimeError("Public Docker event pagination exceeded its expected bound")
        if (
            len(events) != 27
            or len({event.backend_id for event in events if event.kind == "completed"}) != 3
        ):
            raise RuntimeError("Actual public Round Robin routing or event pagination differs")
        for artifact in saved.artifacts:
            digest, length = hashlib.sha256(), 0
            async with client.stream(
                method="GET",
                url=f"/api/v1/docker/simulations/{run_id}/artifact",
                params={"name": artifact.name},
            ) as stream:
                stream.raise_for_status()
                async for block in stream.aiter_bytes(chunk_size=65536):
                    digest.update(block)
                    length += len(block)
            if (digest.hexdigest(), length) != (artifact.sha256, artifact.size_bytes):
                raise RuntimeError("Public download differs from its verified metadata")
        archive = await client.get(url=f"/api/v1/docker/simulations/{run_id}/inputs")
        archive.raise_for_status()
        with zipfile.ZipFile(file=io.BytesIO(initial_bytes=archive.content)) as package:
            if set(package.namelist()) != {artifact.name for artifact in saved.artifacts}:
                raise RuntimeError("Public reproducibility package inventory differs")
            for artifact in saved.artifacts:
                if (
                    hashlib.sha256(string=package.read(name=artifact.name)).hexdigest()
                    != artifact.sha256
                ):
                    raise RuntimeError("Public package content differs")
        async with (
            httpx2.AsyncClient(headers=headers, trust_env=False, timeout=30) as http,
            Client(
                server=streamable_http_client(url="http://api:8000/mcp", http_client=http)
            ) as mcp,
        ):
            accepted = await tool(
                client=mcp,
                name="run_docker_simulation",
                model=SimulationAccepted,
                arguments={"simulation": body, "idempotency_key": key},
            )
            if str(object=accepted.run_id) != run_id:
                raise RuntimeError("REST/MCP idempotency produced duplicate operational work")
            through_mcp = await tool(
                client=mcp,
                name="get_docker_simulation",
                model=DockerRunDetail,
                arguments={"run_id": run_id},
            )
            if through_mcp.run != saved.run or through_mcp.artifacts != saved.artifacts:
                raise RuntimeError("REST/MCP operational identity differs")
            history = await tool(
                client=mcp,
                name="list_docker_simulations",
                model=Page[DockerRunView],
                arguments={"limit": 1},
            )
            if not history.items:
                raise RuntimeError("MCP Docker history is empty")
            first_events = await tool(
                client=mcp,
                name="get_docker_events",
                model=Page[HttpRecord],
                arguments={"run_id": run_id, "limit": 4},
            )
            if first_events.items != tuple(events[:4]):
                raise RuntimeError("REST/MCP operational events differ")
            resource = await mcp.read_resource(uri=f"mslb://docker-runs/{run_id}/summary")
            content = resource.contents[0]
            if not isinstance(content, TextResourceContents):
                raise RuntimeError("Docker resource did not return typed JSON")
            if DockerRunDetail.model_validate_json(json_data=content.text).run != saved.run:
                raise RuntimeError("Docker resource identity differs")
            cancellation_csv = b"t,request_id,work_units,mem_mb\n" + b"".join(
                f"0,cancel-{index:02},1000,1\n".encode() for index in range(48)
            )
            cancellation_upload = await client.post(
                url="/api/v1/datasets",
                files={"file": ("cancel-http.csv", cancellation_csv, "text/csv")},
            )
            cancellation_upload.raise_for_status()
            cancellation = await tool(
                client=mcp,
                name="run_docker_simulation",
                model=SimulationAccepted,
                arguments={
                    "simulation": {
                        "mode": "docker",
                        "dataset_id": cancellation_upload.json()["id"],
                        "backend_inflight": 1,
                        "global_inflight": 2,
                        "duration_seconds": 20,
                    }
                },
            )
            cancel_id = str(object=cancellation.run_id)
            active = await detail(client=client, run_id=cancel_id)
            for _ in range(100):
                if active.run.status == "running" and active.resources:
                    break
                await asyncio.sleep(delay=0.1)
                active = await detail(client=client, run_id=cancel_id)
            else:
                raise RuntimeError("Public cancellation run never acquired resources")
            await tool(
                client=mcp,
                name="cancel_docker_simulation",
                model=DockerRunDetail,
                arguments={"run_id": cancel_id},
            )
            for _ in range(100):
                active = await detail(client=client, run_id=cancel_id)
                if active.run.status == "cancelled" and not active.cleanup_pending:
                    break
                await asyncio.sleep(delay=0.1)
            else:
                raise RuntimeError("Public running cancellation did not resolve owned cleanup")
            if active.artifacts or active.summary is not None:
                raise RuntimeError("Cancelled operational work published success")
        print(
            json.dumps(
                obj={
                    "status": "Installed guarded Docker REST/MCP workflow passed",
                    "run_id": run_id,
                    "completed": 9,
                    "dynamic_removed": 1,
                    "verified_downloads": 5,
                    "events": len(events),
                    "mcp_tools": 5,
                    "mcp_resources": 1,
                    "running_cancellation_cleanup": "passed",
                }
            )
        )


if __name__ == "__main__":
    asyncio.run(main=main())
