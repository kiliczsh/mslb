"""Use the official client against the default Docker MCP service and real worker."""

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
import httpx2
from mslb_api.mcp_catalog import TOOLS
from mslb_contracts.queries import JsonValue
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from pydantic import TypeAdapter


async def result(client: Client, name: str, arguments: dict[str, object]) -> dict[str, JsonValue]:
    reply = await client.call_tool(name, arguments)
    if reply.is_error:
        raise RuntimeError("MCP smoke tool did not succeed: " + name)
    return TypeAdapter(dict[str, JsonValue]).validate_python(reply.structured_content)


async def main() -> None:
    headers = (
        {"Authorization": "Bearer " + os.environ["AUTH_TOKEN"]}
        if os.environ.get("AUTH_ENABLED") == "true"
        else {}
    )
    source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
    async with httpx.AsyncClient(
        base_url="http://api:8000/api/v1/", headers=headers, timeout=30
    ) as rest:
        uploaded = await rest.post("datasets", files={"file": ("requests.csv", source, "text/csv")})
        uploaded.raise_for_status()
        dataset_id = uploaded.json()["id"]
    async with httpx2.AsyncClient(headers=headers, timeout=httpx2.Timeout(30, read=30)) as http:
        async with Client(
            streamable_http_client("http://api:8000/mcp", http_client=http)
        ) as client:
            discovered = await client.list_tools()
            if {tool.name for tool in discovered.tools} != {tool.name for tool in TOOLS}:
                raise RuntimeError("MCP tool discovery differs from the complete catalog")
            accepted = await result(
                client,
                "run_simulation",
                {"simulation": {"dataset_id": dataset_id, "strategy": "round_robin"}},
            )
            run_id = accepted["run_id"]
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                status = await result(client, "get_simulation_status", {"run_id": run_id})
                if status["status"] == "completed":
                    break
                if status["status"] in {"failed", "cancelled"}:
                    raise RuntimeError("MCP smoke run did not complete")
                await asyncio.sleep(0.1)
            else:
                raise RuntimeError("MCP worker completion exceeded smoke timeout")
            await result(client, "get_simulation_metrics", {"run_id": run_id})
            await client.read_resource("mslb://servers")
            await client.read_resource("mslb://strategies")
            await client.read_resource(f"mslb://runs/{run_id}/summary")
            await client.read_resource(f"mslb://runs/{run_id}/events")
            if len((await client.list_prompts()).prompts) != 3:
                raise RuntimeError("MCP prompt catalog is incomplete")
            await client.get_prompt("analyze_performance", {"run_id": str(run_id)})
            print(
                json.dumps(
                    dict(
                        run_id=run_id,
                        protocol_version=client.protocol_version,
                        status="MCP protocol workflow verified",
                    )
                )
            )


if __name__ == "__main__":
    asyncio.run(main())
