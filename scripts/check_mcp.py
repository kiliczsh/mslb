"""Use the official client against the default Docker MCP service and real worker."""

import asyncio
import json
import os
from pathlib import Path

import httpx
import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mslb_api.mcp_catalog import TOOLS
from mslb_contracts.queries import JsonValue
from pydantic import TypeAdapter


async def result(
    *, client: Client, name: str, arguments: dict[str, object]
) -> dict[str, JsonValue]:
    reply = await client.call_tool(name=name, arguments=arguments)
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
        uploaded = await rest.post(
            url="datasets", files={"file": ("requests.csv", source, "text/csv")}
        )
        uploaded.raise_for_status()
        dataset_id = uploaded.json()["id"]
    async with httpx2.AsyncClient(
        headers=headers, timeout=httpx2.Timeout(timeout=30, read=30)
    ) as http:
        async with Client(
            server=streamable_http_client(url="http://api:8000/mcp", http_client=http)
        ) as client:
            discovered = await client.list_tools()
            if {tool.name for tool in discovered.tools} != {tool.name for tool in TOOLS}:
                raise RuntimeError("MCP tool discovery differs from the complete catalog")
            accepted = await result(
                client=client,
                name="run_simulation",
                arguments={"simulation": {"dataset_id": dataset_id, "strategy": "round_robin"}},
            )
            run_id = accepted["run_id"]
            waits = 0
            for _ in range(2):
                status = await result(
                    client=client,
                    name="wait_for_simulations",
                    arguments={"run_ids": [run_id], "wait_seconds": 20},
                )
                waits += 1
                if status["all_terminal"] is True:
                    rows = status["statuses"]
                    if not isinstance(rows, list) or any(
                        not isinstance(row, dict) or row["status"] != "completed" for row in rows
                    ):
                        raise RuntimeError("MCP smoke run did not complete")
                    break
            else:
                raise RuntimeError("MCP worker completion exceeded smoke timeout")
            await result(client=client, name="get_simulation_metrics", arguments={"run_id": run_id})
            await client.read_resource(uri="mslb://servers")
            await client.read_resource(uri="mslb://strategies")
            await client.read_resource(uri=f"mslb://runs/{run_id}/summary")
            await client.read_resource(uri=f"mslb://runs/{run_id}/events")
            if len((await client.list_prompts()).prompts) != 3:
                raise RuntimeError("MCP prompt catalog is incomplete")
            await client.get_prompt(name="analyze_performance", arguments={"run_id": str(run_id)})
            print(
                json.dumps(
                    obj=dict(
                        run_id=run_id,
                        protocol_version=client.protocol_version,
                        status="MCP protocol workflow verified",
                        status_calls=0,
                        wait_calls=waits,
                    )
                )
            )


if __name__ == "__main__":
    asyncio.run(main=main())
