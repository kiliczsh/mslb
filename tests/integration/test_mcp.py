import asyncio
import io
from collections.abc import Mapping
from http import HTTPStatus
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from jsonschema import Draft202012Validator
from mcp import Client, MCPError
from mcp.client.streamable_http import streamable_http_client
from mcp.types import TextContent, TextResourceContents
from mslb_api.app import create_app, get_mcp
from mslb_api.mcp_catalog import TOOLS, output_schema
from mslb_api.settings import Settings
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.services import Services
from mslb_application.worker import Worker
from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.catalog import ServerView
from mslb_contracts.http import RunDetail
from mslb_contracts.queries import JsonValue
from pydantic import TypeAdapter
from sqlalchemy.exc import DBAPIError

from tests.integration.helpers import scratch


async def call(
    client: Client, name: str, arguments: Mapping[str, object] | None = None, error: bool = False
) -> dict[str, JsonValue]:
    result = await client.call_tool(name, dict(arguments) if arguments is not None else None)
    assert result.is_error == error, result.content
    value = TypeAdapter(dict[str, JsonValue]).validate_python(result.structured_content)
    contract = next(tool for tool in TOOLS if tool.name == name)
    Draft202012Validator(output_schema(contract)).validate(value)
    assert len(result.model_dump_json().encode()) < 1024**2
    return value


def test_official_client_discovers_and_exercises_all_tools_resources_and_prompts(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"mcp-test-signing-key" * 2,
            )
            adapter = get_mcp(app)
            async with app.router.lifespan_context(app):
                source = (
                    "t,request_id,work_units,mem_mb\n"
                    + "".join(f"0,r{i:04d},1,1\n" for i in range(251))
                ).encode()
                dataset = await adapter.services.submissions.upload(
                    io.BytesIO(source), "requests.csv"
                )
                async with Client(adapter.server) as client:
                    tools = await client.list_tools()
                    assert {tool.name for tool in tools.tools} == {tool.name for tool in TOOLS}
                    for tool in tools.tools:
                        Draft202012Validator.check_schema(tool.input_schema)
                        assert tool.output_schema is not None
                        Draft202012Validator.check_schema(tool.output_schema)
                    servers = await call(client, "list_servers", {"limit": 1})
                    assert servers["next_cursor"] is not None
                    second = await call(
                        client, "list_servers", {"limit": 1, "cursor": servers["next_cursor"]}
                    )
                    assert servers["items"] != second["items"]
                    created = await call(
                        client,
                        "create_server",
                        {
                            "id": " s3/test ",
                            "cpu_units_per_tick": 1,
                            "mem_mb": 10,
                            "rate_limit_per_sec": 1,
                        },
                    )
                    assert created["id"] == "s3/test"
                    assert (await call(client, "get_server", {"server_id": "s3/test"}))[
                        "version"
                    ] == 1
                    update = {
                        "server_id": "s3/test",
                        "expected_version": 1,
                        "changes": {"mem_mb": 20},
                    }
                    assert (await call(client, "update_server", update))["version"] == 2
                    stale = await call(client, "update_server", update, error=True)
                    assert isinstance(stale["error"], dict) and stale["error"]["code"] == "CONFLICT"
                    assert (
                        await call(
                            client, "delete_server", {"server_id": "s3/test", "expected_version": 2}
                        )
                    )["deleted"] is True
                    strategies = await call(client, "list_strategies")
                    assert isinstance(strategies["items"], list) and len(strategies["items"]) == 4
                    ids: list[str] = []
                    worker = Worker(database, inputs, outputs)
                    for strategy in (
                        "first_fit",
                        "round_robin",
                        "least_connections",
                        "most_available_memory",
                    ):
                        arguments = {
                            "simulation": {"dataset_id": str(dataset.id), "strategy": strategy},
                            "idempotency_key": strategy,
                        }
                        accepted = await call(client, "run_simulation", arguments)
                        repeated = await call(client, "run_simulation", arguments)
                        assert accepted == repeated
                        run_id = accepted["run_id"]
                        assert isinstance(run_id, str)
                        ids.append(run_id)
                        assert await worker.run_once()
                        status = await call(client, "get_simulation_status", {"run_id": run_id})
                        assert isinstance(status["configuration"], dict)
                        assert status["configuration"]["mode"] == "sequential"
                        assert (await call(client, "get_simulation_status", {"run_id": run_id}))[
                            "status"
                        ] == "completed"
                    waited = await call(
                        client=client, name="wait_for_simulations", arguments={"run_ids": ids}
                    )
                    assert waited["all_terminal"] is True and waited["timed_out"] is False
                    metrics = await call(client, "get_simulation_metrics", {"run_id": ids[0]})
                    assert (
                        isinstance(metrics["summary"], dict)
                        and metrics["summary"]["finished"] == 251
                    )
                    chart = await call(
                        client,
                        "get_simulation_metrics",
                        {"run_id": ids[0], "metric": "cpu_utilization", "max_points": 3},
                    )
                    assert isinstance(chart["points"], list) and len(chart["points"]) <= 3
                    assert (await call(client, "compare_simulations", {"run_ids": ids}))[
                        "baseline_run_id"
                    ] == ids[0]
                    sequences: list[int] = []
                    cursor = None
                    while True:
                        page = await call(
                            client,
                            "get_simulation_events",
                            {"run_id": ids[0], "limit": 500, "cursor": cursor},
                        )
                        items = page["items"]
                        assert isinstance(items, list)
                        for item in items:
                            assert isinstance(item, dict) and isinstance(item["sequence"], int)
                            sequences.append(item["sequence"])
                        cursor = page["next_cursor"]
                        if cursor is None:
                            break
                    assert sequences == list(range(1, 754))
                    invalid = await call(
                        client,
                        "get_simulation_events",
                        {"run_id": ids[0], "limit": 501},
                        error=True,
                    )
                    assert (
                        isinstance(invalid["error"], dict)
                        and invalid["error"]["code"] == "VALIDATION_ERROR"
                    )
                    resources = await client.list_resources()
                    templates = await client.list_resource_templates()
                    assert len(resources.resources) == 2 and len(templates.resource_templates) == 2
                    for uri in (
                        "mslb://servers",
                        "mslb://strategies",
                        f"mslb://runs/{ids[0]}/summary",
                        f"mslb://runs/{ids[0]}/events",
                    ):
                        resource = await client.read_resource(uri)
                        assert len(resource.contents) == 1
                        content = resource.contents[0]
                        assert isinstance(content, TextResourceContents)
                        payload = TypeAdapter(dict[str, JsonValue]).validate_json(content.text)
                        if uri == "mslb://servers":
                            assert payload["follow_on_tool"] == "list_servers"
                        if uri.endswith("/events"):
                            assert isinstance(payload["page"], dict)
                            items = payload["page"]["items"]
                            assert isinstance(items, list) and len(items) == 100
                            assert payload["page"]["next_cursor"] is not None
                            assert payload["follow_on_tool"] == "get_simulation_events"
                    with pytest.raises(MCPError) as unknown:
                        await client.read_resource("mslb://unregistered")
                    assert isinstance(unknown.value.data, dict)
                    assert unknown.value.data["error"]["code"] == "NOT_FOUND"
                    prompts = await client.list_prompts()
                    assert {prompt.name for prompt in prompts.prompts} == {
                        "analyze_performance",
                        "compare_strategies",
                        "suggest_autoscaling",
                    }
                    for name in ("analyze_performance", "suggest_autoscaling"):
                        prompt = await client.get_prompt(name, {"run_id": ids[0]})
                        assert isinstance(prompt.messages[0].content, TextContent)
                        assert ids[0] in prompt.messages[0].content.text
                    await client.get_prompt("compare_strategies", {"dataset_id": str(dataset.id)})
                    assert len(await adapter.services.queries.reads.runs()) == 4
                    queued = await call(
                        client, "run_simulation", {"simulation": {"dataset_id": str(dataset.id)}}
                    )
                    assert (await call(client, "cancel_simulation", {"run_id": queued["run_id"]}))[
                        "status"
                    ] == "cancelled"

    asyncio.run(scenario())


def test_tool_and_resource_failures_redact_dependency_details_and_leave_rest_usable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken(self: Services, run_id: UUID) -> RunDetail:
        raise DBAPIError("private-query", {}, OSError("private-value"), False)

    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"failure-check-signing-key" * 2,
            )
            async with app.router.lifespan_context(app):
                monkeypatch.setattr(Services, "detail", broken)
                async with Client(get_mcp(app).server) as client:
                    result = await call(
                        client,
                        "get_simulation_status",
                        {"run_id": "00000000-0000-0000-0000-000000000001"},
                        error=True,
                    )
                    assert (
                        isinstance(result["error"], dict)
                        and result["error"]["code"] == "RESOURCE_PRESSURE"
                    )
                    assert "private-" not in str(result)
                    with pytest.raises(MCPError) as failure:
                        await client.read_resource(
                            "mslb://runs/00000000-0000-0000-0000-000000000001/summary"
                        )
                    assert "private-" not in str(failure.value.data)
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver"
                ) as rest:
                    assert (await rest.get("/api/v1/servers")).status_code == HTTPStatus.OK.value
                assert not await get_mcp(app).services.queries.reads.runs()

    asyncio.run(scenario())


def test_mcp_operation_admission_is_bounded_without_waiting_for_a_free_slot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",)),
                database,
                inputs,
                outputs,
                b"admission-check-signing-key" * 2,
            )
            entered, release = asyncio.Event(), asyncio.Event()
            count = 0
            original = Services.get_server

            async def blocked(self: Services, server_id: str) -> ServerView:
                nonlocal count
                count += 1
                if count == 8:
                    entered.set()
                await release.wait()
                return await original(self, server_id)

            async with app.router.lifespan_context(app):
                monkeypatch.setattr(Services, "get_server", blocked)
                async with Client(get_mcp(app).server) as client:
                    held = [
                        asyncio.create_task(client.call_tool("get_server", {"server_id": "s1"}))
                        for _ in range(8)
                    ]
                    try:
                        await asyncio.wait_for(entered.wait(), 5)
                        rejected = await call(client, "get_server", {"server_id": "s1"}, error=True)
                        assert (
                            isinstance(rejected["error"], dict)
                            and rejected["error"]["code"] == "RATE_LIMITED"
                        )
                    finally:
                        release.set()
                    results = await asyncio.gather(*held)
                    assert all(not reply.is_error for reply in results)
                    assert not (await client.call_tool("get_server", {"server_id": "s1"})).is_error

    asyncio.run(scenario())


def test_stateful_http_session_limit_delete_reconnect_and_idle_cleanup(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            app = create_app(
                Settings(allowed_hosts=("testserver",), mcp_session_idle_seconds=0.5),
                database,
                inputs,
                outputs,
                b"session-check-signing-key" * 2,
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver"
                ) as client,
            ):
                opening = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-11-25",
                        "capabilities": {},
                        "clientInfo": {"name": "session-limit-test", "version": "1"},
                    },
                }
                base = {"Accept": "application/json,text/event-stream"}
                ids = []
                for _ in range(10):
                    response = await client.post("/mcp", json=opening, headers=base)
                    assert response.status_code == HTTPStatus.OK.value, response.text
                    ids.append(response.headers["mcp-session-id"])
                excess = await client.post("/mcp", json=opening, headers=base)
                assert (
                    excess.status_code == HTTPStatus.SERVICE_UNAVAILABLE.value
                    and excess.headers["retry-after"] == "1"
                )
                removed = await client.delete(
                    "/mcp",
                    headers=base | {"Mcp-Session-Id": ids[0], "MCP-Protocol-Version": "2025-11-25"},
                )
                assert removed.status_code == HTTPStatus.OK.value
                replacement = await client.post("/mcp", json=opening, headers=base)
                assert replacement.status_code == HTTPStatus.OK.value
                assert replacement.headers["mcp-session-id"] not in ids
                await asyncio.sleep(1.1)
                stale = await client.post(
                    "/mcp",
                    json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
                    headers=base | {"Mcp-Session-Id": ids[1], "MCP-Protocol-Version": "2025-11-25"},
                )
                assert stale.status_code == HTTPStatus.NOT_FOUND.value
                reopened = await client.post("/mcp", json=opening, headers=base)
                assert reopened.status_code == HTTPStatus.OK.value

    asyncio.run(scenario())


@pytest.mark.parametrize("authenticated", [False, True])
def test_official_http_client_auth_origin_and_request_correlation(
    tmp_path: Path, authenticated: bool
) -> None:
    import httpx2

    async def scenario() -> None:
        async with scratch() as database:
            inputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "inputs", StorageLimits(min_free_bytes=0)
            )
            outputs = await asyncio.to_thread(
                ManagedFiles, tmp_path / "outputs", StorageLimits(min_free_bytes=0)
            )
            token = "synthetic-mcp-credential-" * 2
            app = create_app(
                Settings(
                    auth_enabled=authenticated,
                    token=token,
                    allowed_hosts=("testserver",),
                    allowed_origins=("https://dashboard.test",),
                ),
                database,
                inputs,
                outputs,
                b"mcp-http-signing-key" * 2,
            )
            async with app.router.lifespan_context(app):
                transport = httpx2.ASGITransport(app=app)
                headers = {"Authorization": "Bearer " + token} if authenticated else {}
                request_ids: list[str] = []

                async def observed(response: httpx2.Response) -> None:
                    request_ids.append(response.headers["x-request-id"])
                    if "content-length" in response.headers:
                        assert int(response.headers["content-length"]) <= 1024**2

                async with (
                    httpx2.AsyncClient(
                        transport=transport,
                        headers=headers,
                        timeout=10,
                        event_hooks={"response": [observed]},
                    ) as http,
                    Client(
                        streamable_http_client("http://testserver/mcp", http_client=http)
                    ) as client,
                ):
                    assert client.server_info is not None
                    assert client.server_info.name == "mslb"
                    assert client.server_info.version == "2.0.0"
                    assert len((await client.list_tools()).tools) == 13
                    result = await client.call_tool("list_strategies", {})
                    assert not result.is_error and result.meta is not None
                    assert result.meta["request_id"] == request_ids[-1]
                    protocol_version = client.protocol_version
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app),
                    base_url="http://testserver",
                    headers=headers,
                ) as raw:
                    body = {
                        "jsonrpc": "2.0",
                        "id": 3,
                        "method": "tools/call",
                        "params": {
                            "name": "list_strategies",
                            "arguments": {},
                            "_meta": {
                                "io.modelcontextprotocol/protocolVersion": protocol_version,
                                "io.modelcontextprotocol/clientCapabilities": {},
                            },
                        },
                    }
                    request_headers = {
                        "Accept": "application/json,text/event-stream",
                        "MCP-Protocol-Version": protocol_version,
                    }
                    refused = await raw.post(
                        "/mcp", json=body, headers=request_headers | {"Origin": "https://bad.test"}
                    )
                    assert refused.status_code == HTTPStatus.FORBIDDEN.value
                    malformed = await raw.post(
                        "/mcp",
                        content=canonical_bytes(body),
                        headers={
                            **request_headers,
                            "Content-Type": "text/plain",
                            "MCP-Method": "tools/call",
                            "MCP-Name": "list_strategies",
                        },
                    )
                    assert malformed.status_code == HTTPStatus.BAD_REQUEST.value
                    assert malformed.text == "Invalid Content-Type header"
                    if authenticated:
                        unauthorized = await raw.post(
                            "/mcp",
                            json=body,
                            headers=request_headers | {"Authorization": "Bearer wrong"},
                        )
                        assert (
                            unauthorized.status_code == HTTPStatus.UNAUTHORIZED.value
                            and token not in unauthorized.text
                        )

    asyncio.run(scenario())
