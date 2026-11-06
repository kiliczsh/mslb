"""Exercise the actual streaming transport, persisted jobs and wait admission."""

import asyncio
import json
from contextvars import ContextVar
from http import HTTPStatus
from pathlib import Path
from threading import Event
from uuid import UUID, uuid4

import httpx
import pytest
from mcp import Client
from mcp.client.streamable_http import streamable_http_client
from mcp.server import ServerRequestContext
from mcp.types import CallToolResult
from mslb_api.mcp_adapter import McpAdapter
from mslb_application import waiting
from mslb_contracts.mcp import WaitArguments
from mslb_contracts.queries import WaitResult
from mslb_persistence.reads import Reads
from mslb_persistence.records import Lease
from sqlalchemy import event
from sqlalchemy.engine import Connection, ExecutionContext

from tests.integration.mcp_http import eventually, http_workspace, worker


def waited(*, reply: CallToolResult) -> WaitResult:
    assert not reply.is_error, reply.content
    return WaitResult.model_validate_json(json_data=json.dumps(obj=reply.structured_content))


def packets(*, payload: str) -> list[dict[str, object]]:
    return [
        json.loads(s=line.removeprefix("data:"))
        for line in payload.splitlines()
        if line.startswith("data:") and line.removeprefix("data:").strip()
    ]


def test_stateful_disconnect_releases_tokens_and_rejects_active_token_reuse(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with http_workspace(root=tmp_path) as workspace:
            run_id = await workspace.enqueue()
            base = {
                "Accept": "application/json,text/event-stream",
                "MCP-Protocol-Version": "2025-11-25",
            }
            async with httpx.AsyncClient(timeout=5) as raw:
                opened = await raw.post(
                    url=workspace.url,
                    headers=base,
                    json={
                        "jsonrpc": "2.0",
                        "id": "init",
                        "method": "initialize",
                        "params": {
                            "protocolVersion": "2025-11-25",
                            "capabilities": {},
                            "clientInfo": {"name": "stateful-wait-check", "version": "1"},
                        },
                    },
                )
                assert opened.status_code == HTTPStatus.OK.value, opened.text
                headers = base | {"Mcp-Session-Id": opened.headers["mcp-session-id"]}
                initialized = await raw.post(
                    url=workspace.url,
                    headers=headers,
                    json={
                        "jsonrpc": "2.0",
                        "method": "notifications/initialized",
                        "params": {},
                    },
                )
                assert initialized.status_code == HTTPStatus.ACCEPTED.value

                def body(*, request_id: str, seconds: int) -> dict[str, object]:
                    return {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "tools/call",
                        "params": {
                            "name": "wait_for_simulations",
                            "arguments": {
                                "run_ids": [str(run_id)],
                                "wait_seconds": seconds,
                            },
                            "_meta": {"progressToken": "one-active-token"},
                        },
                    }

                async with raw.stream(
                    method="POST",
                    url=workspace.url,
                    headers=headers,
                    json=body(request_id="first", seconds=20),
                ) as first:
                    assert first.status_code == HTTPStatus.OK.value
                    async for line in first.aiter_lines():
                        if line.startswith("data:"):
                            item = json.loads(s=line.removeprefix("data:"))
                            if item.get("method") == "notifications/progress":
                                assert item["params"]["progressToken"] == "one-active-token"
                                assert item["params"]["progress"] == 1
                                assert "total" not in item["params"]
                                break
                    else:
                        pytest.fail("Stateful HTTP did not deliver progress")
                    duplicate = await raw.post(
                        url=workspace.url,
                        headers=headers,
                        json=body(request_id="duplicate", seconds=0),
                    )
                    packet = packets(payload=duplicate.text)[-1]
                    assert "result" in packet, duplicate.text
                    result = CallToolResult.model_validate(obj=packet["result"])
                    assert (
                        result.is_error
                        and result.structured_content["error"]["code"] == "VALIDATION_ERROR"
                    )
                await eventually(predicate=lambda: workspace.adapter.wait_calls._value == 2)
                assert workspace.adapter.progress_tokens == set()
                assert (
                    await workspace.adapter.services.queries.run(run_id=run_id)
                ).status == "queued"
                reused = await raw.post(
                    url=workspace.url, headers=headers, json=body(request_id="reused", seconds=0)
                )
                result = CallToolResult.model_validate(
                    obj=packets(payload=reused.text)[-1]["result"]
                )
                assert not result.is_error and result.structured_content["timed_out"]
                assert workspace.adapter.progress_tokens == set()

    asyncio.run(main=scenario())


@pytest.mark.parametrize("opt_in", [True, False])
def test_official_http_progress_precedes_result_and_only_reports_persisted_state(
    tmp_path: Path, opt_in: bool
) -> None:
    async def scenario() -> None:
        async with http_workspace(root=tmp_path) as workspace:
            run_id = await workspace.enqueue()
            release, held = Event(), asyncio.Event()
            loop = asyncio.get_running_loop()
            execution = worker(workspace=workspace)

            def observe(phase: str, lease: Lease, tick: int) -> None:
                if phase == "candidate_published":
                    loop.call_soon_threadsafe(held.set)
                    assert release.wait(timeout=10), "Worker publication was never released"

            execution.observer = observe
            updates: list[tuple[float, float, float | None, str | None]] = []

            async def progress(progress: float, total: float | None, message: str | None) -> None:
                updates.append((loop.time(), progress, total, message))

            async with Client(server=streamable_http_client(url=workspace.url)) as client:
                assert client.server_info is not None
                assert client.server_info.name == "mslb"
                assert client.server_info.version == "2.0.0"
                task = asyncio.create_task(
                    coro=client.call_tool(
                        name="wait_for_simulations",
                        arguments={"run_ids": [str(run_id)], "wait_seconds": 8},
                        progress_callback=progress if opt_in else None,
                    )
                )
                if opt_in:
                    await eventually(predicate=lambda: len(updates) == 1)
                    assert "queued" in str(updates[0][3]) and not task.done()
                running = asyncio.create_task(coro=execution.run_once())
                try:
                    await asyncio.wait_for(fut=held.wait(), timeout=5)
                    intermediate = waited(
                        reply=await client.call_tool(
                            name="wait_for_simulations",
                            arguments={"run_ids": [str(run_id)], "wait_seconds": 0},
                        )
                    )
                    row = intermediate.statuses[0]
                    assert row.status == "running" and row.validation is None
                    assert row.progress is not None and row.progress.kind == "terminal"
                    assert intermediate.timed_out and not intermediate.all_terminal
                    if opt_in:
                        await eventually(predicate=lambda: len(updates) >= 2)
                        assert "running" in str(updates[-1][3])
                        assert "committed events" in str(updates[-1][3])
                        count = len(updates)
                        await asyncio.sleep(delay=1.1)
                        assert len(updates) == count, "Unchanged persisted state emitted progress"
                    assert not task.done(), "Wait mistook terminal output for published completion"
                finally:
                    release.set()
                    assert await asyncio.wait_for(fut=running, timeout=5)
                result = waited(reply=await asyncio.wait_for(fut=task, timeout=5))
                assert result.all_terminal and not result.timed_out
                assert result.statuses[0].status == "completed"
                assert result.statuses[0].attempts == 1
                receipt = result.statuses[0].validation
                assert receipt is not None and receipt.independent_replay == "passed"
                await asyncio.sleep(delay=0.05)
                if opt_in:
                    assert [update[1] for update in updates] == list(range(1, len(updates) + 1))
                    assert all(update[2] is None for update in updates)
                    assert all(
                        b[0] - a[0] >= 0.95 for a, b in zip(updates, updates[1:], strict=False)
                    )
                else:
                    assert updates == []
                assert workspace.adapter.progress_tokens == set()
                assert workspace.adapter.wait_calls._value == 2

    asyncio.run(main=scenario())


def test_four_strategy_http_batch_wait_measures_protocol_and_database_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with http_workspace(root=tmp_path) as workspace:
            dataset_id = await workspace.dataset(count=251)
            in_wait = ContextVar("measure_wait", default=False)
            reads, batches, notifications = 0, 0, 0
            opted_in = False
            original = McpAdapter.wait

            async def measured(
                self: McpAdapter, *, context: ServerRequestContext[object], argument: WaitArguments
            ) -> object:
                nonlocal opted_in
                opted_in = (
                    context.meta is not None and context.meta.get("progress_token") is not None
                )
                marker = in_wait.set(True)
                try:
                    return await original(self=self, context=context, argument=argument)
                finally:
                    in_wait.reset(marker)

            def sql_read(
                connection: Connection,
                cursor: object,
                statement: str,
                parameters: object,
                context: ExecutionContext,
                executemany: bool,
            ) -> None:
                nonlocal reads, batches
                if in_wait.get() and statement.lstrip().upper().startswith("SELECT"):
                    reads += 1
                    if " IN (" in statement and "output_progress" in statement:
                        batches += 1

            async def progress(progress: float, total: float | None, message: str | None) -> None:
                nonlocal notifications
                notifications += 1
                assert progress == notifications and total is None
                assert message is not None and "Persisted runs:" in message

            monkeypatch.setattr(target=McpAdapter, name="wait", value=measured)
            event.listen(
                target=workspace.database.engine.sync_engine,
                identifier="before_cursor_execute",
                fn=sql_read,
            )
            async with Client(server=streamable_http_client(url=workspace.url)) as client:
                ids = []
                for strategy in (
                    "first_fit",
                    "round_robin",
                    "least_connections",
                    "most_available_memory",
                ):
                    accepted = await client.call_tool(
                        name="run_simulation",
                        arguments={
                            "simulation": {"dataset_id": str(dataset_id), "strategy": strategy}
                        },
                    )
                    assert not accepted.is_error
                    ids.append(str(accepted.structured_content["run_id"]))
                execution = worker(workspace=workspace)

                async def run_all() -> None:
                    for _ in ids:
                        assert await execution.run_once()

                task = asyncio.create_task(
                    coro=client.call_tool(
                        name="wait_for_simulations",
                        arguments={"run_ids": ids, "wait_seconds": 25},
                        progress_callback=progress,
                    )
                )
                running = asyncio.create_task(coro=run_all())
                try:
                    result = waited(reply=await asyncio.wait_for(fut=task, timeout=28))
                    assert result.all_terminal and not result.timed_out
                    assert all(
                        row.status == "completed" and row.validation is not None
                        for row in result.statuses
                    )
                    assert opted_in and 1 <= notifications <= 26
                    assert 1 <= batches <= 52 and reads == batches + 12
                finally:
                    await asyncio.wait_for(fut=running, timeout=10)
                    event.remove(
                        target=workspace.database.engine.sync_engine,
                        identifier="before_cursor_execute",
                        fn=sql_read,
                    )
                for run_id in ids:
                    reply = await client.call_tool(
                        name="get_simulation_metrics", arguments={"run_id": run_id}
                    )
                    assert (
                        not reply.is_error
                        and reply.structured_content["summary"]["finished"] == 251
                    )
                print(
                    json.dumps(
                        obj={
                            "workload": "4 strategies x 251 requests",
                            "transport": "official SDK real HTTP",
                            "run_calls": 4,
                            "status_calls": 0,
                            "wait_calls": 1,
                            "metrics_calls": 4,
                            "progress_notifications": notifications,
                            "wait_database_selects": reads,
                            "batch_selects": batches,
                            "publication_selects": 12,
                            "all_validated_and_published": True,
                        },
                        sort_keys=True,
                    )
                )

    asyncio.run(main=scenario())


@pytest.mark.parametrize("ending", ["timeout", "disconnect", "cancel"])
def test_wait_cleanup_preserves_execution_and_reconnects(tmp_path: Path, ending: str) -> None:
    async def scenario() -> None:
        async with http_workspace(root=tmp_path) as workspace:
            run_id = await workspace.enqueue()
            release, held = Event(), asyncio.Event()
            loop = asyncio.get_running_loop()
            execution = worker(workspace=workspace)

            def observe(phase: str, lease: Lease, tick: int) -> None:
                if phase == "terminal_committed":
                    loop.call_soon_threadsafe(held.set)
                    assert release.wait(timeout=10), "Test failed to release validation"

            execution.observer = observe
            running = asyncio.create_task(coro=execution.run_once())
            try:
                await asyncio.wait_for(fut=held.wait(), timeout=5)
                if ending == "disconnect":
                    # Close a real response socket without a protocol cancellation.
                    body = {
                        "jsonrpc": "2.0",
                        "id": "wait-disconnect",
                        "method": "tools/call",
                        "params": {
                            "name": "wait_for_simulations",
                            "arguments": {"run_ids": [str(run_id)], "wait_seconds": 20},
                            "_meta": {
                                "progressToken": 0,
                                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                                "io.modelcontextprotocol/clientCapabilities": {},
                            },
                        },
                    }
                    async with (
                        httpx.AsyncClient(timeout=5) as raw,
                        raw.stream(
                            method="POST",
                            url=workspace.url,
                            json=body,
                            headers={
                                "Accept": "application/json,text/event-stream",
                                "MCP-Protocol-Version": "2026-07-28",
                                "MCP-Method": "tools/call",
                                "MCP-Name": "wait_for_simulations",
                            },
                        ) as response,
                    ):
                        assert response.status_code == HTTPStatus.OK.value, response.text
                        async for line in response.aiter_lines():
                            if line.startswith("data:"):
                                notification = json.loads(s=line.removeprefix("data:"))
                                if notification.get("method") == "notifications/progress":
                                    assert notification["params"]["progressToken"] == 0
                                    break
                        else:
                            pytest.fail("No progress before disconnect")
                else:
                    async with Client(server=streamable_http_client(url=workspace.url)) as client:
                        task = asyncio.create_task(
                            coro=client.call_tool(
                                name="wait_for_simulations",
                                arguments={
                                    "run_ids": [str(run_id)],
                                    "wait_seconds": 1 if ending == "timeout" else 20,
                                },
                            )
                        )
                        if ending == "timeout":
                            result = waited(reply=await asyncio.wait_for(fut=task, timeout=4))
                            assert result.timed_out and result.statuses[0].status == "running"
                        else:
                            await eventually(
                                predicate=lambda: workspace.adapter.wait_calls._value == 1
                            )
                            task.cancel()
                            with pytest.raises(asyncio.CancelledError):
                                await task
                await eventually(predicate=lambda: workspace.adapter.wait_calls._value == 2)
                assert workspace.adapter.progress_tokens == set()
                active = await workspace.adapter.services.queries.run(run_id=run_id)
                assert active.status == "running" and active.attempts == 1
                assert not running.done(), "Wait cleanup cancelled or ended background execution"
            finally:
                release.set()
                assert await asyncio.wait_for(fut=running, timeout=5)
            async with Client(server=streamable_http_client(url=workspace.url)) as reconnected:
                result = waited(
                    reply=await reconnected.call_tool(
                        name="wait_for_simulations", arguments={"run_ids": [str(run_id)]}
                    )
                )
                assert result.all_terminal and result.statuses[0].status == "completed"
                assert (
                    result.statuses[0].attempts == 1 and result.statuses[0].validation is not None
                )

    asyncio.run(main=scenario())


def test_wait_admission_leaves_crud_and_cancellation_available(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with http_workspace(root=tmp_path) as workspace:
            run_id = await workspace.enqueue()
            async with Client(server=streamable_http_client(url=workspace.url)) as client:
                tasks = [
                    asyncio.create_task(
                        coro=client.call_tool(
                            name="wait_for_simulations", arguments={"run_ids": [str(run_id)]}
                        )
                    )
                    for _ in range(2)
                ]
                try:
                    await eventually(predicate=lambda: workspace.adapter.wait_calls.locked())
                    excess = await client.call_tool(
                        name="wait_for_simulations", arguments={"run_ids": [str(run_id)]}
                    )
                    assert (
                        excess.is_error
                        and excess.structured_content["error"]["code"] == "RATE_LIMITED"
                    )
                    assert workspace.adapter.calls._value == 8
                    assert not (await client.call_tool(name="list_servers", arguments={})).is_error
                    assert not (
                        await client.call_tool(
                            name="create_server",
                            arguments={
                                "id": "wait-test",
                                "cpu_units_per_tick": 1,
                                "mem_mb": 1,
                                "rate_limit_per_sec": 1,
                            },
                        )
                    ).is_error
                    assert not (
                        await client.call_tool(
                            name="delete_server",
                            arguments={"server_id": "wait-test", "expected_version": 1},
                        )
                    ).is_error
                    assert not (
                        await client.call_tool(
                            name="cancel_simulation", arguments={"run_id": str(run_id)}
                        )
                    ).is_error
                    replies = await asyncio.wait_for(fut=asyncio.gather(*tasks), timeout=3)
                    assert all(
                        waited(reply=reply).statuses[0].status == "cancelled" for reply in replies
                    )
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                assert workspace.adapter.wait_calls._value == 2

    asyncio.run(main=scenario())


def test_wait_batch_order_failure_unknown_input_and_dependency_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        async with http_workspace(root=tmp_path) as workspace:
            first = await workspace.enqueue()
            second = await workspace.enqueue()
            third = await workspace.enqueue()
            jobs = workspace.adapter.services.jobs
            lease = await jobs.claim(owner="failed-test-owner")
            assert lease is not None and lease.run_id == first
            await jobs.fail(lease=lease, error_code="VALIDATION_FAILED")
            await jobs.cancel(run_id=second)
            async with Client(server=streamable_http_client(url=workspace.url)) as client:
                result = waited(
                    reply=await client.call_tool(
                        name="wait_for_simulations",
                        arguments={
                            "run_ids": [str(third), str(second), str(first)],
                            "wait_seconds": 0,
                        },
                    )
                )
                assert [row.run_id for row in result.statuses] == [third, second, first]
                assert [row.status for row in result.statuses] == ["queued", "cancelled", "failed"]
                assert result.statuses[-1].error_code == "VALIDATION_FAILED" and result.timed_out
                invalid = [
                    ({"run_ids": []}, "VALIDATION_ERROR"),
                    ({"run_ids": [str(first)] * 2}, "VALIDATION_ERROR"),
                    ({"run_ids": [str(uuid4()) for _ in range(9)]}, "VALIDATION_ERROR"),
                    ({"run_ids": ["not-a-uuid"]}, "VALIDATION_ERROR"),
                    ({"run_ids": [str(first), str(uuid4())]}, "NOT_FOUND"),
                    *[
                        ({"run_ids": [str(first)], "wait_seconds": value}, "VALIDATION_ERROR")
                        for value in (-1, 26, True, "2", 1.5)
                    ],
                    ({"run_ids": [str(first)], "extra": 1}, "VALIDATION_ERROR"),
                ]
                for arguments, code in invalid:
                    reply = await client.call_tool(name="wait_for_simulations", arguments=arguments)
                    assert reply.is_error and reply.structured_content["error"]["code"] == code

                async def unavailable(
                    self: Reads, *, run_ids: tuple[UUID, ...]
                ) -> tuple[object, ...]:
                    raise OSError("private dependency password and path")

                with monkeypatch.context() as patch:
                    patch.setattr(target=Reads, name="runs_by_ids", value=unavailable)
                    reply = await client.call_tool(
                        name="wait_for_simulations", arguments={"run_ids": [str(first)]}
                    )
                    assert (
                        reply.is_error
                        and reply.structured_content["error"]["code"] == "RESOURCE_PRESSURE"
                    )
                    assert "private" not in reply.model_dump_json()
                assert workspace.adapter.wait_calls._value == 2
                assert not (await client.call_tool(name="list_servers", arguments={})).is_error

                async def stalled(self: Reads, *, run_ids: tuple[UUID, ...]) -> tuple[object, ...]:
                    await asyncio.sleep(delay=60)
                    raise AssertionError("Dependency deadline was not enforced")

                with monkeypatch.context() as patch:
                    patch.setattr(target=Reads, name="runs_by_ids", value=stalled)
                    patch.setattr(target=waiting, name="READ_SECONDS", value=0.05)
                    reply = await asyncio.wait_for(
                        fut=client.call_tool(
                            name="wait_for_simulations", arguments={"run_ids": [str(first)]}
                        ),
                        timeout=2,
                    )
                    assert (
                        reply.is_error
                        and reply.structured_content["error"]["code"] == "RESOURCE_PRESSURE"
                    )
                assert workspace.adapter.wait_calls._value == 2

                assert await worker(workspace=workspace).run_once()
                published = await workspace.adapter.services.queries.durable.published(run_id=third)
                workspace.outputs.path(key=published.key).write_bytes(data=b"corrupt")
                reply = await client.call_tool(
                    name="wait_for_simulations", arguments={"run_ids": [str(third)]}
                )
                assert (
                    reply.is_error
                    and reply.structured_content["error"]["code"] == "VALIDATION_FAILED"
                )
                assert workspace.adapter.wait_calls._value == 2

    asyncio.run(main=scenario())
