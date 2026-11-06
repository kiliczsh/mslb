"""Correlation metadata survives adapter callbacks and the SDK wire aliases."""

import asyncio
from unittest.mock import AsyncMock, Mock
from uuid import UUID

from mcp.server import ServerRequestContext
from mcp.types import (
    CallToolRequestParams,
    GetPromptRequestParams,
    ReadResourceRequestParams,
    TextContent,
    TextResourceContents,
)
from mslb_api.mcp_adapter import McpAdapter
from mslb_api.settings import Settings
from mslb_application.services import Services
from starlette.requests import Request


def test_response_metadata_preserves_request_correlation_for_all_result_paths() -> None:
    async def scenario() -> None:
        services = Mock(spec=Services)
        services.strategies.return_value = Services.strategies()
        services.queries = Mock()
        services.queries.run = AsyncMock(return_value=Mock(id=UUID(int=1), status="completed"))
        adapter = McpAdapter(services=services, settings=Settings())
        context = Mock(spec=ServerRequestContext)
        context.request = Request(
            scope={"type": "http", "state": {"request_id": "correlated-request"}}
        )

        success = await adapter.call_tool(
            context=context, params=CallToolRequestParams(name="list_strategies")
        )
        failure = await adapter.call_tool(
            context=context, params=CallToolRequestParams(name="unknown-tool")
        )
        resource = await adapter.read_resource(
            context=context, params=ReadResourceRequestParams(uri="mslb://strategies")
        )
        prompt = await adapter.get_prompt(
            context=context,
            params=GetPromptRequestParams(
                name="analyze_performance", arguments={"run_id": str(UUID(int=1))}
            ),
        )

        assert not success.is_error
        assert failure.is_error
        resource_content = resource.contents[0]
        prompt_content = prompt.messages[0].content
        assert isinstance(resource_content, TextResourceContents)
        assert isinstance(prompt_content, TextContent)
        assert "first_fit" in resource_content.text
        assert "completed" in prompt_content.text
        for result in (success, failure, resource, prompt):
            assert result.meta == {"request_id": "correlated-request"}
            wire = result.model_dump(mode="json", by_alias=True)
            assert wire["_meta"] == {"request_id": "correlated-request"}
            assert "meta" not in wire

    asyncio.run(main=scenario())
