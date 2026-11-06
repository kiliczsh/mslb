"""Boundary diagnostics retain useful categories without private exception payloads."""

import asyncio
import logging
from unittest.mock import Mock
from uuid import UUID

import pytest
from mcp.server import ServerRequestContext
from mcp.types import CallToolRequestParams
from mslb_api.mcp_adapter import McpAdapter
from mslb_api.settings import Settings
from mslb_application.diagnostics import report_failure
from mslb_application.errors import OperationError
from mslb_application.failures import failure
from mslb_application.services import Services
from mslb_contracts.inputs import InputError
from sqlalchemy.exc import DBAPIError

IDENTIFIER = str(UUID(int=1))


@pytest.mark.parametrize(
    argnames=("error", "code", "level"),
    argvalues=[
        (OSError("private-file-body"), "RESOURCE_PRESSURE", logging.WARNING),
        (TimeoutError("private-dependency-url"), "RESOURCE_PRESSURE", logging.WARNING),
        (
            DBAPIError(
                statement="private-query",
                params={"token": "private-token"},
                orig=OSError("private-db-url"),
                connection_invalidated=False,
            ),
            "RESOURCE_PRESSURE",
            logging.WARNING,
        ),
        (RuntimeError("private-unexpected-body"), "INTERNAL_ERROR", logging.ERROR),
    ],
)
def test_translated_failures_log_only_bounded_type_category_and_uuid(
    *, error: Exception, code: str, level: int, caplog: pytest.LogCaptureFixture
) -> None:
    result = failure(error=error, request_id=IDENTIFIER)
    assert result.error.code == code and result.request_id == IDENTIFIER
    assert "private-" not in result.model_dump_json() + caplog.text
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.levelno == level and record.exc_info is None and record.stack_info is None
    assert IDENTIFIER in record.getMessage() and type(error).__name__ in record.getMessage()
    assert len(record.getMessage()) < 256


def test_expected_input_and_domain_refusals_do_not_emit_failure_logs(
    *, caplog: pytest.LogCaptureFixture
) -> None:
    for error in (
        OperationError(code="CONFLICT", message="The current version differs"),
        InputError(field="requests", reason="invalid_csv"),
    ):
        result = failure(error=error, request_id=IDENTIFIER)
        assert result.error.code in {"CONFLICT", "VALIDATION_ERROR"}
    assert not caplog.records


@pytest.mark.parametrize(
    argnames="request_id", argvalues=["private-token\nforged-line", "x" * 100_000]
)
def test_untrusted_correlation_cannot_inject_or_expand_diagnostics(
    *, request_id: str, caplog: pytest.LogCaptureFixture
) -> None:
    report_failure(
        error=RuntimeError("private-error"),
        code="INTERNAL_ERROR",
        source="application",
        request_id=request_id,
    )
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "request_id=unavailable" in message and "private-" not in message
    assert "\n" not in message and len(message) < 256


def test_mcp_cancellation_escapes_error_translation_without_diagnostics(
    *, caplog: pytest.LogCaptureFixture
) -> None:
    async def scenario() -> None:
        services = Mock(spec=Services)
        services.queries = Mock()
        services.strategies.side_effect = asyncio.CancelledError()
        adapter = McpAdapter(services=services, settings=Settings())
        context = Mock(spec=ServerRequestContext)
        context.request = None
        with pytest.raises(expected_exception=asyncio.CancelledError):
            await adapter.call_tool(
                context=context, params=CallToolRequestParams(name="list_strategies")
            )
        assert adapter.calls._value == 8

    asyncio.run(main=scenario())
    assert not caplog.records
