"""Explicit same-origin browser preflight response policy."""

from http import HTTPStatus

from starlette.responses import Response

from mslb_api.errors import error_response
from mslb_api.settings import Settings


def preflight_response(*, origin: str | None, settings: Settings, request_id: str) -> Response:
    """Advertise only the installation's configured origins and supported headers."""
    response = error_response(
        code="FORBIDDEN",
        message="Cross-origin preflight is not enabled",
        request_id=request_id,
    )
    if origin is not None and origin in settings.allowed_origins:
        response.status_code = HTTPStatus.NO_CONTENT.value
        response.body = b""
        response.headers["Content-Length"] = "0"
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Vary"] = "Origin"
        response.headers["Access-Control-Allow-Methods"] = "GET,POST,PUT,PATCH,DELETE,OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = (
            "Authorization,Content-Type,If-Match,Idempotency-Key,Last-Event-ID,"
            "Mcp-Session-Id,MCP-Protocol-Version,MCP-Method,MCP-Name"
        )
    return response
