"""Host, origin and credential-location validation before framework parsing."""

from mslb_application.errors import OperationError
from starlette.datastructures import Headers, QueryParams
from starlette.types import Scope

from mslb_api.settings import Settings

# Bounds cover metadata independently of the request-body budget.
MAX_HEADER_COUNT = 64
MAX_HEADER_BYTES = 16 * 1024
MAX_URL_BYTES = 4 * 1024


def validate_request(*, scope: Scope, headers: Headers, settings: Settings) -> None:
    """Reject ambiguous headers, unknown request origins and URL credentials."""
    path = scope["path"]
    if (
        len(headers) > MAX_HEADER_COUNT
        or sum(len(k) + len(v) for k, v in scope["headers"]) > MAX_HEADER_BYTES
        or len(path) + len(scope.get("query_string", b"")) > MAX_URL_BYTES
    ):
        raise OperationError(
            code="PAYLOAD_TOO_LARGE", message="Request headers or URL exceed limits"
        )
    for name in (
        "host",
        "origin",
        "authorization",
        "content-length",
        "content-type",
        "if-match",
        "last-event-id",
        "idempotency-key",
        "mcp-session-id",
        "mcp-protocol-version",
        "mcp-method",
        "mcp-name",
    ):
        if len(headers.getlist(key=name)) > 1:
            raise OperationError(
                code="VALIDATION_ERROR", message="Duplicate singleton request header"
            )
    if headers.get("host", "").lower() not in settings.allowed_hosts:
        raise OperationError(code="FORBIDDEN", message="Unexpected request Host")
    origin = headers.get("origin")
    if origin is not None and origin not in settings.allowed_origins:
        raise OperationError(code="FORBIDDEN", message="Unexpected request Origin")
    if any(
        key.lower() in {"token", "access_token", "authorization"}
        for key in QueryParams(scope.get("query_string", b""))
    ):
        raise OperationError(
            code="UNAUTHORIZED",
            message="Bearer credentials belong in the Authorization header",
        )
