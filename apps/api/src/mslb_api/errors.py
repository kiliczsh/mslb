"""Safe shared error envelopes without raw validation inputs or SQL details."""

from mslb_contracts.http import ErrorEnvelope, ErrorInfo, FieldError
from starlette.responses import Response

STATUS = {
    "VALIDATION_ERROR": 422,
    "NOT_FOUND": 404,
    "CONFLICT": 409,
    "INVALID_STATE": 409,
    "UNAUTHORIZED": 401,
    "FORBIDDEN": 403,
    "RATE_LIMITED": 429,
    "PAYLOAD_TOO_LARGE": 413,
    "RESOURCE_PRESSURE": 503,
    "NOT_READY": 503,
    "INTERNAL_ERROR": 500,
    "VALIDATION_FAILED": 500,
    "STORAGE_LIMIT_EXCEEDED": 503,
    "RESOURCE_LIMIT_EXCEEDED": 413,
}


def error_response(
    code: str, message: str, request_id: str, details: tuple[FieldError, ...] = ()
) -> Response:
    headers = {"X-Request-ID": request_id, "Cache-Control": "no-store"}
    if code in {"RESOURCE_PRESSURE", "RATE_LIMITED", "NOT_READY", "STORAGE_LIMIT_EXCEEDED"}:
        headers["Retry-After"] = "1"
    if code == "UNAUTHORIZED":
        headers["WWW-Authenticate"] = "Bearer"
    envelope = ErrorEnvelope(
        error=ErrorInfo(code=code, message=message, details=details), request_id=request_id
    )
    return Response(
        envelope.model_dump_json(),
        status_code=STATUS.get(code, 500),
        media_type="application/json",
        headers=headers,
    )
