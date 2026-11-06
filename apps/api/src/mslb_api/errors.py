"""Safe shared error envelopes without raw validation inputs or SQL details."""

from http import HTTPStatus

from mslb_contracts.http import ErrorEnvelope, ErrorInfo, FieldError
from starlette.responses import Response

STATUS = {
    "VALIDATION_ERROR": HTTPStatus.UNPROCESSABLE_CONTENT.value,
    "NOT_FOUND": HTTPStatus.NOT_FOUND.value,
    "CONFLICT": HTTPStatus.CONFLICT.value,
    "INVALID_STATE": HTTPStatus.CONFLICT.value,
    "UNAUTHORIZED": HTTPStatus.UNAUTHORIZED.value,
    "FORBIDDEN": HTTPStatus.FORBIDDEN.value,
    "RATE_LIMITED": HTTPStatus.TOO_MANY_REQUESTS.value,
    "PAYLOAD_TOO_LARGE": HTTPStatus.CONTENT_TOO_LARGE.value,
    "RESOURCE_PRESSURE": HTTPStatus.SERVICE_UNAVAILABLE.value,
    "NOT_READY": HTTPStatus.SERVICE_UNAVAILABLE.value,
    "INTERNAL_ERROR": HTTPStatus.INTERNAL_SERVER_ERROR.value,
    "VALIDATION_FAILED": HTTPStatus.INTERNAL_SERVER_ERROR.value,
    "STORAGE_LIMIT_EXCEEDED": HTTPStatus.SERVICE_UNAVAILABLE.value,
    "RESOURCE_LIMIT_EXCEEDED": HTTPStatus.CONTENT_TOO_LARGE.value,
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
        status_code=STATUS.get(code, HTTPStatus.INTERNAL_SERVER_ERROR.value),
        media_type="application/json",
        headers=headers,
    )
