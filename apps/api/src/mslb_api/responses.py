"""Bounded shared REST JSON response encoding."""

from http import HTTPStatus

from mslb_application.errors import OperationError
from pydantic import BaseModel
from starlette.responses import Response


def json_response(
    model: BaseModel, status: int = HTTPStatus.OK.value, headers: dict[str, str] | None = None
) -> Response:
    """Serialize typed response data using the public JSON representation and headers."""
    payload = model.model_dump_json().encode()
    if len(payload) > 1024**2:
        raise OperationError(
            code="RESOURCE_LIMIT_EXCEEDED", message="Response exceeds its encoded byte budget"
        )
    return Response(
        content=payload, status_code=status, media_type="application/json", headers=headers
    )
