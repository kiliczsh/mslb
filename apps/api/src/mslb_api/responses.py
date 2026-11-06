"""Bounded shared REST JSON response encoding."""

from mslb_application.errors import OperationError
from pydantic import BaseModel
from starlette.responses import Response

from mslb_api.multipart import _Multipart as _Multipart


def json_response(
    model: BaseModel, status: int = 200, headers: dict[str, str] | None = None
) -> Response:
    payload = model.model_dump_json().encode()
    if len(payload) > 1024**2:
        raise OperationError("RESOURCE_LIMIT_EXCEEDED", "Response exceeds its encoded byte budget")
    return Response(payload, status_code=status, media_type="application/json", headers=headers)
