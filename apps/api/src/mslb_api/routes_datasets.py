"""REST datasets routes over shared application services."""

from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter, Request
from mslb_application.errors import OperationError
from mslb_application.queries import Queries
from mslb_application.submissions import Submissions
from mslb_contracts.dataset_views import DatasetView
from mslb_contracts.pagination import Page
from starlette.datastructures import UploadFile
from starlette.responses import Response

from mslb_api.multipart import _Multipart
from mslb_api.responses import json_response
from mslb_api.route_values import Cursor, Limit


def register_datasets(*, router: APIRouter, submissions: Submissions, queries: Queries) -> None:
    """Register the datasets endpoints on the composed application router."""

    @router.post(
        "/datasets",
        status_code=HTTPStatus.CREATED.value,
        response_model=DatasetView,
        openapi_extra={
            "requestBody": {
                "required": True,
                "content": {
                    "multipart/form-data": {
                        "schema": {
                            "type": "object",
                            "required": ["file"],
                            "properties": {"file": {"type": "string", "format": "binary"}},
                            "additionalProperties": False,
                        }
                    }
                },
            }
        },
    )
    async def upload(request: Request) -> Response:
        if not request.headers.get("content-type", "").startswith("multipart/form-data;"):
            raise OperationError("VALIDATION_ERROR", "Upload one multipart CSV file named file")
        parser = _Multipart(
            request.headers, request.stream(), max_files=1, max_fields=0, max_part_size=10 * 1024**2
        )
        form = await parser.parse()
        try:
            if len(form.multi_items()) != 1 or not isinstance(form.get("file"), UploadFile):
                raise OperationError("VALIDATION_ERROR", "Upload exactly one CSV file named file")
            source = form["file"]
            if (
                not isinstance(source, UploadFile)
                or source.size is None
                or source.size > 10 * 1024**2
            ):
                raise OperationError("PAYLOAD_TOO_LARGE", "CSV upload exceeds 10 MiB")
            data = await submissions.upload(source.file, source.filename or "requests.csv")
            return json_response(await queries.dataset(data.id), HTTPStatus.CREATED.value)
        finally:
            await form.close()

    @router.get("/datasets", response_model=Page[DatasetView])
    async def datasets(limit: Limit = 100, cursor: Cursor = None) -> Response:
        return json_response(await queries.datasets(limit, cursor))

    @router.get("/datasets/{dataset_id}", response_model=DatasetView)
    async def dataset(dataset_id: UUID) -> Response:
        return json_response(await queries.dataset(dataset_id))
