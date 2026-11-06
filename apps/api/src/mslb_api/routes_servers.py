"""REST servers routes over shared application services."""

import re
from http import HTTPStatus

from fastapi import APIRouter, Request
from mslb_application.errors import OperationError
from mslb_application.services import Services
from mslb_contracts.catalog import ServerView
from mslb_contracts.http import (
    ServerPatch,
)
from mslb_contracts.inputs import ServerInput
from mslb_contracts.pagination import Page
from starlette.responses import Response

from mslb_api.responses import json_response
from mslb_api.route_values import Cursor, Limit


def version(request: Request) -> int:
    match = re.fullmatch(r'"([1-9][0-9]{0,9})"', request.headers.get("if-match", ""))
    if match is None:
        raise OperationError("CONFLICT", 'Supply the current strong ETag in If-Match, such as "1"')
    return int(match[1])


def register_servers(*, router: APIRouter, services: Services) -> None:
    """Register the servers endpoints on the composed application router."""

    async def server(server_id: str) -> ServerView:
        return await services.get_server(server_id)

    @router.get("/servers", response_model=Page[ServerView])
    async def servers(limit: Limit = 100, cursor: Cursor = None) -> Response:
        """List versioned servers with bounded signed-cursor pagination."""
        return json_response(await services.list_servers(limit, cursor))

    @router.post("/servers", status_code=HTTPStatus.CREATED.value, response_model=ServerView)
    async def create_server(body: ServerInput) -> Response:
        """Create future logical capacity and return its initial strong ETag."""
        value = await services.create_server(body)
        return json_response(value, HTTPStatus.CREATED.value, {"ETag": f'"{value.version}"'})

    @router.get("/servers/{server_id:path}", response_model=ServerView)
    async def get_server(server_id: str) -> Response:
        """Read current logical capacities and their strong version ETag."""
        value = await server(server_id)
        return json_response(value, headers={"ETag": f'"{value.version}"'})

    @router.patch("/servers/{server_id:path}", response_model=ServerView)
    async def patch_server(server_id: str, body: ServerPatch, request: Request) -> Response:
        """Apply capacity changes only for the matching current strong ETag."""
        expected = version(request)
        value = await services.update_server(server_id, body, expected)
        return json_response(value, headers={"ETag": f'"{value.version}"'})

    @router.delete("/servers/{server_id:path}", status_code=HTTPStatus.NO_CONTENT.value)
    async def delete_server(server_id: str, request: Request) -> Response:
        """Delete future configuration only for the matching strong ETag."""
        expected = version(request)
        await services.delete_server(server_id, expected)
        return Response(status_code=HTTPStatus.NO_CONTENT.value)
