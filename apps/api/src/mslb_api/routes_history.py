"""Browser history with SQL filtering and descending signed pagination."""

from typing import Annotated

from fastapi import APIRouter, Query
from mslb_application.queries import Queries
from mslb_contracts.history import (
    HistoryEntry,
    HistoryFilters,
    HistoryKind,
    HistoryMode,
    HistoryStatus,
    HistoryStrategy,
)
from mslb_contracts.pagination import Page
from starlette.responses import Response

from mslb_api.responses import json_response
from mslb_api.route_values import Cursor, Limit


def register_history(*, router: APIRouter, queries: Queries) -> None:
    """Expose bounded metadata without changing legacy REST/MCP list ordering."""

    @router.get(path="/history/{kind}", response_model=Page[HistoryEntry])
    async def history(
        *,
        kind: HistoryKind,
        limit: Limit = 20,
        cursor: Cursor = None,
        search: Annotated[str, Query(max_length=128)] = "",
        status: HistoryStatus | None = None,
        strategy: HistoryStrategy | None = None,
        mode: HistoryMode | None = None,
    ) -> Response:
        return json_response(
            model=await queries.history(
                kind=kind,
                limit=limit,
                cursor=cursor,
                filters=HistoryFilters(
                    search=search.strip(), status=status, strategy=strategy, mode=mode
                ),
            )
        )
