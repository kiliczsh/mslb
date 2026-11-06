"""REST results routes over shared application services."""

from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Query, Request
from mslb_application.errors import OperationError
from mslb_application.event_feed import EventFeed
from mslb_application.queries import ArtifactName, ChartMetric, Queries
from mslb_contracts.durability import PersistedEvent
from mslb_contracts.metric_views import ChartView, MetricsView, ServerMetricView
from mslb_contracts.pagination import Page
from mslb_contracts.playback import PlaybackView
from mslb_persistence.database import Database
from starlette.responses import Response

from mslb_api.multipart import _Multipart as _Multipart
from mslb_api.responses import json_response
from mslb_api.route_values import Cursor, Limit, Tick
from mslb_api.streams import ArtifactResponse, EventResponse


def register_results(*, router: APIRouter, queries: Queries, database: Database) -> None:
    """Register the results endpoints on the composed application router."""

    @router.get("/simulations/{run_id}/events", response_model=Page[PersistedEvent])
    async def events(
        run_id: UUID,
        limit: Limit = 100,
        cursor: Cursor = None,
        start_tick: Tick = 0,
        end_tick: Tick = 1_000_000,
    ) -> Response:
        return json_response(await queries.events(run_id, limit, cursor, start_tick, end_tick))

    @router.get("/simulations/{run_id}/metrics", response_model=MetricsView | ChartView)
    async def metrics(
        run_id: UUID,
        metric: ChartMetric | None = None,
        start_tick: Tick = 0,
        end_tick: Tick = 1_000_000,
        max_points: Annotated[int, Query(ge=1, le=2000)] = 2000,
        server_id: Annotated[str | None, Query(max_length=128)] = None,
    ) -> Response:
        if metric is not None:
            return json_response(
                await queries.chart(run_id, metric, start_tick, end_tick, max_points, server_id)
            )
        return json_response(
            await queries.metrics(run_id, (server_id,) if server_id is not None else ())
        )

    @router.get("/simulations/{run_id}/metrics/servers", response_model=Page[ServerMetricView])
    async def server_metrics(run_id: UUID, limit: Limit = 100, cursor: Cursor = None) -> Response:
        return json_response(await queries.server_metrics(run_id, limit, cursor))

    @router.get("/simulations/{run_id}/playback", response_model=PlaybackView)
    async def playback(
        run_id: UUID,
        tick: Tick = 0,
        server_offset: Annotated[int, Query(ge=0, le=64)] = 0,
        request_kind: Literal["pending", "active"] = "pending",
        server_id: Annotated[str | None, Query(max_length=128)] = None,
        request_offset: Annotated[int, Query(ge=0, le=10_000)] = 0,
    ) -> Response:
        return json_response(
            await queries.playback(
                run_id,
                tick,
                server_offset,
                request_kind,
                server_id,
                request_offset,
            )
        )

    @router.get(
        "/simulations/{run_id}/artifact",
        response_class=Response,
        responses={
            200: {
                "content": {
                    "application/x-ndjson": {"schema": {"type": "string"}},
                    "text/csv": {"schema": {"type": "string"}},
                    "application/json": {"schema": {"type": "object"}},
                }
            }
        },
    )
    async def artifact(run_id: UUID, name: ArtifactName = "run.jsonl") -> Response:
        return ArtifactResponse(downloads=(await queries.download(run_id=run_id, name=name),))

    @router.get(
        "/simulations/{run_id}/inputs",
        response_class=Response,
        responses={
            200: {
                "content": {"application/zip": {"schema": {"type": "string", "format": "binary"}}}
            }
        },
    )
    async def run_inputs(run_id: UUID) -> Response:
        downloads = tuple(
            [
                await queries.download(run_id=run_id, name=name)
                for name in ("servers.json", "requests.csv", "snapshot.json", "manifest.json")
            ]
        )
        return ArtifactResponse(downloads=downloads, archive=True)

    @router.get(
        "/simulations/{run_id}/stream",
        response_class=Response,
        responses={200: {"content": {"text/event-stream": {"schema": {"type": "string"}}}}},
    )
    async def stream(run_id: UUID, request: Request) -> Response:
        last = request.headers.get("last-event-id", "0")
        if not last.isascii() or not last.isdecimal() or len(last) > 5:
            raise OperationError("VALIDATION_ERROR", "Invalid Last-Event-ID")
        feed = EventFeed(database, run_id, int(last))
        initial = await queries.run(run_id)
        return EventResponse(
            source=feed.stream(initial=initial),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
