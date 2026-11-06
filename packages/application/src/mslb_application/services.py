"""Shared operations used by REST and MCP over one admitted application context."""

from uuid import UUID

from mslb_contracts.catalog import ServerView
from mslb_contracts.http import (
    RunDetail,
    ServerPatch,
    SimulationAccepted,
    SimulationCreate,
    StrategyView,
)
from mslb_contracts.inputs import ServerInput, normalize_identifier
from mslb_contracts.queries import Page, RunView
from mslb_contracts.runs import RunSettings
from mslb_persistence.catalog import Catalog
from mslb_persistence.jobs import Jobs
from mslb_persistence.reads import check_limit

from mslb_application.cursors import Position
from mslb_application.errors import OperationError
from mslb_application.queries import Queries
from mslb_application.submissions import Submissions


class Services:
    """Coordinate transport-neutral catalog, submission, cancellation and result use cases."""

    def __init__(
        self, catalog: Catalog, jobs: Jobs, submissions: Submissions, queries: Queries
    ) -> None:
        self.catalog, self.jobs, self.submissions, self.queries = (
            catalog,
            jobs,
            submissions,
            queries,
        )

    async def list_servers(self, limit: int = 100, cursor: str | None = None) -> Page[ServerView]:
        """Return a byte-bounded versioned server page through the shared query policy."""
        check_limit(limit)
        point = self.queries.cursors.decode("servers", cursor)
        if point.id is not None or point.created_at is not None or point.sequence:
            raise OperationError("VALIDATION_ERROR", "Cursor does not contain a server position")
        values = tuple(
            row
            for row in await self.catalog.list_servers()
            if point.key is None or row.id > point.key
        )
        return self.queries._page(
            values[:limit], limit, "servers", lambda row: Position(key=row.id)
        )

    async def get_server(self, server_id: str) -> ServerView:
        try:
            normalized = normalize_identifier(value=server_id)
        except ValueError:
            raise OperationError("VALIDATION_ERROR", "Invalid server identifier") from None
        for value in await self.catalog.list_servers():
            if value.id == normalized:
                return value
        raise OperationError("NOT_FOUND", "Server does not exist")

    async def create_server(self, body: ServerInput) -> ServerView:
        return await self.catalog.create_server(body)

    async def update_server(
        self, server_id: str, body: ServerPatch, expected_version: int
    ) -> ServerView:
        previous = await self.get_server(server_id)
        values = previous.model_dump(exclude={"version"}) | body.model_dump(exclude_unset=True)
        return await self.catalog.update_server(
            ServerInput.model_validate(values), expected_version
        )

    async def delete_server(self, server_id: str, expected_version: int) -> str:
        previous = await self.get_server(server_id)
        await self.catalog.delete_server(previous.id, expected_version)
        return previous.id

    @staticmethod
    def strategies() -> Page[StrategyView]:
        """Return the same four supported strategy identities for every transport."""
        return Page[StrategyView](
            items=tuple(
                StrategyView(id=name, display_name=title, description=description)
                for name, title, description in (
                    ("first_fit", "First Fit", "Choose the first eligible server in ID order."),
                    ("round_robin", "Round Robin", "Rotate through eligible servers in ID order."),
                    (
                        "least_connections",
                        "Least Connections",
                        "Choose the eligible server with fewest active requests.",
                    ),
                    (
                        "most_available_memory",
                        "Most Available Memory",
                        "Choose the eligible server with most free memory.",
                    ),
                )
            )
        )

    async def submit(self, body: SimulationCreate, key: str | None = None) -> SimulationAccepted:
        settings = RunSettings.model_validate(body.model_dump(exclude={"dataset_id"}))
        run = await self.submissions.submit(body.dataset_id, settings, key)
        status = (await self.queries.run(run.id)).status
        return SimulationAccepted(run_id=run.id, status=status, snapshot_digest=run.snapshot_digest)

    async def detail(self, run_id: UUID) -> RunDetail:
        run = await self.queries.run(run_id)
        validation = (
            (await self.queries.completed(run_id))[1] if run.status == "completed" else None
        )
        return RunDetail(
            **run.model_dump(),
            mode=run.configuration.mode,
            strategy=run.configuration.strategy,
            validation=validation,
        )

    async def cancel(self, run_id: UUID) -> RunView:
        await self.jobs.cancel(run_id)
        return await self.queries.run(run_id)
