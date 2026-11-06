"""Shared strict MCP arguments, bounded resource envelopes, and prompt inputs."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator

from mslb_contracts.catalog import ServerView
from mslb_contracts.durability import PersistedEvent
from mslb_contracts.http import ServerPatch, SimulationCreate
from mslb_contracts.inputs import normalize_identifier
from mslb_contracts.pagination import Page
from mslb_contracts.read_base import ReadModel

type Limit = Annotated[int, Field(ge=1, le=500)]
type Cursor = Annotated[str, Field(min_length=1, max_length=2048)]
type RunId = Annotated[UUID, Field(strict=False)]


class NoArguments(ReadModel):
    """Require an empty argument object; extra keys are forbidden."""


class PageArguments(ReadModel):
    limit: Limit = 100
    cursor: Cursor | None = None


class ServerReference(ReadModel):
    server_id: Annotated[str, Field(min_length=1, max_length=256)]

    @field_validator("server_id")
    @classmethod
    def normalized(cls, value: str) -> str:
        return normalize_identifier(value=value)


class ServerUpdate(ServerReference):
    expected_version: Annotated[int, Field(ge=1, le=2**31 - 1)]
    changes: ServerPatch


class ServerDelete(ServerReference):
    expected_version: Annotated[int, Field(ge=1, le=2**31 - 1)]


class ServerDeleted(ReadModel):
    server_id: str
    deleted: Literal[True] = True


class RunReference(ReadModel):
    run_id: RunId


class RunCommand(ReadModel):
    simulation: SimulationCreate
    idempotency_key: Annotated[str, Field(min_length=1, max_length=256)] | None = None


class WaitArguments(ReadModel):
    run_ids: Annotated[tuple[RunId, ...], Field(min_length=1, max_length=8)]
    wait_seconds: Annotated[int, Field(ge=0, le=25)] = 20

    @field_validator("run_ids")
    @classmethod
    def unique(cls, value: tuple[UUID, ...]) -> tuple[UUID, ...]:
        if len(set(value)) != len(value):
            raise ValueError("Run IDs must be unique")
        return value


class EventArguments(RunReference, PageArguments):
    start_tick: Annotated[int, Field(ge=0, le=1_000_000)] = 0
    end_tick: Annotated[int, Field(ge=0, le=1_000_000)] = 1_000_000


class MetricArguments(RunReference):
    metric: (
        Literal["cpu_utilization", "memory_utilization", "pending_depth", "active_requests"] | None
    ) = None
    start_tick: Annotated[int, Field(ge=0, le=999_999)] = 0
    end_tick: Annotated[int, Field(ge=1, le=1_000_000)] = 1_000_000
    max_points: Annotated[int, Field(ge=1, le=2000)] = 2000
    server_id: Annotated[str, Field(min_length=1, max_length=128)] | None = None


class ServerResource(ReadModel):
    page: Page[ServerView]
    follow_on_tool: Literal["list_servers"] = "list_servers"
    guidance: str = "Use page.next_cursor with the follow-on tool for continuation."


class EventResource(ReadModel):
    page: Page[PersistedEvent]
    follow_on_tool: Literal["get_simulation_events"] = "get_simulation_events"
    guidance: str = "Use run_id and page.next_cursor with the follow-on tool for continuation."


class CompareStrategiesArguments(ReadModel):
    dataset_id: RunId
    mode: Literal["concurrent", "sequential"] = "sequential"
