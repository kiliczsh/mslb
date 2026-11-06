"""Bounded views of one verified persisted logical interval, never engine execution."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field

from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import ServerInput
from mslb_contracts.read_base import ReadModel
from mslb_contracts.runs import Digest

type Tick = Annotated[int, Field(ge=0, le=1_000_000)]
type Identifier = Annotated[str, Field(min_length=1, max_length=128)]
type ExactValue = Annotated[int, Field(ge=0, le=2**53 - 1)] | FractionValue


class PlaybackServer(ServerInput):
    """Expose one server's saved membership, reservations and interval activity."""

    active_count: Annotated[int, Field(ge=0, le=10_000)]
    reserved_mem_mb: Annotated[int, Field(ge=0, le=10**9)] | None
    allocated_cpu: ExactValue | None
    consumed_cpu: ExactValue | None
    starts: Annotated[int, Field(ge=0, le=10_000)] | None


class PlaybackDecision(ReadModel):
    """Expose the saved simulated scaling decision at a playback boundary."""

    t: Tick
    action: Literal["none", "scale_out", "scale_in"]
    trigger: Annotated[tuple[Annotated[str, Field(max_length=64)], ...], Field(max_length=8)]
    applied_at: Tick | None
    server_ids_added: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    server_ids_removed: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    reason: Annotated[str, Field(max_length=64)] | None


class PlaybackView(ReadModel):
    """Expose bounded persisted playback slices without re-executing the simulation."""

    run_id: UUID
    bundle_digest: Digest
    tick: Tick
    frame_tick: Tick
    duration_ticks: Tick
    terminal: bool
    membership_count: Annotated[int, Field(ge=1, le=64)]
    server_offset: Annotated[int, Field(ge=0, le=64)]
    servers: Annotated[tuple[PlaybackServer, ...], Field(max_length=32)]
    membership_added: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    membership_removed: Annotated[tuple[Identifier, ...], Field(max_length=64)]
    queue_depth: Annotated[int, Field(ge=0, le=10_000)]
    active_count: Annotated[int, Field(ge=0, le=10_000)]
    request_kind: Literal["pending", "active"]
    server_id: Identifier | None
    server_present: bool
    request_offset: Annotated[int, Field(ge=0, le=10_000)]
    request_count: Annotated[int, Field(ge=0, le=10_000)]
    request_ids: Annotated[tuple[Identifier, ...], Field(max_length=100)]
    decisions: Annotated[tuple[PlaybackDecision, ...], Field(max_length=2)]
