"""Bounded immutable output references and fenced progress commit contracts."""

import json
from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from mslb_contracts.checkpoints import ArtifactPrefix, CheckpointModel, PrefixName, Tick
from mslb_contracts.runs import Digest


class StoredFile(CheckpointModel):
    key: Annotated[str, Field(min_length=1, max_length=256, pattern=r"^[a-z0-9][a-z0-9._/-]*$")]
    sha256: Digest
    size_bytes: Annotated[int, Field(ge=0, le=256 * 1024**2)]


class OutputChunk(CheckpointModel):
    name: PrefixName
    file: StoredFile
    first_sequence: Annotated[int, Field(ge=1, le=1_000_001)]
    last_sequence: Annotated[int, Field(ge=1, le=1_000_001)]
    byte_offset: Annotated[int, Field(ge=0, le=256 * 1024**2)]
    first_tick: Tick
    last_tick: Tick


class PersistedEvent(CheckpointModel):
    sequence: Annotated[int, Field(ge=1, le=30_000)]
    t: Tick
    event: Literal["REQUEST_ARRIVED", "REQUEST_STARTED", "REQUEST_FINISHED", "REQUEST_DROPPED"]
    request_id: Annotated[str, Field(min_length=1, max_length=128)]
    server_id: Annotated[str, Field(min_length=1, max_length=128)] | None = None
    reason: Literal["UNSCHEDULABLE_MEMORY", "QUEUE_TIMEOUT", "QUEUE_CAPACITY"] | None = None

    @model_validator(mode="after")
    def lifecycle_fields(self) -> Self:
        if (
            (self.server_id is not None) != (self.event in {"REQUEST_STARTED", "REQUEST_FINISHED"})
            or (self.reason is not None) != (self.event == "REQUEST_DROPPED")
            or len(self.request_id.encode()) > 128
            or self.server_id is not None
            and len(self.server_id.encode()) > 128
        ):
            raise ValueError("invalid_event_fields")
        return self

    def canonical_trace_line(self) -> bytes:
        value: dict[str, str | int] = {
            "t": self.t,
            "event": self.event,
            "request_id": self.request_id,
        }
        if self.server_id is not None:
            value["server_id"] = self.server_id
        return (json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n").encode()


class ProgressCommit(CheckpointModel):
    kind: Literal["checkpoint", "terminal"]
    reference: StoredFile
    snapshot_digest: Digest
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    next_tick: Tick
    executed_intervals: Annotated[int, Field(ge=0, le=1_000_000)]
    prefixes: Annotated[tuple[ArtifactPrefix, ...], Field(min_length=4, max_length=4)]
    metrics: StoredFile | None = None
    chunks: Annotated[tuple[OutputChunk, ...], Field(max_length=64)] = ()
    events: Annotated[tuple[PersistedEvent, ...], Field(max_length=30_000)] = ()


class RecoveryState(CheckpointModel):
    kind: Literal["checkpoint", "terminal"]
    reference: StoredFile
    snapshot_digest: Digest
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    next_tick: Tick
    executed_intervals: Annotated[int, Field(ge=0, le=1_000_000)]
    prefixes: Annotated[tuple[ArtifactPrefix, ...], Field(min_length=4, max_length=4)]
    metrics: StoredFile | None
    revision: Annotated[int, Field(ge=1)]


class TerminalSeal(CheckpointModel):
    schema_version: Annotated[int, Field(ge=1, le=1)] = 1
    snapshot_digest: Digest
    engine_version: Annotated[str, Field(min_length=1, max_length=64)]
    terminal_tick: Tick
    executed_intervals: Annotated[int, Field(ge=0, le=1_000_000)]
    prefixes: Annotated[tuple[ArtifactPrefix, ...], Field(min_length=4, max_length=4)]
    metrics: StoredFile
