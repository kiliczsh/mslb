"""Immutable Docker repository records and strict stored-provenance decoding."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.docker_runs import CreationIntent, DeploymentSpec, StaticReservation

from mslb_persistence.docker_models import DockerResource
from mslb_persistence.errors import StoreError


@dataclass(frozen=True, slots=True)
class DeploymentRecord:
    id: UUID
    definition: DeploymentSpec


@dataclass(frozen=True, slots=True)
class ControlLease:
    run_id: UUID
    fence: int
    owner: str
    expires_at: datetime
    phase: Literal["running", "cleanup"]


@dataclass(frozen=True, slots=True)
class ResourceRecord:
    id: UUID
    ordinal: int
    slot: int
    kind: str
    state: str
    name: str
    container_id: str | None
    definition: CreationIntent | StaticReservation


def resource_record(row: DockerResource) -> ResourceRecord:
    raw = canonical_bytes(row.definition)
    definition = (
        CreationIntent.model_validate_json(raw)
        if row.kind == "dynamic"
        else StaticReservation.model_validate_json(raw)
    )
    identifier = (
        definition.intent_id if isinstance(definition, CreationIntent) else definition.resource_id
    )
    name = definition.name if isinstance(definition, CreationIntent) else definition.backend.name
    if (
        identifier != row.id
        or definition.run_id != row.run_id
        or definition.slot != row.slot
        or name != row.name
        or definition.digest() != row.definition_digest
    ):
        raise StoreError("VALIDATION_FAILED", "Stored resource provenance is inconsistent")
    return ResourceRecord(
        row.id, row.ordinal, row.slot, row.kind, row.state, row.name, row.container_id, definition
    )
