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
    """Freeze the enrolled deployment metadata used for physical run snapshots."""

    id: UUID
    definition: DeploymentSpec


@dataclass(frozen=True, slots=True)
class ControlLease:
    """Carry the controller owner, phase, expiry and fencing token for guarded mutations."""

    run_id: UUID
    fence: int
    owner: str
    expires_at: datetime
    phase: Literal["running", "cleanup"]


@dataclass(frozen=True, slots=True)
class ResourceRecord:
    """Return an immutable persisted resource definition and observed lifecycle state."""

    id: UUID
    ordinal: int
    slot: int
    kind: str
    state: str
    name: str
    container_id: str | None
    definition: CreationIntent | StaticReservation


def resource_record(row: DockerResource) -> ResourceRecord:
    """Decode trusted stored resource definitions into immutable application values."""
    raw = canonical_bytes(value=row.definition)
    definition = (
        CreationIntent.model_validate_json(json_data=raw)
        if row.kind == "dynamic"
        else StaticReservation.model_validate_json(json_data=raw)
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
        raise StoreError(
            code="VALIDATION_FAILED", message="Stored resource provenance is inconsistent"
        )
    return ResourceRecord(
        id=row.id,
        ordinal=row.ordinal,
        slot=row.slot,
        kind=row.kind,
        state=row.state,
        name=row.name,
        container_id=row.container_id,
        definition=definition,
    )
