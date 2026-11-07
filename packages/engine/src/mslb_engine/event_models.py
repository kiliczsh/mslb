"""Canonical events, scaling decisions and bounded interval observations."""

import json
from dataclasses import dataclass
from fractions import Fraction
from typing import Literal

from mslb_engine.input_models import Server

type EventKind = Literal[
    "REQUEST_FINISHED", "REQUEST_ARRIVED", "REQUEST_DROPPED", "REQUEST_STARTED"
]


EVENT_PRIORITY = {
    "REQUEST_FINISHED": 0,
    "REQUEST_ARRIVED": 1,
    "REQUEST_DROPPED": 2,
    "REQUEST_STARTED": 3,
}


@dataclass(frozen=True, slots=True)
class ScaleDecision:
    """Encode one deterministic scaling decision and its effective boundary."""

    t: int
    action: str
    trigger: tuple[str, ...]
    applied_at: int | None
    server_ids_added: tuple[str, ...]
    server_ids_removed: tuple[str, ...]
    reason: str | None

    def to_data(self) -> dict[str, object]:
        """Serialize the stable scaling decision fields used by canonical artifacts."""
        return {
            "t": self.t,
            "action": self.action,
            "trigger": list(self.trigger),
            "applied_at": self.applied_at,
            "server_ids_added": list(self.server_ids_added),
            "server_ids_removed": list(self.server_ids_removed),
            "reason": self.reason,
        }

    def canonical_line(self) -> str:
        """Encode one deterministic scaling decision as compact JSON followed by LF."""
        return json.dumps(obj=self.to_data(), ensure_ascii=False, separators=(",", ":")) + "\n"


@dataclass(frozen=True, slots=True)
class Event:
    """Encode a canonical logical request lifecycle event without operational timestamps."""

    t: int
    event: EventKind
    request_id: str
    server_id: str | None = None
    reason: str | None = None

    def canonical_line(self) -> str:
        """Serialize evaluator-facing keys only, including a final LF."""
        value: dict[str, int | str] = {
            "t": self.t,
            "event": self.event,
            "request_id": self.request_id,
        }
        if self.server_id is not None:
            value["server_id"] = self.server_id
        return json.dumps(obj=value, ensure_ascii=False, separators=(",", ":")) + "\n"


@dataclass(frozen=True, slots=True)
class ServerSample:
    """Record exact allocated/consumed CPU and memory reservations for one interval."""

    server_id: str
    capacity: int
    memory_capacity: int
    active_ids: tuple[str, ...]
    reserved_mem_mb: int
    allocated_cpu: Fraction
    consumed_cpu: Fraction
    starts: int


@dataclass(frozen=True, slots=True)
class TickFrame:
    """Expose one deterministic interval or terminal boundary to metrics and artifact writers."""

    t: int
    duration_ticks: int
    events: tuple[Event, ...]
    pending_ids: tuple[str, ...]
    samples: tuple[ServerSample, ...]
    terminal: bool = False
    membership: tuple[Server, ...] = ()
    scale_decision: ScaleDecision | None = None
