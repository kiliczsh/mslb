"""Bounded checkpoint state capture and verified execution restoration."""

import json
from collections.abc import Mapping
from itertools import chain
from typing import BinaryIO

from mslb_contracts.checkpoints import (
    ActiveWork,
    EndedRequest,
    IntervalCheckpoint,
    MetricExecution,
    PendingChange,
    ScalingExecution,
    ScalingObservation,
    ServerExecution,
    ServerIntegrals,
    StartedRequest,
)
from mslb_contracts.fractions import FractionValue
from mslb_contracts.inputs import ServerInput
from mslb_contracts.runs import RunSnapshot
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.models import Server
from mslb_engine.simulator import Simulation

from mslb_application.checkpoint_prefixes import _require, _scan_prefix
from mslb_application.checkpoint_restore import _fraction
from mslb_application.checkpoint_restore import _restore as _restore
from mslb_application.checkpoint_values import (
    MAX_CHECKPOINT_BYTES,
    PREFIX_NAMES,
)
from mslb_application.errors import OperationError


def _server_input(server: Server) -> ServerInput:
    return ServerInput(
        id=server.id,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


def _quoted_size(*, value: str) -> int:
    """Count actual escaped UTF-8 identifier bytes without constructing checkpoint DTOs."""
    return len(json.dumps(obj=value, ensure_ascii=False).encode(encoding="utf-8"))


def _preflight(simulation: Simulation, metrics: MetricAccumulator, maximum: int) -> None:
    """Bound duplicate DTO/JSON allocation before constructing it.

    Fixed allowances cover field names, separators, bounded integers and escaped
    identifiers. Fraction decimal digits are estimated from bit lengths, without
    first allocating decimal representations of every exact value.
    """
    _require(
        condition=type(maximum) is int and 1 <= maximum <= MAX_CHECKPOINT_BYTES,
        message="Invalid byte budget",
    )
    _require(
        condition=simulation.checkpoint_ready
        and not simulation.terminal
        and not metrics.terminal
        and simulation.tick == metrics.next_tick
        and simulation.tick > 0,
        message="Checkpoint requires a completed, observed, nonterminal interval",
    )
    pending = simulation.autoscaler.pending
    estimated = (
        8192
        + sum(16 + _quoted_size(value=identifier) for identifier in metrics.arrivals)
        + sum(
            128 + _quoted_size(value=identifier) + _quoted_size(value=server_id)
            for identifier, (_, server_id) in metrics.starts.items()
        )
        + sum(
            64 + _quoted_size(value=identifier)
            for identifier in chain(metrics.finishes, metrics.drops)
        )
        + sum(
            128 + _quoted_size(value=identifier)
            for active in simulation.active.values()
            for identifier in active
        )
        + sum(16 + _quoted_size(value=request.request_id) for request in simulation.pending)
        + len(metrics.servers) * 2048
        + len(simulation.servers) * 4096
        + len(simulation.autoscaler.history) * 512
        + (
            512
            + sum(
                16 + _quoted_size(value=identifier)
                for identifier in chain(pending.server_ids_added, pending.server_ids_removed)
            )
            if pending is not None
            else 0
        )
    )
    fractions = (
        [value for active in simulation.active.values() for value in active.values()]
        + [
            value
            for total in metrics.servers.values()
            for value in (total.cpu_allocated, total.cpu_consumed)
        ]
        + [
            value
            for item in simulation.autoscaler.history
            for value in (item.cpu_utilization, item.drop_ratio)
        ]
    )
    for value in fractions:
        _fraction(value=value)
        estimated += (
            value.numerator.bit_length() + value.denominator.bit_length()
        ) * 30103 // 100000 + 96
    if estimated > maximum:
        raise OperationError(
            code="RESOURCE_LIMIT_EXCEEDED", message="Checkpoint allocation exceeds its budget"
        )


def _capture(
    snapshot: RunSnapshot,
    simulation: Simulation,
    metrics: MetricAccumulator,
    streams: Mapping[str, BinaryIO],
) -> IntervalCheckpoint:
    """Capture bounded completed-interval state with exact fractions and committed prefixes."""
    _require(
        condition=set(streams) == set(PREFIX_NAMES),
        message="All four checkpoint prefixes are required",
    )
    pending = simulation.autoscaler.pending
    change = (
        PendingChange.model_validate_json(json_data=pending.canonical_line())
        if pending is not None
        else None
    )
    return IntervalCheckpoint(
        engine_version=snapshot.engine_version,
        snapshot_digest=snapshot.snapshot_digest,
        next_tick=simulation.tick,
        input_cursor=simulation.cursor,
        executed_intervals=simulation.executed_intervals,
        event_count=simulation.event_count,
        decision_count=simulation.decision_count,
        recorded_samples=simulation.recorded_samples,
        round_robin_anchor=simulation.anchor,
        servers=tuple(
            ServerExecution(
                server=_server_input(server=server),
                reserved_mem_mb=simulation.reserved[server.id],
                start_counter=simulation.last_start_counters[server.id],
                active=tuple(
                    ActiveWork(request_id=rid, remaining=FractionValue.from_fraction(value=work))
                    for rid, work in sorted(simulation.active[server.id].items())
                ),
            )
            for server in simulation.servers
        ),
        pending_ids=tuple(request.request_id for request in simulation.pending),
        autoscaler=ScalingExecution(
            history=tuple(
                ScalingObservation(
                    ending_tick=item.ending_tick,
                    cpu_utilization=FractionValue.from_fraction(value=item.cpu_utilization),
                    drops=item.drops,
                    drop_ratio=FractionValue.from_fraction(value=item.drop_ratio),
                )
                for item in simulation.autoscaler.history
            ),
            next_eligible_tick=simulation.autoscaler.next_eligible_tick,
            counter=simulation.autoscaler.counter,
            pending=change,
        ),
        metrics=MetricExecution(
            arrivals=tuple(sorted(metrics.arrivals)),
            starts=tuple(
                StartedRequest(request_id=rid, t=t, server_id=sid)
                for rid, (t, sid) in sorted(metrics.starts.items())
            ),
            finishes=tuple(
                EndedRequest(request_id=rid, t=t) for rid, t in sorted(metrics.finishes.items())
            ),
            drops=tuple(
                EndedRequest(request_id=rid, t=t) for rid, t in sorted(metrics.drops.items())
            ),
            servers=tuple(
                ServerIntegrals(
                    server_id=sid,
                    cpu_units_per_tick=total.cpu_units_per_tick,
                    mem_mb=total.mem_mb,
                    cpu_allocated=FractionValue.from_fraction(value=total.cpu_allocated),
                    cpu_consumed=FractionValue.from_fraction(value=total.cpu_consumed),
                    cpu_capacity_ticks=total.cpu_capacity_ticks,
                    memory_reserved_mb_ticks=total.memory_reserved_mb_ticks,
                    memory_capacity_mb_ticks=total.memory_capacity_mb_ticks,
                )
                for sid, total in sorted(metrics.servers.items())
            ),
            queue_peak=metrics.queue_peak,
            queue_integral=metrics.queue_integral,
        ),
        prefixes=tuple(_scan_prefix(name=name, stream=streams[name]) for name in PREFIX_NAMES),
    )
