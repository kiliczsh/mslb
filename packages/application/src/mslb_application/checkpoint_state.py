"""Bounded checkpoint state capture and verified execution restoration."""

import json
import re
from collections import Counter
from collections.abc import Mapping
from fractions import Fraction
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
from mslb_engine.autoscaling import Observation
from mslb_engine.metrics import MetricAccumulator, ServerTotals
from mslb_engine.models import ScaleDecision, Server
from mslb_engine.simulator import Simulation

from mslb_application.checkpoint_prefixes import _require, _scan_prefix
from mslb_application.checkpoint_values import (
    MAX_CHECKPOINT_BYTES,
    PREFIX_NAMES,
    RestoredCheckpoint,
)
from mslb_application.errors import OperationError
from mslb_application.snapshots import domain_config, domain_inputs


def _fraction(value: Fraction, bits: int = 4096) -> Fraction:
    _require(
        condition=max(value.numerator.bit_length(), value.denominator.bit_length()) <= bits,
        message="Checkpoint arithmetic complexity exceeds the frozen limit",
    )
    return value


def _server_input(server: Server) -> ServerInput:
    return ServerInput(
        id=server.id,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


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
    estimated = (
        8192
        + sum(
            512
            + 8
            * len(json.dumps(obj=request.request_id, ensure_ascii=False).encode(encoding="utf-8"))
            + 6
            * len(
                json.dumps(
                    obj=metrics.starts.get(request.request_id, (0, ""))[1], ensure_ascii=False
                ).encode(encoding="utf-8")
            )
            for request in simulation.requests
        )
        + len(metrics.servers) * 2048
        + len(simulation.servers) * 4096
        + len(simulation.autoscaler.history) * 512
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


def _restore(snapshot: RunSnapshot, state: IntervalCheckpoint) -> RestoredCheckpoint:
    """Rebuild known classes explicitly; no pickle, imports, or executable payloads."""
    _require(
        condition=state.snapshot_digest == snapshot.snapshot_digest
        and state.engine_version == snapshot.engine_version,
        message="Checkpoint belongs to incompatible inputs, configuration, or engine",
    )
    servers, requests = domain_inputs(snapshot=snapshot)
    simulation = Simulation(
        servers=servers, requests=requests, config=domain_config(settings=snapshot.configuration)
    )
    _require(
        condition=state.next_tick <= simulation.config.max_simulation_ticks,
        message="Invalid checkpoint horizon",
    )
    _require(
        condition=state.recorded_samples <= simulation.config.max_samples,
        message="Sample budget exceeded",
    )
    _require(
        condition=state.executed_intervals <= state.next_tick,
        message="Invalid executed interval count",
    )
    _require(
        condition=len(state.metrics.servers) <= simulation.config.max_metric_series,
        message="History budget exceeded",
    )
    _require(condition=state.input_cursor <= len(requests), message="Invalid input cursor")
    expected_arrivals = tuple(sorted(r.request_id for r in requests[: state.input_cursor]))
    _require(
        condition=state.metrics.arrivals == expected_arrivals,
        message="Arrival cursor/lifecycle mismatch",
    )
    _require(
        condition=all(r.t < state.next_tick for r in requests[: state.input_cursor])
        and all(r.t >= state.next_tick for r in requests[state.input_cursor :]),
        message="Input cursor crosses the next boundary",
    )
    simulation.servers = tuple(
        Server(
            id=s.server.id,
            cpu_units_per_tick=s.server.cpu_units_per_tick,
            mem_mb=s.server.mem_mb,
            rate_limit_per_sec=s.server.rate_limit_per_sec,
        )
        for s in state.servers
    )
    _require(
        condition=tuple(s.id for s in simulation.servers)
        == tuple(sorted({s.id for s in simulation.servers})),
        message="Membership must be unique and sorted",
    )
    original = {server.id: server for server in servers}
    members = {server.id: server for server in simulation.servers}
    _require(
        condition=original.items() <= members.items(),
        message="Original membership or capacities changed",
    )
    template = simulation.config.autoscaling.server_template

    def capacity(sid: str) -> tuple[int, int]:
        if sid in original:
            return original[sid].cpu_units_per_tick, original[sid].mem_mb
        match = re.fullmatch(pattern=r"autoscale-([0-9]{6,})", string=sid)
        _require(
            condition=match is not None
            and template is not None
            and simulation.config.autoscaling.enabled
            and 1 <= int(match[1]) <= state.autoscaler.counter
            and sid == f"autoscale-{int(match[1]):06d}",
            message="Unknown generated server identity",
        )
        if template is None:
            raise OperationError(code="CHECKPOINT_INVALID", message="Missing scaling template")
        return template.cpu_units_per_tick, template.mem_mb

    for server in simulation.servers:
        _require(
            condition=(server.cpu_units_per_tick, server.mem_mb) == capacity(sid=server.id)
            and (
                server.id in original
                or (
                    template is not None
                    and server.rate_limit_per_sec == template.rate_limit_per_sec
                )
            ),
            message="Membership capacities differ from frozen inputs",
        )
    metrics = MetricAccumulator(
        requests=requests, servers=servers, max_series=simulation.config.max_metric_series
    )
    metrics.current_servers = members
    metrics.arrivals = set(state.metrics.arrivals)
    metrics.starts = {item.request_id: (item.t, item.server_id) for item in state.metrics.starts}
    metrics.finishes = {item.request_id: item.t for item in state.metrics.finishes}
    metrics.drops = {item.request_id: item.t for item in state.metrics.drops}
    _require(
        condition=len(metrics.starts) == len(state.metrics.starts),
        message="Duplicate request starts",
    )
    _require(
        condition=len(metrics.finishes) == len(state.metrics.finishes),
        message="Duplicate completions",
    )
    _require(condition=len(metrics.drops) == len(state.metrics.drops), message="Duplicate drops")
    _require(
        condition=set(metrics.starts) <= metrics.arrivals
        and set(metrics.finishes) <= set(metrics.starts)
        and set(metrics.drops) <= metrics.arrivals
        and not set(metrics.drops) & set(metrics.starts),
        message="Invalid checkpoint request lifecycle",
    )
    metrics.servers = {}
    for total in state.metrics.servers:
        sid = total.server_id
        _require(condition=sid not in metrics.servers, message="Duplicate server integrals")
        _require(
            condition=(total.cpu_units_per_tick, total.mem_mb) == capacity(sid=sid),
            message="Historical capacities changed",
        )
        allocated = _fraction(value=total.cpu_allocated.to_fraction())
        consumed = _fraction(value=total.cpu_consumed.to_fraction())
        _require(
            condition=0 <= consumed <= allocated <= total.cpu_capacity_ticks
            and total.memory_reserved_mb_ticks <= total.memory_capacity_mb_ticks
            and total.cpu_capacity_ticks % total.cpu_units_per_tick == 0
            and total.memory_capacity_mb_ticks % total.mem_mb == 0
            and total.cpu_capacity_ticks // total.cpu_units_per_tick
            == total.memory_capacity_mb_ticks // total.mem_mb
            <= state.next_tick,
            message="Invalid historical resource integrals",
        )
        metrics.servers[sid] = ServerTotals(
            cpu_units_per_tick=total.cpu_units_per_tick,
            mem_mb=total.mem_mb,
            cpu_allocated=allocated,
            cpu_consumed=consumed,
            cpu_capacity_ticks=total.cpu_capacity_ticks,
            memory_reserved_mb_ticks=total.memory_reserved_mb_ticks,
            memory_capacity_mb_ticks=total.memory_capacity_mb_ticks,
        )
    _require(
        condition=set(members) <= set(metrics.servers),
        message="Current membership missing from metrics",
    )
    for rid, (t, sid) in metrics.starts.items():
        request = simulation.by_id[rid]
        _require(
            condition=request.t <= t < state.next_tick and sid in metrics.servers,
            message="Invalid start tick/server",
        )
        total = metrics.servers[sid]
        total.started += 1
        total.waits.append(t - request.t)
    for rid, t in metrics.finishes.items():
        start, sid = metrics.starts[rid]
        _require(condition=start < t < state.next_tick, message="Invalid completion tick")
        total = metrics.servers[sid]
        total.finished += 1
        total.responses.append(t - simulation.by_id[rid].t)
        total.executions.append(t - start)
    for rid, t in metrics.drops.items():
        _require(
            condition=simulation.by_id[rid].t <= t < state.next_tick, message="Invalid drop tick"
        )
    simulation.active, simulation.reserved, simulation.last_start_counters = {}, {}, {}
    live: set[str] = set()
    work_consumed = dict.fromkeys(metrics.servers, Fraction())
    for rid in metrics.finishes:
        sid = metrics.starts[rid][1]
        work_consumed[sid] = _fraction(value=work_consumed[sid] + simulation.by_id[rid].work_units)
    for item in state.servers:
        sid = item.server.id
        active: dict[str, Fraction] = {}
        for work in item.active:
            rid = work.request_id
            _require(
                condition=rid in metrics.starts and rid not in metrics.finishes and rid not in live,
                message="Invalid active request",
            )
            remaining = _fraction(
                value=work.remaining.to_fraction(), bits=simulation.config.max_fraction_bits
            )
            _require(
                condition=metrics.starts[rid][1] == sid
                and 0 <= remaining <= simulation.by_id[rid].work_units,
                message="Invalid remaining work or active server",
            )
            active[rid] = remaining
            live.add(rid)
            work_consumed[sid] = _fraction(
                value=work_consumed[sid] + simulation.by_id[rid].work_units - remaining
            )
        _require(
            condition=sum(simulation.by_id[rid].mem_mb for rid in active)
            == item.reserved_mem_mb
            <= item.server.mem_mb
            and item.start_counter <= item.server.rate_limit_per_sec
            and (simulation.config.mode == "concurrent" or len(active) <= 1),
            message="Invalid memory reservation, start counter, or sequential concurrency",
        )
        _require(
            condition=item.start_counter
            == sum(
                t == state.next_tick - 1 and server_id == sid
                for t, server_id in metrics.starts.values()
            ),
            message="Last interval start counters do not match request times",
        )
        simulation.active[sid] = active
        simulation.reserved[sid] = item.reserved_mem_mb
        simulation.last_start_counters[sid] = item.start_counter
    _require(
        condition=live == set(metrics.starts) - set(metrics.finishes),
        message="Missing active request state",
    )
    _require(
        condition=all(
            metrics.servers[sid].cpu_consumed == work for sid, work in work_consumed.items()
        ),
        message="CPU conservation mismatch",
    )
    _require(
        condition=set(state.pending_ids)
        == metrics.arrivals - set(metrics.starts) - set(metrics.drops)
        and len(state.pending_ids) == len(set(state.pending_ids)),
        message="Pending queue/lifecycle mismatch",
    )
    simulation.pending = [simulation.by_id[rid] for rid in state.pending_ids]
    _require(
        condition=simulation.pending
        == sorted(simulation.pending, key=lambda r: (r.t, r.request_id)),
        message="Queue order changed",
    )
    _require(
        condition=simulation.config.max_queue_size is None
        or len(simulation.pending) <= simulation.config.max_queue_size,
        message="Queue capacity exceeded",
    )
    _require(
        condition=state.metrics.queue_peak >= len(simulation.pending)
        and state.metrics.queue_integral <= len(requests) * state.next_tick,
        message="Invalid queue integrals",
    )
    metrics.next_tick = simulation.tick = state.next_tick
    metrics.queue_peak, metrics.queue_integral = (
        state.metrics.queue_peak,
        state.metrics.queue_integral,
    )
    simulation.cursor = state.input_cursor
    simulation.anchor = state.round_robin_anchor
    _require(
        condition=simulation.anchor is None
        or (simulation.config.strategy == "round_robin" and simulation.anchor in metrics.servers),
        message="Invalid scheduler anchor",
    )
    simulation.recorded_samples = state.recorded_samples
    simulation.executed_intervals = state.executed_intervals
    simulation.event_count = state.event_count
    simulation.decision_count = state.decision_count
    if simulation.config.autoscaling.enabled:
        samples = sum(
            total.cpu_capacity_ticks // total.cpu_units_per_tick
            for total in metrics.servers.values()
        )
    else:
        samples = len(servers) * state.executed_intervals
    _require(
        condition=samples == state.recorded_samples,
        message="Recorded sample count does not match intervals",
    )
    _require(
        condition=state.event_count
        == len(metrics.arrivals) + len(metrics.starts) + len(metrics.finishes) + len(metrics.drops),
        message="Event sequence does not match lifecycle counts",
    )
    _restore_scaler(simulation=simulation, state=state, metrics=metrics)
    prefixes = {prefix.name: prefix for prefix in state.prefixes}
    _require(
        condition=len(prefixes) == 4 and set(prefixes) == set(PREFIX_NAMES),
        message="Invalid prefix names",
    )
    _require(
        condition=prefixes["run.jsonl"].records == state.event_count
        and prefixes["autoscaling_decisions.jsonl"].records == state.decision_count
        and prefixes["timeline.jsonl"].records == state.executed_intervals
        and prefixes["drop_reasons.jsonl"].records == len(metrics.drops)
        and sum(prefix.size_bytes for prefix in state.prefixes) <= 512 * 1024 * 1024,
        message="Prefix sequences or storage bounds disagree with execution state",
    )
    simulation.checkpoint_ready = True
    return RestoredCheckpoint(simulation=simulation, metrics=metrics, prefixes=state.prefixes)


def _restore_scaler(
    simulation: Simulation, state: IntervalCheckpoint, metrics: MetricAccumulator
) -> None:
    """Reject inconsistent scaling windows and rebuild only the verified frozen policy state."""
    policy, saved = simulation.config.autoscaling, state.autoscaler
    pending = saved.pending
    if not policy.enabled:
        _require(
            condition=not saved.history
            and saved.counter == saved.next_eligible_tick == state.decision_count == 0
            and pending is None,
            message="Disabled scaler contains runtime state",
        )
    else:
        count = min(policy.window_ticks, state.executed_intervals)
        _require(
            condition=state.executed_intervals == state.next_tick
            and tuple(item.ending_tick for item in saved.history)
            == tuple(range(state.next_tick - count + 1, state.next_tick + 1))
            and state.decision_count <= state.executed_intervals,
            message="Scaling observation window or sequence is inconsistent",
        )
        _require(
            condition=policy.min_servers is not None and policy.max_servers is not None,
            message="Unresolved policy",
        )
        minimum, maximum = policy.min_servers, policy.max_servers
        if minimum is None or maximum is None:
            raise OperationError(code="CHECKPOINT_INVALID", message="Unresolved policy bounds")
        _require(
            condition=minimum <= len(simulation.servers) <= maximum,
            message="Membership outside policy bounds",
        )
    drops_by_tick = Counter(metrics.drops.values())
    arrivals_by_tick = Counter(request.t for request in simulation.requests)
    for item in saved.history:
        cpu = _fraction(
            value=item.cpu_utilization.to_fraction(), bits=simulation.config.max_fraction_bits
        )
        ratio = _fraction(
            value=item.drop_ratio.to_fraction(), bits=simulation.config.max_fraction_bits
        )
        _require(
            condition=0 <= cpu <= 1 and 0 <= ratio <= 10_000, message="Invalid scaling observation"
        )
        drops = drops_by_tick[item.ending_tick - 1]
        arrivals = arrivals_by_tick[item.ending_tick - 1]
        _require(
            condition=item.drops == drops
            and ratio == Fraction(numerator=drops, denominator=max(1, arrivals)),
            message="Scaling drop observations disagree with request lifecycle",
        )
        simulation.autoscaler.history.append(
            Observation(
                ending_tick=item.ending_tick,
                cpu_utilization=cpu,
                drops=item.drops,
                drop_ratio=ratio,
            )
        )
    _require(
        condition=saved.next_eligible_tick <= state.next_tick + policy.cooldown_ticks
        and (saved.counter > 0 or saved.next_eligible_tick == 0),
        message="Scaling cooldown exceeds possible decision history",
    )
    simulation.autoscaler.counter = saved.counter
    simulation.autoscaler.next_eligible_tick = saved.next_eligible_tick
    added = set(pending.server_ids_added) if pending is not None else set()
    _require(
        condition=len(metrics.servers)
        == len(simulation.initial_servers) + saved.counter - len(added)
        and saved.counter + len(simulation.initial_servers) <= simulation.config.max_metric_series,
        message="Generated membership history count mismatch",
    )
    if pending is not None:
        members = set(simulation.active)
        originals = {server.id for server in simulation.initial_servers}
        removed = set(pending.server_ids_removed)
        _require(
            condition=pending.t == state.next_tick - 1
            and pending.applied_at == state.next_tick
            and saved.next_eligible_tick == pending.t + policy.cooldown_ticks + 1
            and (
                (pending.action == "scale_out" and bool(added) and not removed)
                or (pending.action == "scale_in" and bool(removed) and not added)
            )
            and len(added) == len(pending.server_ids_added)
            and len(removed) == len(pending.server_ids_removed)
            and not added & set(metrics.servers)
            and removed <= members - originals
            and all(not simulation.active[sid] for sid in removed),
            message="Invalid pending membership change",
        )
        if added:
            _require(
                condition=pending.server_ids_added
                == tuple(
                    f"autoscale-{index:06d}"
                    for index in range(saved.counter - len(added) + 1, saved.counter + 1)
                ),
                message="Pending generated names are not contiguous",
            )
        _require(
            condition=len(added) <= policy.scale_out_step
            and len(removed) <= policy.scale_in_step
            and pending.server_ids_removed == tuple(sorted(removed, reverse=True))
            and policy.min_servers is not None
            and policy.max_servers is not None
            and policy.min_servers
            <= len(members) + len(added) - len(removed)
            <= policy.max_servers,
            message="Pending membership change exceeds step or population bounds",
        )
        simulation.autoscaler.pending = ScaleDecision(
            t=pending.t,
            action=pending.action,
            trigger=pending.trigger,
            applied_at=pending.applied_at,
            server_ids_added=pending.server_ids_added,
            server_ids_removed=pending.server_ids_removed,
            reason=pending.reason,
        )
