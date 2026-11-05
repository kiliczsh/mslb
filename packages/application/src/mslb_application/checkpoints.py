"""Bounded immutable checkpoint files and verified completed-interval restore."""

import hashlib
import json
import os
import re
import tempfile
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from fractions import Fraction
from itertools import chain
from pathlib import Path
from typing import BinaryIO

from mslb_contracts.checkpoints import (
    ActiveWork,
    ArtifactPrefix,
    EndedRequest,
    IntervalCheckpoint,
    MetricExecution,
    PendingChange,
    PrefixName,
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
from pydantic import ValidationError

from mslb_application.errors import OperationError
from mslb_application.snapshots import domain_config, domain_inputs, verify_snapshot

MAX_CHECKPOINT_BYTES = 32 * 1024 * 1024
PREFIX_NAMES: tuple[PrefixName, ...] = (
    "run.jsonl",
    "autoscaling_decisions.jsonl",
    "timeline.jsonl",
    "drop_reasons.jsonl",
)


@dataclass(frozen=True, slots=True)
class CheckpointArtifact:
    path: Path
    sha256: str
    size_bytes: int
    next_tick: int
    executed_intervals: int
    event_count: int


@dataclass(frozen=True, slots=True)
class RestoredCheckpoint:
    simulation: Simulation
    metrics: MetricAccumulator
    prefixes: tuple[ArtifactPrefix, ...]


def checkpoint_due(simulation: Simulation, previous: CheckpointArtifact | None = None) -> bool:
    """Use executed intervals/events, never fast-forwarded wall-clock ticks."""
    intervals = previous.executed_intervals if previous is not None else 0
    events = previous.event_count if previous is not None else 0
    return simulation.checkpoint_ready and (
        simulation.executed_intervals - intervals >= 100 or simulation.event_count - events >= 1000
    )


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise OperationError("CHECKPOINT_INVALID", message)


def _fraction(value: Fraction, bits: int = 4096) -> Fraction:
    _require(
        max(value.numerator.bit_length(), value.denominator.bit_length()) <= bits,
        "Checkpoint arithmetic complexity exceeds the frozen limit",
    )
    return value


def _server_input(server: Server) -> ServerInput:
    return ServerInput(
        id=server.id,
        cpu_units_per_tick=server.cpu_units_per_tick,
        mem_mb=server.mem_mb,
        rate_limit_per_sec=server.rate_limit_per_sec,
    )


def _scan_prefix(
    name: PrefixName, stream: BinaryIO, expected: ArtifactPrefix | None = None
) -> ArtifactPrefix:
    stream.seek(0)
    digest, size, records, last = hashlib.sha256(), 0, 0, b""
    maximum = expected.size_bytes if expected is not None else 256 * 1024 * 1024
    while size < maximum:
        block = stream.read(min(64 * 1024, maximum - size))
        if not block:
            break
        digest.update(block)
        size += len(block)
        records += block.count(b"\n")
        last = block[-1:]
    if expected is None:
        _require(not stream.read(1), "Checkpoint prefix exceeds its storage limit")
        stream.seek(size)
    _require(not size or last == b"\n", "Checkpoint prefix ends inside a record")
    actual = ArtifactPrefix(name=name, size_bytes=size, records=records, sha256=digest.hexdigest())
    _require(expected is None or actual == expected, "Checkpoint prefix length or digest mismatch")
    return actual


def _preflight(simulation: Simulation, metrics: MetricAccumulator, maximum: int) -> None:
    """Bound duplicate DTO/JSON allocation before constructing it.

    Fixed allowances cover field names, separators, bounded integers and escaped
    identifiers. Fraction decimal digits are estimated from bit lengths, without
    first allocating decimal representations of every exact value.
    """
    _require(type(maximum) is int and 1 <= maximum <= MAX_CHECKPOINT_BYTES, "Invalid byte budget")
    _require(
        simulation.checkpoint_ready
        and not simulation.terminal
        and not metrics.terminal
        and simulation.tick == metrics.next_tick
        and simulation.tick > 0,
        "Checkpoint requires a completed, observed, nonterminal interval",
    )
    estimated = (
        8192
        + sum(
            512
            + 8 * len(json.dumps(request.request_id, ensure_ascii=False).encode("utf-8"))
            + 6
            * len(
                json.dumps(
                    metrics.starts.get(request.request_id, (0, ""))[1], ensure_ascii=False
                ).encode("utf-8")
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
        _fraction(value)
        estimated += (
            value.numerator.bit_length() + value.denominator.bit_length()
        ) * 30103 // 100000 + 96
    if estimated > maximum:
        raise OperationError("RESOURCE_LIMIT_EXCEEDED", "Checkpoint allocation exceeds its budget")


def _capture(
    snapshot: RunSnapshot,
    simulation: Simulation,
    metrics: MetricAccumulator,
    streams: Mapping[str, BinaryIO],
) -> IntervalCheckpoint:
    _require(set(streams) == set(PREFIX_NAMES), "All four checkpoint prefixes are required")
    pending = simulation.autoscaler.pending
    change = (
        PendingChange.model_validate_json(pending.canonical_line()) if pending is not None else None
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
                server=_server_input(server),
                reserved_mem_mb=simulation.reserved[server.id],
                start_counter=simulation.last_start_counters[server.id],
                active=tuple(
                    ActiveWork(request_id=rid, remaining=FractionValue.from_fraction(work))
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
                    cpu_utilization=FractionValue.from_fraction(item.cpu_utilization),
                    drops=item.drops,
                    drop_ratio=FractionValue.from_fraction(item.drop_ratio),
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
                    cpu_allocated=FractionValue.from_fraction(total.cpu_allocated),
                    cpu_consumed=FractionValue.from_fraction(total.cpu_consumed),
                    cpu_capacity_ticks=total.cpu_capacity_ticks,
                    memory_reserved_mb_ticks=total.memory_reserved_mb_ticks,
                    memory_capacity_mb_ticks=total.memory_capacity_mb_ticks,
                )
                for sid, total in sorted(metrics.servers.items())
            ),
            queue_peak=metrics.queue_peak,
            queue_integral=metrics.queue_integral,
        ),
        prefixes=tuple(_scan_prefix(name, streams[name]) for name in PREFIX_NAMES),
    )


def _restore(snapshot: RunSnapshot, state: IntervalCheckpoint) -> RestoredCheckpoint:
    """Rebuild known classes explicitly; no pickle, imports, or executable payloads."""
    _require(
        state.snapshot_digest == snapshot.snapshot_digest
        and state.engine_version == snapshot.engine_version,
        "Checkpoint belongs to incompatible inputs, configuration, or engine",
    )
    servers, requests = domain_inputs(snapshot)
    simulation = Simulation(servers, requests, domain_config(snapshot.configuration))
    _require(
        state.next_tick <= simulation.config.max_simulation_ticks, "Invalid checkpoint horizon"
    )
    _require(state.recorded_samples <= simulation.config.max_samples, "Sample budget exceeded")
    _require(state.executed_intervals <= state.next_tick, "Invalid executed interval count")
    _require(
        len(state.metrics.servers) <= simulation.config.max_metric_series, "History budget exceeded"
    )
    _require(state.input_cursor <= len(requests), "Invalid input cursor")
    expected_arrivals = tuple(sorted(r.request_id for r in requests[: state.input_cursor]))
    _require(state.metrics.arrivals == expected_arrivals, "Arrival cursor/lifecycle mismatch")
    _require(
        all(r.t < state.next_tick for r in requests[: state.input_cursor])
        and all(r.t >= state.next_tick for r in requests[state.input_cursor :]),
        "Input cursor crosses the next boundary",
    )
    simulation.servers = tuple(
        Server(
            s.server.id, s.server.cpu_units_per_tick, s.server.mem_mb, s.server.rate_limit_per_sec
        )
        for s in state.servers
    )
    _require(
        tuple(s.id for s in simulation.servers)
        == tuple(sorted({s.id for s in simulation.servers})),
        "Membership must be unique and sorted",
    )
    original = {server.id: server for server in servers}
    members = {server.id: server for server in simulation.servers}
    _require(original.items() <= members.items(), "Original membership or capacities changed")
    template = simulation.config.autoscaling.server_template

    def capacity(sid: str) -> tuple[int, int]:
        if sid in original:
            return original[sid].cpu_units_per_tick, original[sid].mem_mb
        match = re.fullmatch(r"autoscale-([0-9]{6,})", sid)
        _require(
            match is not None
            and template is not None
            and simulation.config.autoscaling.enabled
            and 1 <= int(match[1]) <= state.autoscaler.counter
            and sid == f"autoscale-{int(match[1]):06d}",
            "Unknown generated server identity",
        )
        if template is None:
            raise OperationError("CHECKPOINT_INVALID", "Missing scaling template")
        return template.cpu_units_per_tick, template.mem_mb

    for server in simulation.servers:
        _require(
            (server.cpu_units_per_tick, server.mem_mb) == capacity(server.id)
            and (
                server.id in original
                or template is not None
                and server.rate_limit_per_sec == template.rate_limit_per_sec
            ),
            "Membership capacities differ from frozen inputs",
        )
    metrics = MetricAccumulator(requests, servers, simulation.config.max_metric_series)
    metrics.current_servers = members
    metrics.arrivals = set(state.metrics.arrivals)
    metrics.starts = {item.request_id: (item.t, item.server_id) for item in state.metrics.starts}
    metrics.finishes = {item.request_id: item.t for item in state.metrics.finishes}
    metrics.drops = {item.request_id: item.t for item in state.metrics.drops}
    _require(len(metrics.starts) == len(state.metrics.starts), "Duplicate request starts")
    _require(len(metrics.finishes) == len(state.metrics.finishes), "Duplicate completions")
    _require(len(metrics.drops) == len(state.metrics.drops), "Duplicate drops")
    _require(
        set(metrics.starts) <= metrics.arrivals
        and set(metrics.finishes) <= set(metrics.starts)
        and set(metrics.drops) <= metrics.arrivals
        and not set(metrics.drops) & set(metrics.starts),
        "Invalid checkpoint request lifecycle",
    )
    metrics.servers = {}
    for total in state.metrics.servers:
        sid = total.server_id
        _require(sid not in metrics.servers, "Duplicate server integrals")
        _require(
            (total.cpu_units_per_tick, total.mem_mb) == capacity(sid),
            "Historical capacities changed",
        )
        allocated = _fraction(total.cpu_allocated.to_fraction())
        consumed = _fraction(total.cpu_consumed.to_fraction())
        _require(
            0 <= consumed <= allocated <= total.cpu_capacity_ticks
            and total.memory_reserved_mb_ticks <= total.memory_capacity_mb_ticks
            and total.cpu_capacity_ticks % total.cpu_units_per_tick == 0
            and total.memory_capacity_mb_ticks % total.mem_mb == 0
            and total.cpu_capacity_ticks // total.cpu_units_per_tick
            == total.memory_capacity_mb_ticks // total.mem_mb
            <= state.next_tick,
            "Invalid historical resource integrals",
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
    _require(set(members) <= set(metrics.servers), "Current membership missing from metrics")
    for rid, (t, sid) in metrics.starts.items():
        request = simulation.by_id[rid]
        _require(
            request.t <= t < state.next_tick and sid in metrics.servers, "Invalid start tick/server"
        )
        total = metrics.servers[sid]
        total.started += 1
        total.waits.append(t - request.t)
    for rid, t in metrics.finishes.items():
        start, sid = metrics.starts[rid]
        _require(start < t < state.next_tick, "Invalid completion tick")
        total = metrics.servers[sid]
        total.finished += 1
        total.responses.append(t - simulation.by_id[rid].t)
        total.executions.append(t - start)
    for rid, t in metrics.drops.items():
        _require(simulation.by_id[rid].t <= t < state.next_tick, "Invalid drop tick")
    simulation.active, simulation.reserved, simulation.last_start_counters = {}, {}, {}
    live: set[str] = set()
    work_consumed = dict.fromkeys(metrics.servers, Fraction())
    for rid in metrics.finishes:
        sid = metrics.starts[rid][1]
        work_consumed[sid] = _fraction(work_consumed[sid] + simulation.by_id[rid].work_units)
    for item in state.servers:
        sid = item.server.id
        active: dict[str, Fraction] = {}
        for work in item.active:
            rid = work.request_id
            _require(
                rid in metrics.starts and rid not in metrics.finishes and rid not in live,
                "Invalid active request",
            )
            remaining = _fraction(work.remaining.to_fraction(), simulation.config.max_fraction_bits)
            _require(
                metrics.starts[rid][1] == sid
                and 0 <= remaining <= simulation.by_id[rid].work_units,
                "Invalid remaining work or active server",
            )
            active[rid] = remaining
            live.add(rid)
            work_consumed[sid] = _fraction(
                work_consumed[sid] + simulation.by_id[rid].work_units - remaining
            )
        _require(
            sum(simulation.by_id[rid].mem_mb for rid in active)
            == item.reserved_mem_mb
            <= item.server.mem_mb
            and item.start_counter <= item.server.rate_limit_per_sec
            and (simulation.config.mode == "concurrent" or len(active) <= 1),
            "Invalid memory reservation, start counter, or sequential concurrency",
        )
        _require(
            item.start_counter
            == sum(
                t == state.next_tick - 1 and server_id == sid
                for t, server_id in metrics.starts.values()
            ),
            "Last interval start counters do not match request times",
        )
        simulation.active[sid] = active
        simulation.reserved[sid] = item.reserved_mem_mb
        simulation.last_start_counters[sid] = item.start_counter
    _require(live == set(metrics.starts) - set(metrics.finishes), "Missing active request state")
    _require(
        all(metrics.servers[sid].cpu_consumed == work for sid, work in work_consumed.items()),
        "CPU conservation mismatch",
    )
    _require(
        set(state.pending_ids) == metrics.arrivals - set(metrics.starts) - set(metrics.drops)
        and len(state.pending_ids) == len(set(state.pending_ids)),
        "Pending queue/lifecycle mismatch",
    )
    simulation.pending = [simulation.by_id[rid] for rid in state.pending_ids]
    _require(
        simulation.pending == sorted(simulation.pending, key=lambda r: (r.t, r.request_id)),
        "Queue order changed",
    )
    _require(
        simulation.config.max_queue_size is None
        or len(simulation.pending) <= simulation.config.max_queue_size,
        "Queue capacity exceeded",
    )
    _require(
        state.metrics.queue_peak >= len(simulation.pending)
        and state.metrics.queue_integral <= len(requests) * state.next_tick,
        "Invalid queue integrals",
    )
    metrics.next_tick = simulation.tick = state.next_tick
    metrics.queue_peak, metrics.queue_integral = (
        state.metrics.queue_peak,
        state.metrics.queue_integral,
    )
    simulation.cursor = state.input_cursor
    simulation.anchor = state.round_robin_anchor
    _require(
        simulation.anchor is None
        or simulation.config.strategy == "round_robin"
        and simulation.anchor in metrics.servers,
        "Invalid scheduler anchor",
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
    _require(samples == state.recorded_samples, "Recorded sample count does not match intervals")
    _require(
        state.event_count
        == len(metrics.arrivals) + len(metrics.starts) + len(metrics.finishes) + len(metrics.drops),
        "Event sequence does not match lifecycle counts",
    )
    _restore_scaler(simulation, state, metrics)
    prefixes = {prefix.name: prefix for prefix in state.prefixes}
    _require(len(prefixes) == 4 and set(prefixes) == set(PREFIX_NAMES), "Invalid prefix names")
    _require(
        prefixes["run.jsonl"].records == state.event_count
        and prefixes["autoscaling_decisions.jsonl"].records == state.decision_count
        and prefixes["timeline.jsonl"].records == state.executed_intervals
        and prefixes["drop_reasons.jsonl"].records == len(metrics.drops)
        and sum(prefix.size_bytes for prefix in state.prefixes) <= 512 * 1024 * 1024,
        "Prefix sequences or storage bounds disagree with execution state",
    )
    simulation.checkpoint_ready = True
    return RestoredCheckpoint(simulation, metrics, state.prefixes)


def _restore_scaler(
    simulation: Simulation, state: IntervalCheckpoint, metrics: MetricAccumulator
) -> None:
    policy, saved = simulation.config.autoscaling, state.autoscaler
    pending = saved.pending
    if not policy.enabled:
        _require(
            not saved.history
            and saved.counter == saved.next_eligible_tick == state.decision_count == 0
            and pending is None,
            "Disabled scaler contains runtime state",
        )
    else:
        count = min(policy.window_ticks, state.executed_intervals)
        _require(
            state.executed_intervals == state.next_tick
            and tuple(item.ending_tick for item in saved.history)
            == tuple(range(state.next_tick - count + 1, state.next_tick + 1))
            and state.decision_count <= state.executed_intervals,
            "Scaling observation window or sequence is inconsistent",
        )
        _require(
            policy.min_servers is not None and policy.max_servers is not None, "Unresolved policy"
        )
        minimum, maximum = policy.min_servers, policy.max_servers
        if minimum is None or maximum is None:
            raise OperationError("CHECKPOINT_INVALID", "Unresolved policy bounds")
        _require(minimum <= len(simulation.servers) <= maximum, "Membership outside policy bounds")
    drops_by_tick = Counter(metrics.drops.values())
    arrivals_by_tick = Counter(request.t for request in simulation.requests)
    for item in saved.history:
        cpu = _fraction(item.cpu_utilization.to_fraction(), simulation.config.max_fraction_bits)
        ratio = _fraction(item.drop_ratio.to_fraction(), simulation.config.max_fraction_bits)
        _require(0 <= cpu <= 1 and 0 <= ratio <= 10_000, "Invalid scaling observation")
        drops = drops_by_tick[item.ending_tick - 1]
        arrivals = arrivals_by_tick[item.ending_tick - 1]
        _require(
            item.drops == drops and ratio == Fraction(drops, max(1, arrivals)),
            "Scaling drop observations disagree with request lifecycle",
        )
        simulation.autoscaler.history.append(Observation(item.ending_tick, cpu, item.drops, ratio))
    _require(
        saved.next_eligible_tick <= state.next_tick + policy.cooldown_ticks
        and (saved.counter > 0 or saved.next_eligible_tick == 0),
        "Scaling cooldown exceeds possible decision history",
    )
    simulation.autoscaler.counter = saved.counter
    simulation.autoscaler.next_eligible_tick = saved.next_eligible_tick
    added = set(pending.server_ids_added) if pending is not None else set()
    _require(
        len(metrics.servers) == len(simulation.initial_servers) + saved.counter - len(added)
        and saved.counter + len(simulation.initial_servers) <= simulation.config.max_metric_series,
        "Generated membership history count mismatch",
    )
    if pending is not None:
        members = set(simulation.active)
        originals = {server.id for server in simulation.initial_servers}
        removed = set(pending.server_ids_removed)
        _require(
            pending.t == state.next_tick - 1
            and pending.applied_at == state.next_tick
            and saved.next_eligible_tick == pending.t + policy.cooldown_ticks + 1
            and (
                pending.action == "scale_out"
                and bool(added)
                and not removed
                or pending.action == "scale_in"
                and bool(removed)
                and not added
            )
            and len(added) == len(pending.server_ids_added)
            and len(removed) == len(pending.server_ids_removed)
            and not added & set(metrics.servers)
            and removed <= members - originals
            and all(not simulation.active[sid] for sid in removed),
            "Invalid pending membership change",
        )
        if added:
            _require(
                pending.server_ids_added
                == tuple(
                    f"autoscale-{index:06d}"
                    for index in range(saved.counter - len(added) + 1, saved.counter + 1)
                ),
                "Pending generated names are not contiguous",
            )
        _require(
            len(added) <= policy.scale_out_step
            and len(removed) <= policy.scale_in_step
            and pending.server_ids_removed == tuple(sorted(removed, reverse=True))
            and policy.min_servers is not None
            and policy.max_servers is not None
            and policy.min_servers
            <= len(members) + len(added) - len(removed)
            <= policy.max_servers,
            "Pending membership change exceeds step or population bounds",
        )
        simulation.autoscaler.pending = ScaleDecision(
            pending.t,
            pending.action,
            pending.trigger,
            pending.applied_at,
            pending.server_ids_added,
            pending.server_ids_removed,
            pending.reason,
        )


def write_checkpoint(
    directory: Path,
    snapshot: RunSnapshot,
    simulation: Simulation,
    metrics: MetricAccumulator,
    prefixes: Mapping[str, BinaryIO],
    max_bytes: int = MAX_CHECKPOINT_BYTES,
) -> CheckpointArtifact:
    """Fsync a content-addressed file; callers still must commit its DB reference."""
    verify_snapshot(snapshot)
    _preflight(simulation, metrics, max_bytes)
    servers, requests = domain_inputs(snapshot)
    _require(
        simulation.initial_servers == servers
        and simulation.requests == requests
        and simulation.config == domain_config(snapshot.configuration)
        and metrics.requests == simulation.by_id
        and metrics.max_series == simulation.config.max_metric_series,
        "Execution differs from its immutable snapshot",
    )
    state = _capture(snapshot, simulation, metrics, prefixes)
    _restore(snapshot, state)
    directory.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=directory, prefix=".checkpoint-", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            digest, size = hashlib.sha256(), 0
            encoder = json.JSONEncoder(
                ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
            )
            for piece in chain(encoder.iterencode(state.model_dump(mode="json")), ("\n",)):
                data = piece.encode("utf-8")
                size += len(data)
                if size > max_bytes:
                    raise OperationError(
                        "RESOURCE_LIMIT_EXCEEDED", "Checkpoint bytes exceed the budget"
                    )
                if stream.write(data) != len(data):
                    raise OSError("Short checkpoint write")
                digest.update(data)
            stream.flush()
            os.fsync(stream.fileno())
            checksum = digest.hexdigest()
            path = directory / f"checkpoint-{checksum}.json"
            try:
                os.link(temporary, path)
            except FileExistsError:
                _require(not path.is_symlink() and path.is_file(), "Unsafe existing checkpoint")
                with path.open("rb") as existing:
                    _require(
                        hashlib.sha256(existing.read(max_bytes + 1)).hexdigest() == checksum,
                        "Existing checkpoint identity mismatch",
                    )
            directory_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            return CheckpointArtifact(
                path, checksum, size, state.next_tick, state.executed_intervals, state.event_count
            )
        finally:
            stream.close()
            temporary.unlink(missing_ok=True)


def read_checkpoint(
    stream: BinaryIO,
    expected_digest: str,
    snapshot: RunSnapshot,
    prefixes: Mapping[str, BinaryIO],
) -> RestoredCheckpoint:
    """Verify an externally recorded digest and exact prefixes before restore.

    Prefix streams may contain an uncommitted suffix; only the sealed byte range
    is read. On return each stream is positioned at its verified append offset.
    """
    verify_snapshot(snapshot)
    _require(bool(re.fullmatch(r"[0-9a-f]{64}", expected_digest)), "Invalid checkpoint digest")
    payload = stream.read(MAX_CHECKPOINT_BYTES + 1)
    _require(len(payload) <= MAX_CHECKPOINT_BYTES, "Checkpoint payload too large")
    _require(hashlib.sha256(payload).hexdigest() == expected_digest, "Checkpoint digest mismatch")
    try:
        state = IntervalCheckpoint.model_validate_json(payload)
    except ValidationError:
        raise OperationError("CHECKPOINT_INVALID", "Invalid checkpoint schema") from None
    _require(set(prefixes) == set(PREFIX_NAMES), "All four checkpoint prefixes are required")
    restored = _restore(snapshot, state)
    for prefix in state.prefixes:
        _scan_prefix(prefix.name, prefixes[prefix.name], prefix)
    return restored
