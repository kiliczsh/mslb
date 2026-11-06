"""Independent static trace replay; never calls engine scheduling or CPU logic."""

import json
from collections.abc import Iterator
from dataclasses import dataclass
from fractions import Fraction
from typing import BinaryIO

from mslb_engine.models import Request, RunConfig, Server
from mslb_engine.replay_errors import ReplayError
from mslb_engine.scaling_replay import ScalingReplay

_PRIORITY = {
    "REQUEST_FINISHED": 0,
    "REQUEST_ARRIVED": 1,
    "REQUEST_DROPPED": 2,
    "REQUEST_STARTED": 3,
}


@dataclass(frozen=True)
class ReplaySummary:
    arrived: int
    started: int
    finished: int
    dropped: int
    terminal_tick: int
    events: int


@dataclass(frozen=True)
class _Record:
    t: int
    kind: str
    rid: str
    sid: str | None


def _records(stream: BinaryIO, count: int, horizon: int) -> Iterator[_Record]:
    previous: tuple[int, int, str, str] | None = None
    for index in range(3 * count + 1):
        raw = stream.readline(2049)
        if not raw:
            return
        if index == 3 * count or len(raw) > 2048 or not raw.endswith(b"\n"):
            raise ReplayError("Trace exceeds event/line bounds or lacks final LF")
        try:
            value: object = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ReplayError("Invalid UTF-8 JSON event") from None
        if not isinstance(value, dict):
            raise ReplayError("Event must be a JSON object")
        tick, kind, rid, sid = (
            value.get("t"),
            value.get("event"),
            value.get("request_id"),
            value.get("server_id"),
        )
        if type(tick) is not int or not 0 <= tick <= horizon:
            raise ReplayError("Invalid event tick")
        if not isinstance(kind, str) or kind not in _PRIORITY or not isinstance(rid, str):
            raise ReplayError("Invalid event kind or request ID")
        canonical: dict[str, int | str] = {"t": tick, "event": kind, "request_id": rid}
        assigned = kind in {"REQUEST_STARTED", "REQUEST_FINISHED"}
        if assigned:
            if not isinstance(sid, str):
                raise ReplayError("Assigned event requires a server ID")
            canonical["server_id"] = sid
        elif sid is not None:
            raise ReplayError("Arrival/drop cannot include a server ID")
        if (
            raw
            != (json.dumps(canonical, ensure_ascii=False, separators=(",", ":")) + "\n").encode()
        ):
            raise ReplayError("Noncanonical fields, key order, or JSON encoding")
        key = (tick, _PRIORITY[kind], rid, str(sid) if assigned else "")
        if previous is not None and key < previous:
            raise ReplayError("Events are out of canonical order")
        previous = key
        yield _Record(tick, kind, rid, sid if isinstance(sid, str) else None)


def validate_trace(
    stream: BinaryIO,
    servers: tuple[Server, ...],
    requests: tuple[Request, ...],
    config: RunConfig,
    scale_stream: BinaryIO | None = None,
) -> ReplaySummary:
    """Reconstruct lifecycle, shares, constraints, drops, and terminal accounting.

    Selection strategy is deliberately not reused as an oracle. Every recorded
    assignment must be eligible and work-conserving; strategy identity is checked
    separately by golden fixtures. Dynamic membership requires an independently
    reconstructed scaling-decision sidecar.
    """
    if not 1 <= len(servers) <= 64 or not 1 <= len(requests) <= 10_000:
        raise ReplayError("Invalid input collection size")
    if len(servers) > config.max_metric_series:
        raise ReplayError("Initial cluster exceeds metric history budget")
    by_server = {server.id: server for server in sorted(servers, key=lambda server: server.id)}
    by_request = {request.request_id: request for request in requests}
    if len(by_server) != len(servers) or len(by_request) != len(requests):
        raise ReplayError("Duplicate input IDs")
    ordered_requests = sorted(requests, key=lambda request: (request.t, request.request_id))
    if ordered_requests[-1].t >= config.max_simulation_ticks:
        raise ReplayError("Arrival outside configured horizon")
    records = iter(_records(stream, len(requests), config.max_simulation_ticks))
    following = next(records, None)
    pending: dict[str, Request] = {}
    running: dict[str, dict[str, Fraction]] = {sid: {} for sid in by_server}
    memory = dict.fromkeys(by_server, 0)
    started: set[str] = set()
    finished: set[str] = set()
    dropped: set[str] = set()
    arrivals = 0
    event_count = 0
    sample_count = 0
    tick = 0
    max_memory = max(server.mem_mb for server in servers)
    policy = config.autoscaling.resolved(len(servers))
    scaling = ScalingReplay(
        scale_stream, policy, servers, config.max_fraction_bits, config.max_metric_series
    )
    if (
        policy.enabled
        and policy.server_template is not None
        and policy.max_servers is not None
        and policy.max_servers > len(servers)
    ):
        max_memory = max(max_memory, policy.server_template.mem_mb)
    while tick <= config.max_simulation_ticks:
        scaling.activate(tick, by_server, running, memory)
        boundary: dict[str, list[_Record]] = {kind: [] for kind in _PRIORITY}
        while following is not None and following.t == tick:
            if following.rid not in by_request:
                raise ReplayError("Unknown request ID")
            boundary[following.kind].append(following)
            event_count += 1
            following = next(records, None)
        for values in boundary.values():
            if len({record.rid for record in values}) != len(values):
                raise ReplayError("Duplicate lifecycle event")
        due = {
            rid: sid for sid, active in running.items() for rid, left in active.items() if left == 0
        }
        finishes = boundary["REQUEST_FINISHED"]
        if {record.rid for record in finishes} != set(due):
            raise ReplayError("Missing, early, or late completion")
        for record in finishes:
            sid = due[record.rid]
            if record.sid != sid:
                raise ReplayError("Finish server differs from start server")
            del running[sid][record.rid]
            memory[sid] -= by_request[record.rid].mem_mb
            finished.add(record.rid)
        arriving: list[Request] = []
        while arrivals < len(ordered_requests) and ordered_requests[arrivals].t == tick:
            arriving.append(ordered_requests[arrivals])
            arrivals += 1
        if {record.rid for record in boundary["REQUEST_ARRIVED"]} != {
            request.request_id for request in arriving
        }:
            raise ReplayError("Missing, duplicate, or mistimed arrival")
        pending.update((request.request_id, request) for request in arriving)
        due_drops = {
            rid
            for rid, request in pending.items()
            if request.mem_mb > max_memory
            or (
                config.queue_timeout_ticks is not None
                and tick - request.t >= config.queue_timeout_ticks
            )
        }
        for rid in due_drops:
            del pending[rid]
        starts = dict.fromkeys(by_server, 0)
        for record in boundary["REQUEST_STARTED"]:
            sid, rid = record.sid, record.rid
            if (
                sid is None
                or sid not in by_server
                or rid not in pending
                or rid in started
                or rid in dropped
            ):
                raise ReplayError("Illegal start or unknown server")
            server, request = by_server[sid], pending[rid]
            if (
                memory[sid] + request.mem_mb > server.mem_mb
                or starts[sid] >= server.rate_limit_per_sec
                or (config.mode == "sequential" and running[sid])
            ):
                raise ReplayError("Memory, rate, or sequential overlap violation")
            starts[sid] += 1
            memory[sid] += request.mem_mb
            running[sid][rid] = Fraction(request.work_units)
            started.add(rid)
            del pending[rid]
        # A trace may not invent queue waiting or overflow drops while an eligible
        # server could have accepted the request at this boundary.
        for request in pending.values():
            if any(
                request.mem_mb + memory[s.id] <= s.mem_mb
                and starts[s.id] < s.rate_limit_per_sec
                and (config.mode == "concurrent" or not running[s.id])
                for s in by_server.values()
            ):
                raise ReplayError("Scheduling is not work-conserving")
        if config.max_queue_size is not None:
            survivors = sorted(
                pending.values(), key=lambda request: (request.t, request.request_id)
            )
            for request in survivors[config.max_queue_size :]:
                due_drops.add(request.request_id)
                del pending[request.request_id]
        if {record.rid for record in boundary["REQUEST_DROPPED"]} != due_drops:
            raise ReplayError("Illegal, missing, or mistimed drop")
        dropped.update(due_drops)
        if arrivals == len(requests) and not pending and not any(running.values()):
            if following is not None or len(finished) + len(dropped) != len(requests):
                raise ReplayError("Trailing events or incomplete terminal accounting")
            scaling.finish()
            return ReplaySummary(
                arrivals, len(started), len(finished), len(dropped), tick, event_count
            )
        if tick == config.max_simulation_ticks:
            raise ReplayError("Trace does not terminate before horizon")
        sample_count += len(by_server)
        if sample_count > config.max_samples:
            raise ReplayError("Recorded sample bound exceeded")
        scaling.check_decision(tick, by_server, running, len(pending))
        total_consumed = Fraction()
        for sid in sorted(running):
            active = running[sid]
            if active:
                allocation = Fraction(by_server[sid].cpu_units_per_tick, len(active))
                consumed = Fraction(0)
                if (
                    max(allocation.numerator.bit_length(), allocation.denominator.bit_length())
                    > config.max_fraction_bits
                ):
                    raise ReplayError("Arithmetic complexity bound exceeded")
                for rid in sorted(active):
                    consumed += min(active[rid], allocation)
                    remaining = max(Fraction(0), active[rid] - allocation)
                    if (
                        max(consumed.numerator.bit_length(), consumed.denominator.bit_length())
                        > config.max_fraction_bits
                    ):
                        raise ReplayError("Arithmetic complexity bound exceeded")
                    if (
                        max(remaining.numerator.bit_length(), remaining.denominator.bit_length())
                        > config.max_fraction_bits
                    ):
                        raise ReplayError("Arithmetic complexity bound exceeded")
                    active[rid] = remaining
                total_consumed += consumed
                if (
                    max(
                        total_consumed.numerator.bit_length(),
                        total_consumed.denominator.bit_length(),
                    )
                    > config.max_fraction_bits
                ):
                    raise ReplayError("Cluster arithmetic complexity bound exceeded")
        scaling.observe(
            total_consumed,
            sum(server.cpu_units_per_tick for server in by_server.values()),
            len(due_drops),
            len(arriving),
        )
        tick += 1
        if (
            not pending
            and not any(running.values())
            and arrivals < len(requests)
            and not policy.enabled
        ):
            next_arrival = ordered_requests[arrivals].t
            tick = min(next_arrival, following.t if following is not None else next_arrival)
    raise ReplayError("Unreachable terminal state")
