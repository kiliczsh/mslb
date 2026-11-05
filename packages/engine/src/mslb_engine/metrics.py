"""Exact bounded simulation aggregates; host/runtime telemetry is unrelated."""

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from fractions import Fraction

from mslb_engine.models import Request, Server, SimulationError, TickFrame

type JsonValue = None | bool | int | str | list[JsonValue] | dict[str, JsonValue]


def _bounded(value: Fraction) -> Fraction:
    if max(value.numerator.bit_length(), value.denominator.bit_length()) > 4096:
        raise SimulationError("RESOURCE_LIMIT_EXCEEDED", "Metric arithmetic exceeds 4096 bits")
    return value


def exact_value(value: Fraction | None) -> JsonValue:
    """Encode authoritative rational values with lossless decimal components."""
    if value is None:
        return None
    _bounded(value)
    if value.denominator == 1 and abs(value.numerator) <= 2**53 - 1:
        return value.numerator
    return {"numerator": str(value.numerator), "denominator": str(value.denominator)}


@dataclass(frozen=True, slots=True)
class Distribution:
    count: int
    mean: Fraction | None
    minimum: int | None
    maximum: int | None
    p50: int | None
    p95: int | None

    def to_data(self) -> dict[str, JsonValue]:
        return {
            "count": self.count,
            "mean": exact_value(self.mean),
            "min": self.minimum,
            "max": self.maximum,
            "p50": self.p50,
            "p95": self.p95,
        }


def distribution(values: Sequence[int]) -> Distribution:
    """Nearest-rank percentiles over at most the admitted request population."""
    if len(values) > 10_000:
        raise SimulationError("RESOURCE_LIMIT_EXCEEDED", "Too many latency samples")
    if any(type(value) is not int or value < 0 for value in values):
        raise SimulationError("VALIDATION_ERROR", "Latency samples must be nonnegative ticks")
    if not values:
        return Distribution(0, None, None, None, None, None)
    ordered = sorted(values)
    count = len(ordered)
    return Distribution(
        count,
        Fraction(sum(ordered), count),
        ordered[0],
        ordered[-1],
        ordered[((50 * count + 99) // 100) - 1],
        ordered[((95 * count + 99) // 100) - 1],
    )


@dataclass(slots=True)
class _ServerTotals:
    cpu_units_per_tick: int = 0
    mem_mb: int = 0
    waits: list[int] = field(default_factory=list)
    responses: list[int] = field(default_factory=list)
    executions: list[int] = field(default_factory=list)
    started: int = 0
    finished: int = 0
    cpu_allocated: Fraction = field(default_factory=Fraction)
    cpu_consumed: Fraction = field(default_factory=Fraction)
    cpu_capacity_ticks: int = 0
    memory_reserved_mb_ticks: int = 0
    memory_capacity_mb_ticks: int = 0

    def to_data(self) -> dict[str, JsonValue]:
        return {
            "cpu_units_per_tick": self.cpu_units_per_tick,
            "mem_mb": self.mem_mb,
            "started": self.started,
            "finished": self.finished,
            "waiting": distribution(self.waits).to_data(),
            "response": distribution(self.responses).to_data(),
            "execution": distribution(self.executions).to_data(),
            "cpu_allocated": exact_value(self.cpu_allocated),
            "cpu_consumed": exact_value(self.cpu_consumed),
            "cpu_capacity_ticks": exact_value(Fraction(self.cpu_capacity_ticks)),
            "cpu_utilization": exact_value(
                self.cpu_consumed / self.cpu_capacity_ticks
                if self.cpu_capacity_ticks
                else Fraction()
            ),
            "memory_reserved_mb_ticks": exact_value(Fraction(self.memory_reserved_mb_ticks)),
            "memory_capacity_mb_ticks": exact_value(Fraction(self.memory_capacity_mb_ticks)),
            "memory_utilization": exact_value(
                Fraction(self.memory_reserved_mb_ticks, self.memory_capacity_mb_ticks)
                if self.memory_capacity_mb_ticks
                else Fraction()
            ),
        }


class MetricAccumulator:
    """Retain only bounded request times and totals, never interval history."""

    def __init__(
        self, requests: tuple[Request, ...], servers: tuple[Server, ...], max_series: int = 10_000
    ) -> None:
        if not 1 <= len(requests) <= 10_000:
            raise SimulationError("VALIDATION_ERROR", "Invalid metric request population")
        self.requests = {request.request_id: request for request in requests}
        if len(self.requests) != len(requests):
            raise SimulationError("VALIDATION_ERROR", "Duplicate metric request IDs")
        self.arrivals: set[str] = set()
        self.starts: dict[str, tuple[int, str]] = {}
        self.finishes: dict[str, int] = {}
        self.drops: dict[str, int] = {}
        if not 1 <= len(servers) <= 64 or len({server.id for server in servers}) != len(servers):
            raise SimulationError("VALIDATION_ERROR", "Invalid metric server population")
        if type(max_series) is not int or not len(servers) <= max_series <= 10_000:
            raise SimulationError("VALIDATION_ERROR", "Invalid metric history bound")
        self.max_series = max_series
        self.current_servers = {server.id: server for server in servers}
        self.servers = {
            server.id: _ServerTotals(
                cpu_units_per_tick=server.cpu_units_per_tick, mem_mb=server.mem_mb
            )
            for server in sorted(servers, key=lambda server: server.id)
        }
        self.next_tick = 0
        self.terminal = False
        self.queue_peak = 0
        self.queue_integral = 0

    def _server(self, sid: str) -> _ServerTotals:
        if sid not in self.servers:
            if len(self.servers) >= self.max_series:
                raise SimulationError(
                    "RESOURCE_LIMIT_EXCEEDED", "Metric server history bound exceeded"
                )
            self.servers[sid] = _ServerTotals()
        return self.servers[sid]

    def observe(self, frame: TickFrame) -> None:
        """Consume exactly one boundary and its analytically sized interval."""
        if self.terminal or frame.t != self.next_tick or frame.duration_ticks < 0:
            raise SimulationError("INVALID_STATE", "Metric boundaries must be contiguous")
        if frame.terminal != (frame.duration_ticks == 0):
            raise SimulationError("INVALID_STATE", "Only terminal boundaries have zero duration")
        if frame.membership:
            self.current_servers = {server.id: server for server in frame.membership}
            for server in frame.membership:
                total = self._server(server.id)
                total.cpu_units_per_tick, total.mem_mb = server.cpu_units_per_tick, server.mem_mb
        for event in frame.events:
            rid = event.request_id
            request = self.requests.get(rid)
            if request is None or event.t != frame.t:
                raise SimulationError("VALIDATION_ERROR", "Invalid metric event identity or tick")
            if event.event == "REQUEST_ARRIVED":
                if rid in self.arrivals or event.t != request.t:
                    raise SimulationError("VALIDATION_ERROR", "Invalid metric arrival")
                self.arrivals.add(rid)
            elif event.event == "REQUEST_STARTED":
                if (
                    rid not in self.arrivals
                    or rid in self.starts
                    or rid in self.drops
                    or event.server_id is None
                ):
                    raise SimulationError("VALIDATION_ERROR", "Invalid metric start")
                self.starts[rid] = (event.t, event.server_id)
                total = self._server(event.server_id)
                total.started += 1
                total.waits.append(event.t - request.t)
            elif event.event == "REQUEST_FINISHED":
                start = self.starts.get(rid)
                if (
                    start is None
                    or rid in self.finishes
                    or event.server_id != start[1]
                    or event.t <= start[0]
                ):
                    raise SimulationError("VALIDATION_ERROR", "Invalid metric completion")
                self.finishes[rid] = event.t
                total = self.servers[start[1]]
                total.finished += 1
                total.responses.append(event.t - request.t)
                total.executions.append(event.t - start[0])
            else:
                if rid not in self.arrivals or rid in self.starts or rid in self.drops:
                    raise SimulationError("VALIDATION_ERROR", "Invalid metric drop")
                self.drops[rid] = event.t
        self.queue_peak = max(self.queue_peak, len(frame.pending_ids))
        self.queue_integral += len(frame.pending_ids) * frame.duration_ticks
        for sample in frame.samples:
            total = self._server(sample.server_id)
            total.cpu_units_per_tick = sample.capacity
            total.mem_mb = sample.memory_capacity
            if (
                sample.reserved_mem_mb < 0
                or sample.consumed_cpu < 0
                or sample.consumed_cpu > sample.capacity
                or sample.reserved_mem_mb > sample.memory_capacity
            ):
                raise SimulationError("VALIDATION_ERROR", "Invalid interval metric sample")
            total.cpu_allocated = _bounded(
                total.cpu_allocated + sample.allocated_cpu * frame.duration_ticks
            )
            total.cpu_consumed = _bounded(
                total.cpu_consumed + sample.consumed_cpu * frame.duration_ticks
            )
            total.cpu_capacity_ticks += sample.capacity * frame.duration_ticks
            total.memory_reserved_mb_ticks += sample.reserved_mem_mb * frame.duration_ticks
            total.memory_capacity_mb_ticks += sample.memory_capacity * frame.duration_ticks
        self.next_tick = frame.t + frame.duration_ticks
        self.terminal = frame.terminal

    def summary(self) -> dict[str, JsonValue]:
        """Return a complete exact report; unfinished attempts have no final report."""
        if not self.terminal or len(self.finishes) + len(self.drops) != len(self.requests):
            raise SimulationError("INVALID_STATE", "Metrics require a terminal accounted run")
        waiting = [start - self.requests[rid].t for rid, (start, _) in self.starts.items()]
        response = [finish - self.requests[rid].t for rid, finish in self.finishes.items()]
        execution = [finish - self.starts[rid][0] for rid, finish in self.finishes.items()]
        makespan = max([*self.finishes.values(), *self.drops.values()]) - min(
            request.t for request in self.requests.values()
        )
        cluster = _ServerTotals()
        for total in self.servers.values():
            cluster.waits.extend(total.waits)
            cluster.responses.extend(total.responses)
            cluster.executions.extend(total.executions)
            cluster.started += total.started
            cluster.finished += total.finished
            cluster.cpu_allocated = _bounded(cluster.cpu_allocated + total.cpu_allocated)
            cluster.cpu_consumed = _bounded(cluster.cpu_consumed + total.cpu_consumed)
            cluster.cpu_capacity_ticks += total.cpu_capacity_ticks
            cluster.memory_reserved_mb_ticks += total.memory_reserved_mb_ticks
            cluster.memory_capacity_mb_ticks += total.memory_capacity_mb_ticks
        cluster.cpu_units_per_tick = sum(
            server.cpu_units_per_tick for server in self.current_servers.values()
        )
        cluster.mem_mb = sum(server.mem_mb for server in self.current_servers.values())
        return {
            "arrived": len(self.arrivals),
            "started": len(self.starts),
            "finished": len(self.finishes),
            "dropped": len(self.drops),
            "makespan": makespan,
            "terminal_tick": self.next_tick,
            "completed_throughput": exact_value(
                Fraction(len(self.finishes), makespan) if makespan else Fraction()
            ),
            "drop_fraction": exact_value(Fraction(len(self.drops), len(self.arrivals))),
            "queue_depth_peak": self.queue_peak,
            "queue_depth_integral": self.queue_integral,
            "waiting": distribution(waiting).to_data(),
            "response": distribution(response).to_data(),
            "execution": distribution(execution).to_data(),
            "cluster": cluster.to_data(),
            "servers": {
                sid: {**total.to_data(), "active": sid in self.current_servers}
                for sid, total in sorted(self.servers.items())
            },
        }


@dataclass(frozen=True, slots=True)
class ChartSample:
    t: int
    duration_ticks: int
    value: Fraction


def chart_buckets(
    samples: Iterable[ChartSample],
    start: int,
    end: int,
    max_points: int = 2000,
) -> list[dict[str, JsonValue]]:
    """Stream exact time-weighted means/min/max without erasing observed peaks."""
    if not (0 <= start < end <= 1_000_000 and 1 <= max_points <= 2000):
        raise SimulationError("VALIDATION_ERROR", "Invalid chart range or point bound")
    width = (end - start + max_points - 1) // max_points
    buckets: dict[int, tuple[Fraction, int, Fraction, Fraction]] = {}
    previous_end = 0
    for index, sample in enumerate(samples):
        if index >= 1_000_000:
            raise SimulationError("RESOURCE_LIMIT_EXCEEDED", "Too many chart samples")
        if sample.t < previous_end or sample.duration_ticks < 1:
            raise SimulationError("VALIDATION_ERROR", "Chart samples must be ordered and disjoint")
        previous_end = sample.t + sample.duration_ticks
        value = _bounded(sample.value)
        cursor, stop = max(start, sample.t), min(end, previous_end)
        while cursor < stop:
            bucket = (cursor - start) // width
            duration = min(stop, start + (bucket + 1) * width) - cursor
            total, weight, minimum, maximum = buckets.get(bucket, (Fraction(), 0, value, value))
            buckets[bucket] = (
                _bounded(total + value * duration),
                weight + duration,
                min(minimum, value),
                max(maximum, value),
            )
            cursor += duration
    return [
        {
            "t": start + bucket * width,
            "end_tick": min(end, start + (bucket + 1) * width),
            "covered_ticks": weight,
            "min": exact_value(minimum),
            "max": exact_value(maximum),
            "mean": exact_value(total / weight),
        }
        for bucket, (total, weight, minimum, maximum) in sorted(buckets.items())
    ]


@dataclass(frozen=True, slots=True)
class ComparisonIdentity:
    dataset_digest: str
    server_snapshot_digest: str
    mode: str
    engine_version: str
    noncompared_settings_digest: str


def compare_values(
    baseline_identity: ComparisonIdentity,
    candidate_identity: ComparisonIdentity,
    baseline: Fraction,
    candidate: Fraction,
) -> dict[str, JsonValue]:
    """Compare an explicit baseline only for compatible completed-run identities."""
    if baseline_identity != candidate_identity:
        raise SimulationError("CONFLICT", "Run inputs, modes, versions or semantic bounds differ")
    return {
        "baseline": exact_value(baseline),
        "candidate": exact_value(candidate),
        "difference": exact_value(candidate - baseline),
        "percentage_difference": exact_value((candidate - baseline) * 100 / baseline)
        if baseline
        else None,
    }
