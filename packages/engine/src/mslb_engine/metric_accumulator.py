"""Bounded incremental aggregation of deterministic interval observations."""

from fractions import Fraction

from mslb_engine.errors import SimulationError
from mslb_engine.event_models import TickFrame
from mslb_engine.input_models import Request, Server
from mslb_engine.metric_values import JsonValue, ServerTotals, _bounded, distribution, exact_value


class MetricAccumulator:
    """Retain only bounded request times and totals, never interval history."""

    def __init__(
        self, requests: tuple[Request, ...], servers: tuple[Server, ...], max_series: int = 10_000
    ) -> None:
        if not 1 <= len(requests) <= 10_000:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Invalid metric request population"
            )
        self.requests = {request.request_id: request for request in requests}
        if len(self.requests) != len(requests):
            raise SimulationError(code="VALIDATION_ERROR", message="Duplicate metric request IDs")
        self.arrivals: set[str] = set()
        self.starts: dict[str, tuple[int, str]] = {}
        self.finishes: dict[str, int] = {}
        self.drops: dict[str, int] = {}
        if not 1 <= len(servers) <= 64 or len({server.id for server in servers}) != len(servers):
            raise SimulationError(
                code="VALIDATION_ERROR", message="Invalid metric server population"
            )
        if type(max_series) is not int or not len(servers) <= max_series <= 10_000:
            raise SimulationError(code="VALIDATION_ERROR", message="Invalid metric history bound")
        self.max_series = max_series
        self.current_servers = {server.id: server for server in servers}
        self.servers = {
            server.id: ServerTotals(
                cpu_units_per_tick=server.cpu_units_per_tick, mem_mb=server.mem_mb
            )
            for server in sorted(servers, key=lambda server: server.id)
        }
        self.next_tick = 0
        self.terminal = False
        self.queue_peak = 0
        self.queue_integral = 0

    def _server(self, sid: str) -> ServerTotals:
        if sid not in self.servers:
            if len(self.servers) >= self.max_series:
                raise SimulationError(
                    code="RESOURCE_LIMIT_EXCEEDED", message="Metric server history bound exceeded"
                )
            self.servers[sid] = ServerTotals()
        return self.servers[sid]

    def observe(self, frame: TickFrame) -> None:
        """Consume exactly one boundary and its analytically sized interval."""
        if self.terminal or frame.t != self.next_tick or frame.duration_ticks < 0:
            raise SimulationError(
                code="INVALID_STATE", message="Metric boundaries must be contiguous"
            )
        if frame.terminal != (frame.duration_ticks == 0):
            raise SimulationError(
                code="INVALID_STATE", message="Only terminal boundaries have zero duration"
            )
        if frame.membership:
            self.current_servers = {server.id: server for server in frame.membership}
            for server in frame.membership:
                total = self._server(sid=server.id)
                total.cpu_units_per_tick, total.mem_mb = server.cpu_units_per_tick, server.mem_mb
        for event in frame.events:
            rid = event.request_id
            request = self.requests.get(rid)
            if request is None or event.t != frame.t:
                raise SimulationError(
                    code="VALIDATION_ERROR", message="Invalid metric event identity or tick"
                )
            if event.event == "REQUEST_ARRIVED":
                if rid in self.arrivals or event.t != request.t:
                    raise SimulationError(code="VALIDATION_ERROR", message="Invalid metric arrival")
                self.arrivals.add(rid)
            elif event.event == "REQUEST_STARTED":
                if (
                    rid not in self.arrivals
                    or rid in self.starts
                    or rid in self.drops
                    or event.server_id is None
                ):
                    raise SimulationError(code="VALIDATION_ERROR", message="Invalid metric start")
                self.starts[rid] = (event.t, event.server_id)
                total = self._server(sid=event.server_id)
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
                    raise SimulationError(
                        code="VALIDATION_ERROR", message="Invalid metric completion"
                    )
                self.finishes[rid] = event.t
                total = self.servers[start[1]]
                total.finished += 1
                total.responses.append(event.t - request.t)
                total.executions.append(event.t - start[0])
            else:
                if rid not in self.arrivals or rid in self.starts or rid in self.drops:
                    raise SimulationError(code="VALIDATION_ERROR", message="Invalid metric drop")
                self.drops[rid] = event.t
        self.queue_peak = max(self.queue_peak, len(frame.pending_ids))
        self.queue_integral += len(frame.pending_ids) * frame.duration_ticks
        for sample in frame.samples:
            total = self._server(sid=sample.server_id)
            total.cpu_units_per_tick = sample.capacity
            total.mem_mb = sample.memory_capacity
            if (
                sample.reserved_mem_mb < 0
                or sample.consumed_cpu < 0
                or sample.consumed_cpu > sample.capacity
                or sample.reserved_mem_mb > sample.memory_capacity
            ):
                raise SimulationError(
                    code="VALIDATION_ERROR", message="Invalid interval metric sample"
                )
            total.cpu_allocated = _bounded(
                value=total.cpu_allocated + sample.allocated_cpu * frame.duration_ticks
            )
            total.cpu_consumed = _bounded(
                value=total.cpu_consumed + sample.consumed_cpu * frame.duration_ticks
            )
            total.cpu_capacity_ticks += sample.capacity * frame.duration_ticks
            total.memory_reserved_mb_ticks += sample.reserved_mem_mb * frame.duration_ticks
            total.memory_capacity_mb_ticks += sample.memory_capacity * frame.duration_ticks
        self.next_tick = frame.t + frame.duration_ticks
        self.terminal = frame.terminal

    def summary(self) -> dict[str, JsonValue]:
        """Return a complete exact report; unfinished attempts have no final report."""
        if not self.terminal or len(self.finishes) + len(self.drops) != len(self.requests):
            raise SimulationError(
                code="INVALID_STATE", message="Metrics require a terminal accounted run"
            )
        waiting = [start - self.requests[rid].t for rid, (start, _) in self.starts.items()]
        response = [finish - self.requests[rid].t for rid, finish in self.finishes.items()]
        execution = [finish - self.starts[rid][0] for rid, finish in self.finishes.items()]
        makespan = max([*self.finishes.values(), *self.drops.values()]) - min(
            request.t for request in self.requests.values()
        )
        cluster = ServerTotals()
        for total in self.servers.values():
            cluster.waits.extend(total.waits)
            cluster.responses.extend(total.responses)
            cluster.executions.extend(total.executions)
            cluster.started += total.started
            cluster.finished += total.finished
            cluster.cpu_allocated = _bounded(value=cluster.cpu_allocated + total.cpu_allocated)
            cluster.cpu_consumed = _bounded(value=cluster.cpu_consumed + total.cpu_consumed)
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
                value=Fraction(numerator=len(self.finishes), denominator=makespan)
                if makespan
                else Fraction()
            ),
            "drop_fraction": exact_value(
                value=Fraction(numerator=len(self.drops), denominator=len(self.arrivals))
            ),
            "queue_depth_peak": self.queue_peak,
            "queue_depth_integral": self.queue_integral,
            "waiting": distribution(values=waiting).to_data(),
            "response": distribution(values=response).to_data(),
            "execution": distribution(values=execution).to_data(),
            "cluster": cluster.to_data(),
            "servers": {
                sid: {**total.to_data(), "active": sid in self.current_servers}
                for sid, total in sorted(self.servers.items())
            },
        }
