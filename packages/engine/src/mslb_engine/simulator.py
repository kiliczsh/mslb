"""Discrete-time exact CPU execution; all external I/O belongs to adapters."""

import re
from collections.abc import Iterator
from dataclasses import replace
from fractions import Fraction

from mslb_engine.autoscaling import Autoscaler
from mslb_engine.errors import SimulationError
from mslb_engine.event_models import EVENT_PRIORITY, Event, ServerSample, TickFrame
from mslb_engine.input_models import Request, Server
from mslb_engine.run_config import RunConfig


class Simulation:
    """Stream one bounded tick at a time without retaining a complete trace."""

    def __init__(
        self,
        servers: tuple[Server, ...],
        requests: tuple[Request, ...],
        config: RunConfig,
    ) -> None:
        if not 1 <= len(servers) <= 64 or not 1 <= len(requests) <= 10_000:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Input count outside supported envelope"
            )
        if len(servers) > config.max_metric_series:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Initial cluster exceeds metric history budget"
            )
        if len({s.id for s in servers}) != len(servers):
            raise SimulationError(code="VALIDATION_ERROR", message="Duplicate server IDs")
        if len({r.request_id for r in requests}) != len(requests):
            raise SimulationError(code="VALIDATION_ERROR", message="Duplicate request IDs")
        self.servers = tuple(sorted(servers, key=lambda server: server.id))
        self.requests = tuple(sorted(requests, key=lambda request: (request.t, request.request_id)))
        if self.requests[-1].t >= config.max_simulation_ticks:
            raise SimulationError(
                code="VALIDATION_ERROR", message="Latest arrival must precede the horizon"
            )
        self.initial_servers = self.servers
        policy = config.autoscaling.resolved(initial_count=len(servers))
        if policy.enabled and any(
            re.fullmatch(pattern=r"autoscale-[0-9]{6,}", string=server.id) for server in servers
        ):
            raise SimulationError(
                code="VALIDATION_ERROR",
                message="Initial server ID collides with generated namespace",
            )
        self.config = replace(config, autoscaling=policy)
        self.autoscaler = Autoscaler(
            policy=policy,
            initial_ids=frozenset(s.id for s in servers),
            bit_limit=config.max_fraction_bits,
        )
        self.by_id = {r.request_id: r for r in requests}
        self.active: dict[str, dict[str, Fraction]] = {s.id: {} for s in self.servers}
        self.pending: list[Request] = []
        self.reserved = dict.fromkeys(self.active, 0)
        self.cursor = 0
        self.tick = 0
        self.anchor: str | None = None
        self.recorded_samples = 0
        self.terminal = False
        self.checkpoint_ready = False
        self.executed_intervals = 0
        self.event_count = 0
        self.decision_count = 0
        self.last_start_counters = dict.fromkeys(self.active, 0)

    def _bounded(self, value: Fraction) -> Fraction:
        if (
            max(value.numerator.bit_length(), value.denominator.bit_length())
            > self.config.max_fraction_bits
        ):
            raise SimulationError(
                code="RESOURCE_LIMIT_EXCEEDED", message="Exact arithmetic complexity exceeded"
            )
        return value

    def _select(self, eligible: list[Server]) -> Server | None:
        if not eligible:
            return None
        strategy = self.config.strategy
        if strategy == "round_robin" and self.anchor is not None:
            return next((s for s in eligible if s.id > self.anchor), eligible[0])
        if strategy == "least_connections":
            return min(eligible, key=lambda s: (len(self.active[s.id]), s.id))
        if strategy == "most_available_memory":
            return min(eligible, key=lambda s: (-(s.mem_mb - self.reserved[s.id]), s.id))
        return eligible[0]

    def _schedule(self, events: list[Event], starts: dict[str, int]) -> None:
        """Assign stable pending order while enforcing memory, CPU and start eligibility."""
        waiting: list[Request] = []
        for request in self.pending:
            eligible = [
                s
                for s in self.servers
                if request.mem_mb <= s.mem_mb - self.reserved[s.id]
                and starts[s.id] < s.rate_limit_per_sec
                and (self.config.mode == "concurrent" or not self.active[s.id])
            ]
            server = self._select(eligible=eligible)
            if server is None:
                waiting.append(request)
                continue
            self.active[server.id][request.request_id] = Fraction(numerator=request.work_units)
            self.reserved[server.id] += request.mem_mb
            starts[server.id] += 1
            if self.config.strategy == "round_robin":
                self.anchor = server.id
            events.append(
                Event(
                    t=self.tick,
                    event="REQUEST_STARTED",
                    request_id=request.request_id,
                    server_id=server.id,
                )
            )
        # Eligibility only decreases during this phase, so a second stable scan
        # cannot unblock a survivor. One scan is the work-conserving fixed point.
        self.pending = waiting

    def _boundary(self) -> tuple[list[Event], dict[str, int]]:
        """Process finishes, arrivals, expiry, starts and CPU in deterministic order."""
        events: list[Event] = []
        starts = dict.fromkeys(self.active, 0)
        for server in self.servers:
            for rid in sorted(self.active[server.id]):
                if self.active[server.id][rid] == 0:
                    del self.active[server.id][rid]
                    self.reserved[server.id] -= self.by_id[rid].mem_mb
                    events.append(
                        Event(
                            t=self.tick,
                            event="REQUEST_FINISHED",
                            request_id=rid,
                            server_id=server.id,
                        )
                    )
        while self.cursor < len(self.requests) and self.requests[self.cursor].t == self.tick:
            request = self.requests[self.cursor]
            self.pending.append(request)
            events.append(
                Event(t=self.tick, event="REQUEST_ARRIVED", request_id=request.request_id)
            )
            self.cursor += 1
        waiting: list[Request] = []
        max_memory = max(s.mem_mb for s in self.initial_servers)
        policy = self.config.autoscaling
        if (
            policy.enabled
            and policy.max_servers is not None
            and policy.max_servers > len(self.initial_servers)
            and policy.server_template is not None
        ):
            max_memory = max(max_memory, policy.server_template.mem_mb)
        for request in self.pending:
            reason = None
            if request.mem_mb > max_memory:
                reason = "UNSCHEDULABLE_MEMORY"
            elif self.config.queue_timeout_ticks is not None and (
                self.tick - request.t >= self.config.queue_timeout_ticks
            ):
                reason = "QUEUE_TIMEOUT"
            if reason is None:
                waiting.append(request)
            else:
                events.append(
                    Event(
                        t=self.tick,
                        event="REQUEST_DROPPED",
                        request_id=request.request_id,
                        reason=reason,
                    )
                )
        self.pending = waiting
        self._schedule(events=events, starts=starts)
        limit = self.config.max_queue_size
        if limit is not None:
            for request in self.pending[limit:]:
                events.append(
                    Event(
                        t=self.tick,
                        event="REQUEST_DROPPED",
                        request_id=request.request_id,
                        reason="QUEUE_CAPACITY",
                    )
                )
            del self.pending[limit:]
        return events, starts

    def iter_ticks(self) -> Iterator[TickFrame]:
        """Advance execution, emitting canonical boundaries and interval samples."""
        while not self.terminal:
            self.checkpoint_ready = False
            membership = self.autoscaler.activate(
                tick=self.tick,
                servers=self.servers,
                idle_ids=frozenset(sid for sid, active in self.active.items() if not active),
            )
            current_ids = {server.id for server in membership}
            for sid in set(self.active) - current_ids:
                del self.active[sid]
                del self.reserved[sid]
            for sid in current_ids - set(self.active):
                self.active[sid], self.reserved[sid] = {}, 0
            self.servers = membership
            events, starts = self._boundary()
            ordered = tuple(
                sorted(
                    events,
                    key=lambda event: (
                        EVENT_PRIORITY[event.event],
                        event.request_id,
                        event.server_id or "",
                    ),
                )
            )
            active_count = sum(len(active) for active in self.active.values())
            if self.cursor == len(self.requests) and not self.pending and active_count == 0:
                self.terminal = True
                self.event_count += len(ordered)
                yield TickFrame(
                    t=self.tick,
                    duration_ticks=0,
                    events=ordered,
                    pending_ids=(),
                    samples=(),
                    terminal=True,
                    membership=self.servers,
                )
                return
            if self.tick >= self.config.max_simulation_ticks:
                raise SimulationError(
                    code="SIMULATION_LIMIT_EXCEEDED", message="Simulation horizon reached"
                )
            self.recorded_samples += len(self.servers)
            if self.recorded_samples > self.config.max_samples:
                raise SimulationError(
                    code="RESOURCE_LIMIT_EXCEEDED", message="Recorded sample bound reached"
                )
            duration = 1
            if not active_count and not self.pending and not self.config.autoscaling.enabled:
                duration = self.requests[self.cursor].t - self.tick
            decision = self.autoscaler.decide(
                tick=self.tick,
                servers=self.servers,
                idle_ids=frozenset(sid for sid, active in self.active.items() if not active),
                queue_depth=len(self.pending),
            )
            if self.autoscaler.counter + len(self.initial_servers) > self.config.max_metric_series:
                raise SimulationError(
                    code="RESOURCE_LIMIT_EXCEEDED", message="Logical server history budget exceeded"
                )
            samples: list[ServerSample] = []
            cluster_consumed = Fraction()
            for server in self.servers:
                active = self.active[server.id]
                consumed = Fraction(numerator=0)
                if active:
                    share = self._bounded(
                        value=Fraction(numerator=server.cpu_units_per_tick, denominator=len(active))
                    )
                    for rid in sorted(active):
                        removed = min(active[rid], share)
                        consumed = self._bounded(value=consumed + removed)
                        active[rid] = self._bounded(value=active[rid] - removed)
                cluster_consumed = self._bounded(value=cluster_consumed + consumed)
                samples.append(
                    ServerSample(
                        server_id=server.id,
                        capacity=server.cpu_units_per_tick,
                        memory_capacity=server.mem_mb,
                        active_ids=tuple(sorted(active)),
                        reserved_mem_mb=self.reserved[server.id],
                        allocated_cpu=Fraction(
                            numerator=server.cpu_units_per_tick if active else 0
                        ),
                        consumed_cpu=consumed,
                        starts=starts[server.id],
                    )
                )
            frame = TickFrame(
                t=self.tick,
                duration_ticks=duration,
                events=ordered,
                pending_ids=tuple(r.request_id for r in self.pending),
                samples=tuple(samples),
                membership=self.servers,
                scale_decision=decision,
            )
            self.autoscaler.observe(
                ending_tick=self.tick + duration,
                consumed=cluster_consumed,
                capacity=sum(server.cpu_units_per_tick for server in self.servers),
                drops=sum(event.event == "REQUEST_DROPPED" for event in events),
                arrivals=sum(event.event == "REQUEST_ARRIVED" for event in events),
            )
            self.tick += duration
            self.executed_intervals += 1
            self.event_count += len(ordered)
            self.decision_count += decision is not None
            self.last_start_counters = starts
            self.checkpoint_ready = True
            yield frame
