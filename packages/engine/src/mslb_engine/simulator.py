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
            raise SimulationError("VALIDATION_ERROR", "Input count outside supported envelope")
        if len(servers) > config.max_metric_series:
            raise SimulationError(
                "VALIDATION_ERROR", "Initial cluster exceeds metric history budget"
            )
        if len({s.id for s in servers}) != len(servers):
            raise SimulationError("VALIDATION_ERROR", "Duplicate server IDs")
        if len({r.request_id for r in requests}) != len(requests):
            raise SimulationError("VALIDATION_ERROR", "Duplicate request IDs")
        self.servers = tuple(sorted(servers, key=lambda server: server.id))
        self.requests = tuple(sorted(requests, key=lambda request: (request.t, request.request_id)))
        if self.requests[-1].t >= config.max_simulation_ticks:
            raise SimulationError("VALIDATION_ERROR", "Latest arrival must precede the horizon")
        self.initial_servers = self.servers
        policy = config.autoscaling.resolved(len(servers))
        if policy.enabled and any(
            re.fullmatch(r"autoscale-[0-9]{6,}", server.id) for server in servers
        ):
            raise SimulationError(
                "VALIDATION_ERROR", "Initial server ID collides with generated namespace"
            )
        self.config = replace(config, autoscaling=policy)
        self.autoscaler = Autoscaler(
            policy, frozenset(s.id for s in servers), config.max_fraction_bits
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
            raise SimulationError("RESOURCE_LIMIT_EXCEEDED", "Exact arithmetic complexity exceeded")
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
            server = self._select(eligible)
            if server is None:
                waiting.append(request)
                continue
            self.active[server.id][request.request_id] = Fraction(request.work_units)
            self.reserved[server.id] += request.mem_mb
            starts[server.id] += 1
            if self.config.strategy == "round_robin":
                self.anchor = server.id
            events.append(Event(self.tick, "REQUEST_STARTED", request.request_id, server.id))
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
                    events.append(Event(self.tick, "REQUEST_FINISHED", rid, server.id))
        while self.cursor < len(self.requests) and self.requests[self.cursor].t == self.tick:
            request = self.requests[self.cursor]
            self.pending.append(request)
            events.append(Event(self.tick, "REQUEST_ARRIVED", request.request_id))
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
                    Event(self.tick, "REQUEST_DROPPED", request.request_id, reason=reason)
                )
        self.pending = waiting
        self._schedule(events, starts)
        limit = self.config.max_queue_size
        if limit is not None:
            for request in self.pending[limit:]:
                events.append(
                    Event(
                        self.tick,
                        "REQUEST_DROPPED",
                        request.request_id,
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
                self.tick,
                self.servers,
                frozenset(sid for sid, active in self.active.items() if not active),
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
                    self.tick, 0, ordered, (), (), terminal=True, membership=self.servers
                )
                return
            if self.tick >= self.config.max_simulation_ticks:
                raise SimulationError("SIMULATION_LIMIT_EXCEEDED", "Simulation horizon reached")
            self.recorded_samples += len(self.servers)
            if self.recorded_samples > self.config.max_samples:
                raise SimulationError("RESOURCE_LIMIT_EXCEEDED", "Recorded sample bound reached")
            duration = 1
            if not active_count and not self.pending and not self.config.autoscaling.enabled:
                duration = self.requests[self.cursor].t - self.tick
            decision = self.autoscaler.decide(
                self.tick,
                self.servers,
                frozenset(sid for sid, active in self.active.items() if not active),
                len(self.pending),
            )
            if self.autoscaler.counter + len(self.initial_servers) > self.config.max_metric_series:
                raise SimulationError(
                    "RESOURCE_LIMIT_EXCEEDED", "Logical server history budget exceeded"
                )
            samples: list[ServerSample] = []
            cluster_consumed = Fraction()
            for server in self.servers:
                active = self.active[server.id]
                consumed = Fraction(0)
                if active:
                    share = self._bounded(Fraction(server.cpu_units_per_tick, len(active)))
                    for rid in sorted(active):
                        removed = min(active[rid], share)
                        consumed = self._bounded(consumed + removed)
                        active[rid] = self._bounded(active[rid] - removed)
                cluster_consumed = self._bounded(cluster_consumed + consumed)
                samples.append(
                    ServerSample(
                        server.id,
                        server.cpu_units_per_tick,
                        server.mem_mb,
                        tuple(sorted(active)),
                        self.reserved[server.id],
                        Fraction(server.cpu_units_per_tick if active else 0),
                        consumed,
                        starts[server.id],
                    )
                )
            frame = TickFrame(
                self.tick,
                duration,
                ordered,
                tuple(r.request_id for r in self.pending),
                tuple(samples),
                membership=self.servers,
                scale_decision=decision,
            )
            self.autoscaler.observe(
                self.tick + duration,
                cluster_consumed,
                sum(server.cpu_units_per_tick for server in self.servers),
                sum(event.event == "REQUEST_DROPPED" for event in events),
                sum(event.event == "REQUEST_ARRIVED" for event in events),
            )
            self.tick += duration
            self.executed_intervals += 1
            self.event_count += len(ordered)
            self.decision_count += decision is not None
            self.last_start_counters = starts
            self.checkpoint_ready = True
            yield frame
