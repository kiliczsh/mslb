"""Bounded fresh-process determinism probe; no files or operational metadata."""

import argparse
import io
import sys
from fractions import Fraction
from itertools import product

from mslb_contracts.canonical import canonical_bytes
from mslb_engine.metrics import MetricAccumulator
from mslb_engine.models import (
    AutoscalingPolicy,
    Mode,
    Request,
    RunConfig,
    Server,
    ServerTemplate,
    Strategy,
)
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation


def workload(*, name: str) -> tuple[tuple[Server, ...], tuple[Request, ...], AutoscalingPolicy]:
    if name == "static":
        return (
            (
                Server(id="a", cpu_units_per_tick=7, mem_mb=20, rate_limit_per_sec=3),
                Server(id="z", cpu_units_per_tick=11, mem_mb=32, rate_limit_per_sec=2),
            ),
            tuple(
                Request(t=tick, request_id=rid, work_units=work, mem_mb=memory)
                for tick, rid, work, memory in (
                    (0, "a", 13, 4),
                    (0, "b", 2, 6),
                    (1, "c", 19, 7),
                    (3, "d", 1, 4),
                    (11, "late", 5, 1),
                )
            ),
            AutoscalingPolicy(),
        )
    servers = (Server(id="s1", cpu_units_per_tick=2, mem_mb=2, rate_limit_per_sec=1),)
    requests = tuple(
        Request(t=0, request_id=f"r{index}", work_units=6, mem_mb=1) for index in range(5)
    )
    if name == "future_template":
        requests = (
            Request(t=0, request_id="template-only", work_units=9, mem_mb=4),
            Request(t=0, request_id="impossible", work_units=1, mem_mb=8),
            Request(t=1, request_id="small", work_units=7, mem_mb=1),
        )
    requests += (Request(t=30, request_id="late", work_units=1, mem_mb=1),)
    return (
        servers,
        requests,
        AutoscalingPolicy(
            enabled=True,
            window_ticks=2,
            cooldown_ticks=1,
            max_servers=3,
            scale_out_cpu_threshold=Fraction(numerator=1),
            scale_out_drop_rate_threshold=Fraction(numerator=1),
            server_template=ServerTemplate(cpu_units_per_tick=3, mem_mb=4, rate_limit_per_sec=2),
        ),
    )


def outputs(*, reverse_inputs: bool) -> dict[str, dict[str, str]]:
    modes: tuple[Mode, ...] = ("sequential", "concurrent")
    strategies: tuple[Strategy, ...] = (
        "first_fit",
        "round_robin",
        "least_connections",
        "most_available_memory",
    )
    cases = list(product(modes, strategies, ("static", "queue_growth", "future_template")))
    if reverse_inputs:
        cases.reverse()
    result: dict[str, dict[str, str]] = {}
    for mode, strategy, name in cases:
        servers, requests, scaling = workload(name=name)
        if reverse_inputs:
            servers, requests = tuple(reversed(servers)), tuple(reversed(requests))
        simulation = Simulation(
            servers=servers,
            requests=requests,
            config=RunConfig(
                mode=mode,
                strategy=strategy,
                autoscaling=scaling,
                max_simulation_ticks=100,
                max_samples=400,
            ),
        )
        metrics = MetricAccumulator(requests=requests, servers=servers)
        trace: list[str] = []
        decisions: list[str] = []
        for frame in simulation.iter_ticks():
            metrics.observe(frame=frame)
            trace.extend(event.canonical_line() for event in frame.events)
            if frame.scale_decision is not None:
                decisions.append(frame.scale_decision.canonical_line())
        trace_text, decision_text = "".join(trace), "".join(decisions)
        validate_trace(
            stream=io.BytesIO(initial_bytes=trace_text.encode(encoding="utf-8")),
            servers=servers,
            requests=requests,
            config=simulation.config,
            scale_stream=io.BytesIO(initial_bytes=decision_text.encode(encoding="utf-8"))
            if scaling.enabled
            else None,
        )
        result[f"{mode}/{strategy}/{name}"] = {
            "trace": trace_text,
            "decisions": decision_text,
            "metrics": canonical_bytes(value=metrics.summary()).decode(encoding="utf-8"),
        }
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reverse-inputs", action="store_true")
    args = parser.parse_args()
    sys.stdout.buffer.write(canonical_bytes(value=outputs(reverse_inputs=args.reverse_inputs)))
