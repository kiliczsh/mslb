"""Generate a static logical trace atomically using the real engine."""

import argparse
import json
import os
import tempfile
from contextlib import ExitStack
from pathlib import Path

from mslb_contracts.inputs import read_requests, read_servers
from mslb_engine.metrics import JsonValue, MetricAccumulator
from mslb_engine.models import Mode, Request, RunConfig, Server, SimulationError, Strategy
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation


def generate(
    servers_path: Path,
    requests_path: Path,
    output: Path,
    config: RunConfig,
) -> dict[str, JsonValue]:
    """Publish only a complete trace; discard a failed attempt's temporary file."""
    if config.autoscaling.enabled:
        raise SimulationError(
            code="VALIDATION_ERROR", message="Single-file generation requires autoscaling disabled"
        )
    with servers_path.open(mode="rb") as source:
        servers = tuple(
            Server(
                id=s.id,
                cpu_units_per_tick=s.cpu_units_per_tick,
                mem_mb=s.mem_mb,
                rate_limit_per_sec=s.rate_limit_per_sec,
            )
            for s in read_servers(stream=source)
        )
    with requests_path.open(mode="rb") as source:
        requests = tuple(
            Request(t=r.t, request_id=r.request_id, work_units=r.work_units, mem_mb=r.mem_mb)
            for r in read_requests(stream=source)
        )
    simulation = Simulation(servers=servers, requests=requests, config=config)
    metrics = MetricAccumulator(
        requests=requests, servers=servers, max_series=config.max_metric_series
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as cleanup:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="\n",
            dir=output.parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            cleanup.callback(temporary.unlink, missing_ok=True)
            for frame in simulation.iter_ticks():
                metrics.observe(frame=frame)
                for event in frame.events:
                    stream.write(event.canonical_line())
            stream.flush()
            os.fsync(fd=stream.fileno())
        with temporary.open(mode="rb") as validation_source:
            validate_trace(
                stream=validation_source, servers=servers, requests=requests, config=config
            )
        report = metrics.summary()
        os.replace(src=temporary, dst=output)
        return report


class Arguments(argparse.Namespace):
    """Types guaranteed by argparse converters and explicit choices below."""

    servers: Path
    requests: Path
    output: Path
    mode: Mode
    strategy: Strategy
    max_simulation_ticks: int


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--servers", type=Path, required=True)
    parser.add_argument("--requests", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=["concurrent", "sequential"], default="sequential")
    parser.add_argument(
        "--strategy",
        choices=[
            "first_fit",
            "round_robin",
            "least_connections",
            "most_available_memory",
        ],
        default="first_fit",
    )
    parser.add_argument("--autoscaling", choices=["disabled"], default="disabled")
    parser.add_argument("--max-simulation-ticks", type=int, default=100_000)
    args = parser.parse_args(namespace=Arguments())
    # argparse choices validate these transport strings before constructing the domain config.
    mode: Mode = args.mode
    strategy: Strategy = args.strategy
    report = generate(
        servers_path=args.servers,
        requests_path=args.requests,
        output=args.output,
        config=RunConfig(
            mode=mode,
            strategy=strategy,
            max_simulation_ticks=args.max_simulation_ticks,
        ),
    )
    print(json.dumps(obj=report, sort_keys=True, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
