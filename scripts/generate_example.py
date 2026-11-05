"""Generate a static logical trace atomically using the real engine."""

import argparse
import os
import tempfile
from pathlib import Path

from mslb_contracts.inputs import read_requests, read_servers
from mslb_engine.models import Mode, Request, RunConfig, Server, Strategy
from mslb_engine.replay import validate_trace
from mslb_engine.simulator import Simulation


def generate(servers_path: Path, requests_path: Path, output: Path, config: RunConfig) -> None:
    """Publish only a complete trace; discard a failed attempt's temporary file."""
    with servers_path.open("rb") as source:
        servers = tuple(
            Server(s.id, s.cpu_units_per_tick, s.mem_mb, s.rate_limit_per_sec)
            for s in read_servers(source)
        )
    with requests_path.open("rb") as source:
        requests = tuple(
            Request(r.t, r.request_id, r.work_units, r.mem_mb) for r in read_requests(source)
        )
    simulation = Simulation(servers, requests, config)
    output.parent.mkdir(parents=True, exist_ok=True)
    stream = tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        newline="\n",
        dir=output.parent,
        delete=False,
    )
    temporary = Path(stream.name)
    try:
        with stream:
            for frame in simulation.iter_ticks():
                for event in frame.events:
                    stream.write(event.canonical_line())
            stream.flush()
            os.fsync(stream.fileno())
        with temporary.open("rb") as validation_source:
            validate_trace(validation_source, servers, requests, config)
        os.replace(temporary, output)
    finally:
        temporary.unlink(missing_ok=True)


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
    parser.add_argument("--mode", choices=["concurrent", "sequential_compat"], default="concurrent")
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
    generate(
        args.servers,
        args.requests,
        args.output,
        RunConfig(
            mode=mode,
            strategy=strategy,
            max_simulation_ticks=args.max_simulation_ticks,
        ),
    )


if __name__ == "__main__":
    main()
