"""Independently validate a canonical static trace in either logical mode."""

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path

from mslb_contracts.inputs import InputError, read_requests, read_servers
from mslb_engine.models import Mode, Request, RunConfig, Server, SimulationError
from mslb_engine.replay import ReplayError, validate_trace


class Arguments(argparse.Namespace):
    servers: Path
    requests: Path
    run: Path
    mode: Mode
    queue_timeout_ticks: int | None
    max_queue_size: int | None
    max_simulation_ticks: int


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("servers", "requests", "run"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--mode", choices=["concurrent", "sequential_compat"], default="concurrent")
    parser.add_argument("--queue-timeout-ticks", type=int)
    parser.add_argument("--max-queue-size", type=int)
    parser.add_argument("--max-simulation-ticks", type=int, default=100_000)
    args = parser.parse_args(namespace=Arguments())
    with args.servers.open("rb") as stream:
        servers = tuple(
            Server(s.id, s.cpu_units_per_tick, s.mem_mb, s.rate_limit_per_sec)
            for s in read_servers(stream)
        )
    with args.requests.open("rb") as stream:
        requests = tuple(
            Request(r.t, r.request_id, r.work_units, r.mem_mb) for r in read_requests(stream)
        )
    with args.run.open("rb") as stream:
        summary = validate_trace(
            stream,
            servers,
            requests,
            RunConfig(
                mode=args.mode,
                queue_timeout_ticks=args.queue_timeout_ticks,
                max_queue_size=args.max_queue_size,
                max_simulation_ticks=args.max_simulation_ticks,
            ),
        )
    print(json.dumps(asdict(summary), sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (InputError, SimulationError, ReplayError, OSError) as error:
        message = "Unable to read validation input" if isinstance(error, OSError) else str(error)
        print(
            json.dumps({"error": {"code": "VALIDATION_FAILED", "message": message}}),
            file=sys.stderr,
        )
        sys.exit(2)
