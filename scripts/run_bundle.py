"""Execute or independently verify a complete immutable local run bundle."""

import argparse
import json
import sys
from pathlib import Path
from typing import Literal

from mslb_application.artifacts import execute_bundle, verify_bundle
from mslb_application.errors import OperationError
from mslb_application.snapshots import make_snapshot
from mslb_contracts.inputs import InputError, read_requests, read_servers
from mslb_contracts.runs import RunSettings
from mslb_engine.models import SimulationError
from mslb_engine.replay import ReplayError
from pydantic import ValidationError


class Arguments(argparse.Namespace):
    command: Literal["run", "verify"]
    servers: Path
    requests: Path
    config: Path | None
    output: Path
    directory: Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser(name="run")
    for name in ("servers", "requests", "output"):
        run.add_argument("--" + name, type=Path, required=True)
    run.add_argument("--config", type=Path)
    verify = commands.add_parser(name="verify")
    verify.add_argument("directory", type=Path)
    args = parser.parse_args(namespace=Arguments())
    if args.command == "verify":
        manifest = verify_bundle(directory=args.directory)
    else:
        settings = RunSettings()
        if args.config is not None:
            with args.config.open(mode="rb") as stream:
                payload = stream.read(256 * 1024 + 1)
            if len(payload) > 256 * 1024:
                raise InputError(field="config", reason="payload_too_large")
            settings = RunSettings.model_validate_json(json_data=payload)
        with args.servers.open(mode="rb") as stream:
            servers = read_servers(stream=stream)
        with args.requests.open(mode="rb") as stream:
            requests = read_requests(stream=stream)
        snapshot = make_snapshot(servers=servers, requests=requests, settings=settings)
        manifest = execute_bundle(snapshot=snapshot, destination=args.output)
    print(manifest.model_dump_json())


if __name__ == "__main__":
    try:
        main()
    except (
        OperationError,
        InputError,
        SimulationError,
        ReplayError,
        ValidationError,
        OSError,
    ) as error:
        code = "VALIDATION_FAILED"
        message = "Invalid run configuration or artifact data"
        if isinstance(error, (OperationError, SimulationError)):
            code, message = error.code, str(object=error)
        elif isinstance(error, InputError):
            code, message = "VALIDATION_ERROR", str(object=error)
        elif isinstance(error, OSError):
            code, message = "IO_ERROR", "Unable to access the requested local files"
        print(json.dumps(obj={"error": {"code": code, "message": message}}), file=sys.stderr)
        sys.exit(2)
