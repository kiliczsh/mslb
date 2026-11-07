"""Reject unsafe effective Compose bindings without printing configuration/secrets."""

import ipaddress
import sys

from mslb_contracts.resources import MIB, SERVICE_LIMITS_MB
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Port(BaseModel):
    """Read only the host binding needed by exposure preflight."""

    host_ip: str | None = None


class Service(BaseModel):
    """Read effective service isolation without retaining environment secrets."""

    model_config = ConfigDict(extra="ignore")
    ports: list[Port] = Field(default_factory=list)
    volumes: list["Mount"] = Field(default_factory=list)
    mem_limit: int | None = None
    memswap_limit: int | None = None
    cpus: float | None = None
    pids_limit: int | None = None
    read_only: bool = False
    privileged: bool = False
    cap_drop: list[str] = Field(default_factory=list)
    cap_add: list[str] = Field(default_factory=list)
    security_opt: list[str] = Field(default_factory=list)
    devices: list[object] = Field(default_factory=list)
    network_mode: str | None = None
    pid: str | None = None
    ipc: str | None = None


class Mount(BaseModel):
    """Describe only ownership-relevant volume fields."""

    type: str
    source: str = ""
    target: str
    read_only: bool = False


class Compose(BaseModel):
    """Select bounded service metadata from the resolved Compose document."""

    services: dict[str, Service]


def check(*, payload: bytes, full_installation: bool = False) -> None:
    """Reject exposed boundaries and optionally require the complete runtime resource contract."""
    if len(payload) > 1024**2:
        raise ValueError("Compose configuration exceeds preflight size limit")
    config = Compose.model_validate_json(json_data=payload)
    if full_installation:
        check_runtime_bounds(config=config)
    for name, service in config.services.items():
        for mount in service.volumes:
            if mount.type == "bind" and not (
                name in {"docker-proxy", "docker-proxy-check"}
                and mount.source == "/var/run/docker.sock"
                and mount.target == "/var/run/docker.sock"
                and mount.read_only
            ):
                raise ValueError("Only the restricted proxy boundary may bind the daemon socket")
            if "controller-auth" in mount.source and (
                name
                not in {
                    "control-init",
                    "container-controller",
                    "docker-proxy",
                    "docker-proxy-check",
                }
                or (name != "control-init" and not mount.read_only)
            ):
                raise ValueError("Controller credential mounts are private and read-only")
            if "guard-auth" in mount.source and (
                name not in {"control-init", "resource-guard", "docker-proxy", "docker-proxy-check"}
                or (name != "control-init" and not mount.read_only)
            ):
                raise ValueError("Guard credential mounts are isolated and read-only")
            if "host-memory" in mount.source and (
                mount.target != "/run/host-memory"
                or name not in {"resource-guard", "host-memory-writer"}
                or mount.read_only != (name == "resource-guard")
            ):
                raise ValueError("Host memory evidence has one isolated writer and read-only guard")
        for port in service.ports:
            if name in {
                "postgres",
                "redis",
                "worker",
                "backend-a",
                "backend-b",
                "container-controller",
                "docker-proxy",
                "docker-proxy-check",
                "control-init",
                "resource-guard",
                "host-memory-writer",
            }:
                raise ValueError("Private data/control/work services must not publish host ports")
            if port.host_ip is None or not ipaddress.ip_address(address=port.host_ip).is_loopback:
                # Remote exposure also needs the planned HTTPS gateway. Until that
                # gateway is implemented, direct API public publishing is rejected
                # even if someone sets an authentication environment flag.
                raise ValueError(
                    "Published services must bind to loopback; "
                    "remote gateway acceptance is incomplete"
                )


def check_runtime_bounds(*, config: Compose) -> None:
    """Require every runtime role and reject unlimited resources or host isolation bypasses."""
    for name, memory_mib in SERVICE_LIMITS_MB.items():
        service = config.services.get(name)
        if service is None:
            raise ValueError("The complete installation requires every runtime role")
        if (
            service.mem_limit != memory_mib * MIB
            or service.memswap_limit != service.mem_limit
            or service.cpus is None
            or not 0 < service.cpus <= 1
            or service.pids_limit is None
            or not 0 < service.pids_limit <= 128
        ):
            raise ValueError("Runtime roles require approved RAM, equal swap and bounded CPU/PIDs")
        if (
            service.privileged
            or service.cap_add
            or service.devices
            or service.network_mode == "host"
            or service.pid == "host"
            or service.ipc == "host"
        ):
            raise ValueError("Runtime roles cannot bypass container isolation")
        # PostgreSQL's upstream entrypoint initializes its named volume before
        # switching to its database user; other roles use immutable root filesystems.
        if name != "postgres" and (
            not service.read_only
            or "ALL" not in service.cap_drop
            or "no-new-privileges:true" not in service.security_opt
        ):
            raise ValueError("Runtime roles require read-only roots and dropped privileges")


if __name__ == "__main__":
    try:
        check(payload=sys.stdin.buffer.read(1024**2 + 1), full_installation=True)
    except (ValueError, ValidationError):
        print(
            "Deployment rejected: require loopback published ports "
            "private database/worker services and bounded isolated runtime roles.",
            file=sys.stderr,
        )
        sys.exit(2)
    print("Effective deployment bindings verified")
