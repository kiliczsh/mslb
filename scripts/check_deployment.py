"""Reject unsafe effective Compose bindings without printing configuration/secrets."""

import ipaddress
import sys

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Port(BaseModel):
    host_ip: str | None = None


class Service(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ports: list[Port] = Field(default_factory=list)
    volumes: list["Mount"] = Field(default_factory=list)


class Mount(BaseModel):
    type: str
    source: str = ""
    target: str
    read_only: bool = False


class Compose(BaseModel):
    services: dict[str, Service]


def check(payload: bytes) -> None:
    if len(payload) > 1024**2:
        raise ValueError("Compose configuration exceeds preflight size limit")
    config = Compose.model_validate_json(payload)
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
            if port.host_ip is None or not ipaddress.ip_address(port.host_ip).is_loopback:
                # Remote exposure also needs the planned HTTPS gateway. Until that
                # gateway is implemented, direct API public publishing is rejected
                # even if someone sets an authentication environment flag.
                raise ValueError(
                    "Published services must bind to loopback; "
                    "remote gateway acceptance is incomplete"
                )


if __name__ == "__main__":
    try:
        check(sys.stdin.buffer.read(1024**2 + 1))
    except (ValueError, ValidationError):
        print(
            "Deployment rejected: require loopback published ports "
            "and private database/worker services.",
            file=sys.stderr,
        )
        sys.exit(2)
    print("Effective deployment bindings verified")
