"""Reject unsafe effective Compose bindings without printing configuration/secrets."""

import ipaddress
import sys

from pydantic import BaseModel, ConfigDict, Field, ValidationError


class Port(BaseModel):
    host_ip: str | None = None


class Service(BaseModel):
    model_config = ConfigDict(extra="ignore")
    ports: list[Port] = Field(default_factory=list)


class Compose(BaseModel):
    services: dict[str, Service]


def check(payload: bytes) -> None:
    if len(payload) > 1024**2:
        raise ValueError("Compose configuration exceeds preflight size limit")
    config = Compose.model_validate_json(payload)
    for name, service in config.services.items():
        for port in service.ports:
            if name in {"postgres", "redis", "worker"}:
                raise ValueError("Private data/worker services must not publish host ports")
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
