"""Cheap private controller heartbeat without importing its database or RPC stack."""

import sys
from pathlib import Path

from mslb_controller.guard_health import healthy as fresh
from mslb_controller.guard_health import write_health as write

HEALTH = Path("/run/mslb-tmp/controller-health.json")


def write_health() -> None:
    """Record a current database-connected controller heartbeat in its private tmpfs."""
    write(path=HEALTH)


def healthy() -> bool:
    """Require a recent owned heartbeat independently of workload admission permission."""
    return fresh(path=HEALTH)


if __name__ == "__main__":
    sys.exit(0 if healthy() else 1)
