"""Effective installation preflight rejects unbounded or privileged runtime roles."""

import json

import pytest
from mslb_contracts.resources import MIB, SERVICE_LIMITS_MB

from scripts.check_deployment import check


def installation() -> dict[str, dict[str, dict[str, object]]]:
    return {
        "services": {
            name: {
                "mem_limit": memory_mib * MIB,
                "memswap_limit": memory_mib * MIB,
                "cpus": 1,
                "pids_limit": 128,
                "read_only": True,
                "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
            }
            for name, memory_mib in SERVICE_LIMITS_MB.items()
        }
    }


@pytest.mark.parametrize(
    argnames="field,value",
    argvalues=[
        ("mem_limit", None),
        ("memswap_limit", 2**40),
        ("cpus", 0),
        ("pids_limit", -1),
        ("pids_limit", 1024),
        ("read_only", False),
        ("cap_drop", []),
        ("security_opt", []),
        ("privileged", True),
        ("cap_add", ["SYS_ADMIN"]),
        ("devices", ["/dev/mem"]),
        ("network_mode", "host"),
        ("pid", "host"),
        ("ipc", "host"),
    ],
)
def test_each_runtime_resource_or_isolation_regression_is_rejected(
    field: str, value: object
) -> None:
    config = installation()
    check(payload=json.dumps(obj=config).encode(), full_installation=True)
    config["services"]["worker"][field] = value
    with pytest.raises(expected_exception=ValueError):
        check(payload=json.dumps(obj=config).encode(), full_installation=True)


def test_missing_runtime_role_cannot_bypass_the_complete_preflight() -> None:
    config = installation()
    del config["services"]["worker"]
    with pytest.raises(expected_exception=ValueError, match="every runtime role"):
        check(payload=json.dumps(obj=config).encode(), full_installation=True)
