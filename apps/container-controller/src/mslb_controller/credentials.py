"""Runtime-only isolated controller/guard credentials in private project volumes."""

import argparse
import os
import re
import secrets
import stat
import sys
from pathlib import Path
from typing import Literal

type Role = Literal["controller", "guard"]


def root(*, role: Role) -> Path:
    """Locate the configured private directory for controller and resource-guard credentials."""
    if role not in {"controller", "guard"}:
        raise ValueError("Unknown private credential role")
    return Path("/run/controller-auth" if role == "controller" else "/run/guard-auth")


def read_token(*, role: Role = "controller") -> str:
    """Read a bounded private Linux credential and reject unsafe ownership or permissions."""
    if sys.platform != "linux":
        raise RuntimeError("Private credential storage requires Linux containers")
    descriptor = os.open(path=root(role=role) / "token", flags=os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd=descriptor, mode="r", encoding="ascii") as stream:
        info = os.fstat(fd=stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 1000
            or info.st_gid != 1000
            or stat.S_IMODE(info.st_mode) != 0o440
            or info.st_nlink != 1
        ):
            raise ValueError("Controller credential permissions are invalid")
        token = stream.read(65)
    if not re.fullmatch(pattern=r"[0-9a-f]{64}", string=token):
        raise ValueError("Controller credential is invalid")
    return token


def initialize(*, role: Role = "controller") -> None:
    """Create missing private controller credentials without replacing existing secrets."""
    if sys.platform != "linux":
        raise RuntimeError("Private credential storage requires Linux containers")
    directory = root(role=role)
    directory.chmod(mode=0o750)
    try:
        descriptor = os.open(
            path=directory / "token",
            flags=os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            mode=0o440,
        )
    except FileExistsError:
        read_token(role=role)
        return
    with os.fdopen(fd=descriptor, mode="w", encoding="ascii") as stream:
        stream.write(secrets.token_hex(nbytes=32))
        stream.flush()
        os.fsync(fd=stream.fileno())
    read_token(role=role)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Initialize isolated private credentials")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("role", choices=("controller", "guard"), nargs="?", default="controller")
    arguments = parser.parse_args()
    selected: Role = "guard" if arguments.role == "guard" else "controller"
    if arguments.all:
        initialize(role="controller")
        initialize(role="guard")
    else:
        initialize(role=selected)
