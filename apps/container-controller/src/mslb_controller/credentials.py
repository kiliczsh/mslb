"""Runtime-only controller credential shared through a private project volume."""

import os
import re
import secrets
import stat
from pathlib import Path

ROOT = Path("/run/controller-auth")
TOKEN = ROOT / "token"


def read_token() -> str:
    descriptor = os.open(TOKEN, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "r", encoding="ascii") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_uid != 1000
            or info.st_gid != 1000
            or stat.S_IMODE(info.st_mode) != 0o440
            or info.st_nlink != 1
        ):
            raise ValueError("Controller credential permissions are invalid")
        token = stream.read(65)
    if not re.fullmatch(r"[0-9a-f]{64}", token):
        raise ValueError("Controller credential is invalid")
    return token


def initialize() -> None:
    ROOT.chmod(0o750)
    try:
        descriptor = os.open(TOKEN, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o440)
    except FileExistsError:
        read_token()
        return
    with os.fdopen(descriptor, "w", encoding="ascii") as stream:
        stream.write(secrets.token_hex(32))
        stream.flush()
        os.fsync(stream.fileno())
    read_token()


if __name__ == "__main__":
    initialize()
