"""Explicit local/remote access policy and persistent cursor signing material."""

import os
import re
import secrets
import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

from mslb_application.errors import OperationError


def valid_origin(origin: str) -> bool:
    try:
        parsed = urlsplit(origin)
    except ValueError:
        return False
    return (
        origin.isascii()
        and all(33 <= ord(character) <= 126 for character in origin)
        and parsed.scheme in {"http", "https"}
        and bool(parsed.netloc)
        and parsed.username is None
        and parsed.password is None
        and origin == f"{parsed.scheme}://{parsed.netloc}"
        and "*" not in origin
        and len(origin) <= 512
    )


@dataclass(frozen=True, slots=True)
class Settings:
    auth_enabled: bool = False
    token: str = field(default="", repr=False)
    allowed_hosts: tuple[str, ...] = ("localhost:8000", "127.0.0.1:8000", "api:8000")
    allowed_origins: tuple[str, ...] = ("http://localhost:8000", "http://127.0.0.1:8000")
    request_limit: int = 32
    stream_limit: int = 20
    mcp_session_idle_seconds: float = 60

    def __post_init__(self) -> None:
        if (
            type(self.auth_enabled) is not bool
            or (self.auth_enabled and re.fullmatch(r"[A-Za-z0-9_-]{32,256}", self.token) is None)
            or not 1 <= self.request_limit <= 32
            or not 1 <= self.stream_limit <= 20
            or not 0.05 <= self.mcp_session_idle_seconds <= 1800
            or not self.allowed_hosts
            or len(self.allowed_hosts) > 16
            or any(
                not host
                or not host.isascii()
                or any(ord(character) < 33 or ord(character) > 126 for character in host)
                or "*" in host
                or "/" in host
                or "@" in host
                or len(host) > 255
                for host in self.allowed_hosts
            )
            or len(self.allowed_origins) > 16
            or any(not valid_origin(origin) for origin in self.allowed_origins)
        ):
            raise OperationError("VALIDATION_ERROR", "Invalid API access policy")

    @classmethod
    def environment(cls) -> "Settings":
        enabled = os.environ.get("AUTH_ENABLED", "false")
        if enabled not in {"true", "false"}:
            raise OperationError("VALIDATION_ERROR", "AUTH_ENABLED must be true or false")
        return cls(
            auth_enabled=enabled == "true",
            token=os.environ.get("AUTH_TOKEN", ""),
            allowed_hosts=tuple(
                os.environ.get("ALLOWED_HOSTS", "localhost:8000,127.0.0.1:8000,api:8000").split(",")
            ),
            allowed_origins=tuple(
                filter(
                    None,
                    os.environ.get(
                        "ALLOWED_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000"
                    ).split(","),
                )
            ),
        )


def cursor_secret(root: Path) -> bytes:
    if sys.platform != "linux":
        raise RuntimeError("API signing-key storage requires Linux containers")
    if root.is_symlink():
        raise OperationError("VALIDATION_FAILED", "Unsafe API state directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / "cursor.key"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, 0o600)
    except FileExistsError:
        descriptor = None
    if descriptor is not None:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(secrets.token_bytes(32))
            stream.flush()
            os.fsync(stream.fileno())
        directory = os.open(root, os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
            raise OperationError("VALIDATION_FAILED", "API signing key permissions are unsafe")
        result = stream.read(257)
    if not 32 <= len(result) <= 256:
        raise OperationError("VALIDATION_FAILED", "API signing key is invalid")
    return result
