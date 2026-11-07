"""Explicit local/remote access policy and persistent cursor signing material."""

import os
import re
import secrets
import sys
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit
from uuid import UUID

from mslb_application.errors import OperationError
from mslb_contracts.capacity_profiles import (
    CapacityProfile,
    parse_capacity_profile,
    validate_isolated_profile,
)


def valid_origin(origin: str) -> bool:
    """Accept an exact HTTP origin without credentials, paths, wildcards or control characters."""
    try:
        parsed = urlsplit(url=origin)
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
    """Validate explicit API authentication, origins, hosts and admission/session bounds."""

    auth_enabled: bool = False
    token: str = field(default="", repr=False)
    allowed_hosts: tuple[str, ...] = ("localhost:8000", "127.0.0.1:8000", "api:8000")
    allowed_origins: tuple[str, ...] = ("http://localhost:8000", "http://127.0.0.1:8000")
    request_limit: int = 32
    stream_limit: int = 20
    mcp_session_idle_seconds: float = 60
    capacity_profile: CapacityProfile = "standard"
    isolation_id: UUID | None = None

    def __post_init__(self) -> None:
        """Reject inconsistent service access or execution bounds at construction."""
        validate_isolated_profile(profile=self.capacity_profile, installation_id=self.isolation_id)
        if (
            type(self.auth_enabled) is not bool
            or (
                self.auth_enabled
                and re.fullmatch(pattern=r"[A-Za-z0-9_-]{32,256}", string=self.token) is None
            )
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
            or any(not valid_origin(origin=origin) for origin in self.allowed_origins)
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid API access policy")

    @classmethod
    def environment(cls) -> "Settings":
        """Load and validate the service's explicit environment policy."""
        enabled = os.environ.get(key="AUTH_ENABLED", default="false")
        if enabled not in {"true", "false"}:
            raise OperationError(
                code="VALIDATION_ERROR", message="AUTH_ENABLED must be true or false"
            )
        return cls(
            auth_enabled=enabled == "true",
            token=os.environ.get(key="AUTH_TOKEN", default=""),
            capacity_profile=parse_capacity_profile(
                value=os.environ.get(key="CAPACITY_PROFILE", default="standard")
            ),
            isolation_id=UUID(hex=os.environ["SCENARIO_INSTALLATION_ID"])
            if os.environ.get(key="SCENARIO_INSTALLATION_ID") is not None
            else None,
            allowed_hosts=tuple(
                os.environ.get(
                    key="ALLOWED_HOSTS", default="localhost:8000,127.0.0.1:8000,api:8000"
                ).split(sep=",")
            ),
            allowed_origins=tuple(
                filter(
                    None,
                    os.environ.get(
                        key="ALLOWED_ORIGINS", default="http://localhost:8000,http://127.0.0.1:8000"
                    ).split(sep=","),
                )
            ),
        )


def cursor_secret(root: Path) -> bytes:
    """Create or read private persistent signing bytes with Linux ownership checks."""
    if sys.platform != "linux":
        raise RuntimeError("API signing-key storage requires Linux containers")
    if root.is_symlink():
        raise OperationError(code="VALIDATION_FAILED", message="Unsafe API state directory")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    path = root / "cursor.key"
    try:
        descriptor = os.open(
            path=path, flags=os.O_CREAT | os.O_EXCL | os.O_WRONLY | os.O_NOFOLLOW, mode=0o600
        )
    except FileExistsError:
        descriptor = None
    if descriptor is not None:
        with os.fdopen(fd=descriptor, mode="wb") as stream:
            stream.write(secrets.token_bytes(nbytes=32))
            stream.flush()
            os.fsync(fd=stream.fileno())
        directory = os.open(path=root, flags=os.O_DIRECTORY)
        try:
            os.fsync(fd=directory)
        finally:
            os.close(fd=directory)
    descriptor = os.open(path=path, flags=os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd=descriptor, mode="rb") as stream:
        metadata = os.fstat(fd=stream.fileno())
        if metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
            raise OperationError(
                code="VALIDATION_FAILED", message="API signing key permissions are unsafe"
            )
        result = stream.read(257)
    if not 32 <= len(result) <= 256:
        raise OperationError(code="VALIDATION_FAILED", message="API signing key is invalid")
    return result
