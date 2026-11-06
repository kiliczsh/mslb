"""Generate/check deterministic local and authenticated OpenAPI documents in Docker."""

import argparse
import asyncio
import secrets
import tempfile
from pathlib import Path

from mslb_api.app import create_app
from mslb_api.settings import Settings
from mslb_application.managed_files import ManagedFiles
from mslb_contracts.canonical import canonical_bytes
from mslb_persistence.database import Database

SCHEMAS = Path("docs/contracts/openapi")
MODES = {"local.json": False, "authenticated.json": True}


def document(*, auth_enabled: bool) -> dict[str, object]:
    """Build metadata without lifespan startup, database connections or real credentials."""
    database = Database(url="postgresql://schema_only@127.0.0.1/schema_only")
    try:
        with tempfile.TemporaryDirectory(prefix="mslb-openapi-") as name:
            files = ManagedFiles(root=Path(name))
            app = create_app(
                settings=Settings(
                    auth_enabled=auth_enabled, token=secrets.token_urlsafe(nbytes=32)
                ),
                database=database,
                inputs=files,
                outputs=files,
                signing_key=secrets.token_bytes(nbytes=32),
            )
            return dict[str, object](app.openapi())
    finally:
        asyncio.run(main=database.close())


def main() -> None:
    """Generate owned snapshots or reject inventory/content drift."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    arguments = parser.parse_args()
    if SCHEMAS.is_symlink():
        raise SystemExit("OpenAPI schema directory must not be a symlink")
    if arguments.command == "generate":
        SCHEMAS.mkdir(parents=True, exist_ok=True)
    if not SCHEMAS.is_dir() or (
        arguments.command == "check" and {path.name for path in SCHEMAS.iterdir()} != set(MODES)
    ):
        raise SystemExit("OpenAPI schema inventory differs; regenerate owned documents")
    for filename, enabled in MODES.items():
        expected = canonical_bytes(value=document(auth_enabled=enabled))
        path = SCHEMAS / filename
        if path.is_symlink():
            raise SystemExit(f"OpenAPI schema path must not be a symlink: {filename}")
        if arguments.command == "generate":
            path.write_bytes(data=expected)
        elif not path.is_file() or path.read_bytes() != expected:
            raise SystemExit(f"OpenAPI schema drift: {filename}")
    print(f"OpenAPI contract schemas {arguments.command} passed")


if __name__ == "__main__":
    main()
