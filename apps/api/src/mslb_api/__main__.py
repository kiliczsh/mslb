"""Start one bounded HTTP process; deployment policy is checked separately."""

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from mslb_application.managed_files import ManagedFiles
from mslb_application.storage_values import StorageLimits
from mslb_contracts.resources import parse_resource_guard
from mslb_persistence.database import Database

from mslb_api.app import create_app
from mslb_api.settings import Settings, cursor_secret


def build() -> FastAPI:
    """Compose the API from validated environment, durable storage and private signing material."""
    settings = Settings.environment()
    limits = StorageLimits(capacity_profile=settings.capacity_profile)
    return create_app(
        settings=settings,
        database=Database(
            url=os.environ["DATABASE_URL"],
            capacity_profile=settings.capacity_profile,
            enforce_resource_guard=parse_resource_guard(
                value=os.environ.get(key="RESOURCE_GUARD_ENABLED", default="true")
            ),
        ),
        inputs=ManagedFiles(
            root=Path(os.environ.get(key="INPUT_ROOT", default="/data/inputs")), limits=limits
        ),
        outputs=ManagedFiles(
            root=Path(os.environ.get(key="OUTPUT_ROOT", default="/data/outputs")),
            limits=limits,
            read_only=True,
        ),
        signing_key=cursor_secret(
            root=Path(os.environ.get(key="API_STATE_ROOT", default="/data/api-state"))
        ),
        close_database=True,
    )


if __name__ == "__main__":
    uvicorn.run(
        app="mslb_api.__main__:build",
        factory=True,
        host=os.environ.get(key="API_BIND_HOST", default="127.0.0.1"),
        port=8000,
        workers=1,
        proxy_headers=False,
        access_log=False,
        limit_concurrency=40,
        timeout_keep_alive=5,
        backlog=64,
        h11_max_incomplete_event_size=16384,
    )
