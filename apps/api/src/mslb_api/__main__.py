"""Start one bounded HTTP process; deployment policy is checked separately."""

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from mslb_application.files import ManagedFiles
from mslb_persistence.database import Database

from mslb_api.app import create_app
from mslb_api.settings import Settings, cursor_secret


def build() -> FastAPI:
    return create_app(
        Settings.environment(),
        Database(os.environ["DATABASE_URL"]),
        ManagedFiles(Path(os.environ.get("INPUT_ROOT", "/data/inputs"))),
        ManagedFiles(Path(os.environ.get("OUTPUT_ROOT", "/data/outputs")), read_only=True),
        cursor_secret(Path(os.environ.get("API_STATE_ROOT", "/data/api-state"))),
        close_database=True,
    )


if __name__ == "__main__":
    uvicorn.run(
        "mslb_api.__main__:build",
        factory=True,
        host=os.environ.get("API_BIND_HOST", "127.0.0.1"),
        port=8000,
        workers=1,
        proxy_headers=False,
        access_log=False,
        limit_concurrency=40,
        timeout_keep_alive=5,
        backlog=64,
        h11_max_incomplete_event_size=16384,
    )
