"""Start one bounded HTTP process; deployment policy is checked separately."""

import os
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from mslb_application.managed_files import ManagedFiles
from mslb_persistence.database import Database

from mslb_api.app import create_app
from mslb_api.settings import Settings, cursor_secret


def build() -> FastAPI:
    return create_app(
        settings=Settings.environment(),
        database=Database(url=os.environ["DATABASE_URL"]),
        inputs=ManagedFiles(root=Path(os.environ.get(key="INPUT_ROOT", default="/data/inputs"))),
        outputs=ManagedFiles(
            root=Path(os.environ.get(key="OUTPUT_ROOT", default="/data/outputs")), read_only=True
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
