"""One private proxy process owns the raw socket; other services use semantic RPC."""

import os

import uvicorn
from fastapi import FastAPI
from mslb_contracts.resources import parse_resource_guard
from mslb_persistence.database import Database

from mslb_controller.credentials import read_token
from mslb_controller.proxy import create_app


def build() -> FastAPI:
    return create_app(
        database=Database(
            url=os.environ["DATABASE_URL"],
            enforce_resource_guard=parse_resource_guard(
                value=os.environ.get(key="RESOURCE_GUARD_ENABLED", default="true")
            ),
            schema=os.environ.get(key="CONTROL_DATABASE_SCHEMA", default="public"),
        ),
        token=read_token(role="controller"),
        guard_token=read_token(role="guard"),
    )


if __name__ == "__main__":
    uvicorn.run(
        app="mslb_controller.proxy_main:build",
        factory=True,
        host=os.environ.get(key="PROXY_BIND_HOST", default="127.0.0.1"),
        port=8081,
        workers=1,
        proxy_headers=False,
        access_log=False,
        limit_concurrency=24,
        timeout_keep_alive=2,
        timeout_graceful_shutdown=50,
        backlog=32,
        h11_max_incomplete_event_size=8192,
    )
