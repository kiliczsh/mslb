"""Serve one private work backend with finite HTTP connections and shutdown."""

import uvicorn
from fastapi import FastAPI

from mslb_mock_server.app import Settings, create_app


def build() -> FastAPI:
    return create_app(Settings.environment())


if __name__ == "__main__":
    uvicorn.run(
        "mslb_mock_server.__main__:build",
        factory=True,
        host="0.0.0.0",  # noqa: S104 -- isolated un-published Docker traffic network only.
        port=8080,
        workers=1,
        proxy_headers=False,
        access_log=False,
        limit_concurrency=24,
        backlog=32,
        timeout_keep_alive=5,
        timeout_graceful_shutdown=4,
        h11_max_incomplete_event_size=16384,
    )
