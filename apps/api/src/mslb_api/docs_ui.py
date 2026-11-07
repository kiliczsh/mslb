"""Serve offline ReDoc assets while keeping the OpenAPI document authenticated."""

from collections.abc import Awaitable, Callable
from pathlib import Path

from fastapi import FastAPI, Request
from starlette.responses import FileResponse
from starlette.routing import Route

DOCS_ROOT = "/api/docs"
ASSET_DIRECTORY = Path(__file__).with_name("static")
# ReDoc injects computed styles and embeds a WOFF font. External fonts are forbidden.
DOCS_CSP = (
    "default-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'; "
    "connect-src 'self'; img-src 'self' data:; font-src 'self' data:; "
    "object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
)
ASSETS = {
    DOCS_ROOT: ("docs.html", "text/html"),
    DOCS_ROOT + "/client.js": ("client.js", "text/javascript"),
    DOCS_ROOT + "/docs.css": ("docs.css", "text/css"),
    DOCS_ROOT + "/redoc.js": ("redoc.js", "text/javascript"),
    DOCS_ROOT + "/LICENSE": ("redoc.LICENSE", "text/plain"),
    DOCS_ROOT + "/redoc-logo-full.svg": ("redoc-logo-full.svg", "image/svg+xml"),
    DOCS_ROOT + "/redoc-logo.svg": ("redoc-logo.svg", "image/svg+xml"),
}


def is_documentation(*, path: str, method: str) -> bool:
    """Allow anonymous reads of fixed static files, never of API schema/data."""
    return method in {"GET", "HEAD"} and path in ASSETS


def register_documentation(*, app: FastAPI) -> None:
    """Register a public documentation shell with a bearer-aware schema loader."""
    for route_path, (filename, media_type) in ASSETS.items():
        app.router.routes.append(
            Route(
                path=route_path,
                endpoint=_asset_endpoint(filename=filename, media_type=media_type),
                methods=["GET", "HEAD"],
            )
        )


def _asset_endpoint(
    *, filename: str, media_type: str
) -> Callable[[Request], Awaitable[FileResponse]]:
    """Capture one allowlisted filename without accepting path/query overrides."""

    async def serve(request: Request) -> FileResponse:
        """Return one build-owned asset with a restrictive document policy."""
        return FileResponse(
            path=ASSET_DIRECTORY / filename,
            media_type=media_type,
            headers={"Content-Security-Policy": DOCS_CSP, "Referrer-Policy": "no-referrer"},
        )

    return serve
