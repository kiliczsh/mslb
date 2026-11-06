"""OpenAPI metadata for access policy, mutation versions and replay headers."""

from fastapi import APIRouter
from fastapi.routing import APIRoute


def configure_routes(*, router: APIRouter, auth_enabled: bool) -> None:
    """Describe actual access/header policy without changing request enforcement."""
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        extra: dict[str, object] = {}
        if not auth_enabled:
            # FastAPI appends extra security alternatives to its generated bearer entry.
            extra["security"] = [{}]
        if route.name in {"patch_server", "delete_server"}:
            extra["parameters"] = [
                {
                    "name": "If-Match",
                    "in": "header",
                    "required": True,
                    "description": "Current strong ETag; stale or missing versions conflict.",
                    "schema": {"type": "string", "pattern": '^"[1-9][0-9]{0,9}"$'},
                    "example": '"1"',
                }
            ]
        elif route.name == "submit":
            extra["parameters"] = [
                {
                    "name": "Idempotency-Key",
                    "in": "header",
                    "required": False,
                    "description": "Matching retries reuse a run; changed bodies conflict.",
                    "schema": {"type": "string", "minLength": 1, "maxLength": 128},
                    "example": "example-repeatable-submission",
                }
            ]
        elif route.name == "stream":
            extra["parameters"] = [
                {
                    "name": "Last-Event-ID",
                    "in": "header",
                    "required": False,
                    "description": "Resume after this committed sequence; defaults to zero.",
                    "schema": {"type": "string", "pattern": "^[0-9]{1,5}$", "default": "0"},
                    "example": "42",
                }
            ]
        route.openapi_extra = (route.openapi_extra or {}) | extra
