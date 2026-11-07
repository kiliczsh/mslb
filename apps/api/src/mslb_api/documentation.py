"""OpenAPI metadata for access policy, mutation versions and replay headers."""

from fastapi import APIRouter
from fastapi.routing import APIRoute


def configure_routes(*, router: APIRouter, auth_enabled: bool) -> None:
    """Describe actual access/header policy without changing request enforcement."""
    for route in router.routes:
        if not isinstance(route, APIRoute):
            continue
        subject = route.path.removeprefix("/api/v1/").split("/")[0]
        route.tags = [
            {
                "servers": "Servers",
                "datasets": "Datasets",
                "simulations": "Logical simulations",
                "docker": "Docker traffic",
                "history": "History",
                "strategies": "Strategies",
            }.get(subject, "Installation")
        ]
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
        elif route.name in {"submit", "submit_docker"}:
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
            if route.name == "submit":
                # Runtime literals/extra-field rejection select one variant while preserving
                # omitted logical mode. FastAPI's eager tagged-union selection bypasses an
                # outer defaulting validator, so the discriminator is documentation metadata.
                extra["requestBody"] = {
                    "content": {
                        "application/json": {
                            "schema": {
                                "discriminator": {
                                    "propertyName": "mode",
                                    "mapping": {
                                        "sequential": "#/components/schemas/SimulationCreate",
                                        "concurrent": "#/components/schemas/SimulationCreate",
                                        "docker": "#/components/schemas/DockerSimulationCreate",
                                    },
                                }
                            }
                        }
                    }
                }
        elif route.name == "stream":
            extra["parameters"] = [
                {
                    "name": "Last-Event-ID",
                    "in": "header",
                    "required": False,
                    "description": "Resume after this committed sequence; defaults to zero.",
                    "schema": {"type": "string", "pattern": "^[0-9]{1,6}$", "default": "0"},
                    "example": "42",
                }
            ]
        route.openapi_extra = (route.openapi_extra or {}) | extra
