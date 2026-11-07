"""ASGI access and admission checks before framework body parsing."""

import hmac
from uuid import uuid4

from mslb_application.errors import OperationError
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from mslb_api.docs_ui import is_documentation
from mslb_api.errors import error_response
from mslb_api.http_exchange import HttpExchange
from mslb_api.preflight import preflight_response
from mslb_api.request_policy import validate_request
from mslb_api.settings import Settings


class Guard:
    """Enforce access and bounded admission; counters include active streams."""

    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app, self.settings = app, settings
        self.active, self.streams, self.uploading = 0, 0, False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Validate, authorize and release admission after streaming settles."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        request_id = str(object=uuid4())
        scope.setdefault("state", {})["request_id"] = request_id
        headers = Headers(scope=scope)
        path, method = scope["path"], scope["method"]
        health = path in {"/api/v1/health/live", "/api/v1/health/ready"}
        streaming = path.endswith("/stream")
        upload = path == "/api/v1/datasets" and method == "POST"
        origin = headers.get("origin")
        exchange = HttpExchange(
            scope=scope, receive=receive, send=send, request_id=request_id, origin=origin
        )
        admitted = False
        try:
            validate_request(scope=scope, headers=headers, settings=self.settings)
            if method == "OPTIONS":
                response = preflight_response(
                    origin=origin, settings=self.settings, request_id=request_id
                )
                await response(scope, receive, send)
                return
            if not health and not is_documentation(path=path, method=method):
                self._authenticate(headers=headers)
            if not health:
                self._admit(streaming=streaming, upload=upload)
                admitted = True
            await exchange.dispatch(app=self.app, upload=upload, headers=headers)
        except OperationError as error:
            if exchange.started:
                raise
            response = error_response(
                code=error.code, message=str(object=error), request_id=request_id
            )
            if origin is not None and origin in self.settings.allowed_origins:
                response.headers["Access-Control-Allow-Origin"] = origin
                response.headers["Vary"] = "Origin"
                response.headers["Access-Control-Expose-Headers"] = "X-Request-ID"
            await response(scope, receive, send)
        finally:
            if admitted:
                self.active -= 1
                self.streams -= int(streaming)
                if upload:
                    self.uploading = False

    def _authenticate(self, *, headers: Headers) -> None:
        """Compare the complete bearer value in constant time when enabled."""
        if not self.settings.auth_enabled:
            return
        supplied = headers.get("authorization", "").encode(encoding="utf-8")
        expected = ("Bearer " + self.settings.token).encode()
        if not hmac.compare_digest(supplied, expected):
            raise OperationError(
                code="UNAUTHORIZED", message="A valid bearer credential is required"
            )

    def _admit(self, *, streaming: bool, upload: bool) -> None:
        """Reserve one request slot and any exclusive upload/stream capacity."""
        if (
            self.active >= self.settings.request_limit
            or (streaming and self.streams >= self.settings.stream_limit)
            or (upload and self.uploading)
        ):
            raise OperationError(code="RATE_LIMITED", message="API request capacity is occupied")
        self.active += 1
        self.streams += int(streaming)
        self.uploading = self.uploading or upload
