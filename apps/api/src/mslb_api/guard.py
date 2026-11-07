"""ASGI access/body/admission checks happen before framework body parsing."""

import hmac
from http import HTTPStatus
from uuid import uuid4

from mslb_application.errors import OperationError
from starlette.datastructures import Headers, QueryParams
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from mslb_api.errors import error_response
from mslb_api.settings import Settings


class Guard:
    """Enforce access and bounded HTTP admission before framework body parsing."""

    def __init__(self, app: ASGIApp, settings: Settings) -> None:
        self.app, self.settings = app, settings
        self.active, self.streams, self.uploading = 0, 0, False

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Admit bounded HTTP work and release counters after streaming settles."""
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
        admitted, started = False, False
        try:
            if (
                len(headers) > 64
                or sum(len(k) + len(v) for k, v in scope["headers"]) > 16384
                or len(path) + len(scope.get("query_string", b"")) > 4096
            ):
                raise OperationError(
                    code="PAYLOAD_TOO_LARGE", message="Request headers or URL exceed limits"
                )
            for name in (
                "host",
                "origin",
                "authorization",
                "content-length",
                "content-type",
                "if-match",
                "last-event-id",
                "idempotency-key",
                "mcp-session-id",
                "mcp-protocol-version",
                "mcp-method",
                "mcp-name",
            ):
                if len(headers.getlist(key=name)) > 1:
                    raise OperationError(
                        code="VALIDATION_ERROR", message="Duplicate singleton request header"
                    )
            if headers.get("host", "").lower() not in self.settings.allowed_hosts:
                raise OperationError(code="FORBIDDEN", message="Unexpected request Host")
            origin = headers.get("origin")
            if origin is not None and origin not in self.settings.allowed_origins:
                raise OperationError(code="FORBIDDEN", message="Unexpected request Origin")
            if any(
                key.lower() in {"token", "access_token", "authorization"}
                for key in QueryParams(scope.get("query_string", b""))
            ):
                raise OperationError(
                    code="UNAUTHORIZED",
                    message="Bearer credentials belong in the Authorization header",
                )
            if method == "OPTIONS":
                response = error_response(
                    code="FORBIDDEN",
                    message="Cross-origin preflight is not enabled",
                    request_id=request_id,
                )
                if origin is not None and origin in self.settings.allowed_origins:
                    response.status_code = HTTPStatus.NO_CONTENT.value
                    response.body = b""
                    response.headers["Content-Length"] = "0"
                    response.headers["Access-Control-Allow-Origin"] = origin
                    response.headers["Vary"] = "Origin"
                    response.headers["Access-Control-Allow-Methods"] = (
                        "GET,POST,PUT,PATCH,DELETE,OPTIONS"
                    )
                    response.headers["Access-Control-Allow-Headers"] = (
                        "Authorization,Content-Type,If-Match,Idempotency-Key,Last-Event-ID,"
                        "Mcp-Session-Id,MCP-Protocol-Version,MCP-Method,MCP-Name"
                    )
                await response(scope, receive, send)
                return
            if not health and self.settings.auth_enabled:
                supplied = headers.get("authorization", "").encode(encoding="utf-8")
                expected = ("Bearer " + self.settings.token).encode()
                if not hmac.compare_digest(supplied, expected):
                    raise OperationError(
                        code="UNAUTHORIZED", message="A valid bearer credential is required"
                    )
            if not health:
                if (
                    self.active >= self.settings.request_limit
                    or (streaming and self.streams >= self.settings.stream_limit)
                    or (upload and self.uploading)
                ):
                    raise OperationError(
                        code="RATE_LIMITED", message="API request capacity is occupied"
                    )
                self.active += 1
                self.streams += int(streaming)
                self.uploading = self.uploading or upload
                admitted = True
            maximum = 10 * 1024**2 + 64 * 1024 if upload else 64 * 1024
            length = headers.get("content-length")
            if length is not None and (
                not length.isascii() or not length.isdecimal() or len(length) > 10
            ):
                raise OperationError(code="VALIDATION_ERROR", message="Invalid Content-Length")
            if length is not None and int(length) > maximum:
                raise OperationError(
                    code="PAYLOAD_TOO_LARGE", message="Request body exceeds its byte limit"
                )
            received = 0

            async def bounded_receive() -> Message:
                nonlocal received
                message = await receive()
                if message["type"] == "http.request":
                    received += len(message.get("body", b""))
                    if received > maximum:
                        raise OperationError(
                            code="PAYLOAD_TOO_LARGE", message="Request body exceeds its byte limit"
                        )
                return message

            async def tagged_send(message: Message) -> None:
                nonlocal started
                if message["type"] == "http.response.start":
                    started = True
                    response_headers = [
                        (k, v)
                        for k, v in message.get("headers", [])
                        if k.lower() != b"x-request-id"
                    ]
                    response_headers.extend(
                        [
                            (b"x-request-id", request_id.encode()),
                            (b"x-content-type-options", b"nosniff"),
                            (b"cache-control", b"no-store"),
                        ]
                    )
                    if message["status"] == HTTPStatus.SERVICE_UNAVAILABLE.value and not any(
                        key.lower() == b"retry-after" for key, _ in response_headers
                    ):
                        response_headers.append((b"retry-after", b"1"))
                    if origin is not None:
                        response_headers.extend(
                            [
                                (b"access-control-allow-origin", origin.encode()),
                                (b"vary", b"Origin"),
                                (
                                    b"access-control-expose-headers",
                                    b"ETag,Location,X-Request-ID,Mcp-Session-Id",
                                ),
                            ]
                        )
                    message["headers"] = response_headers
                await send(message)

            await self.app(scope, bounded_receive, tagged_send)
        except OperationError as error:
            if started:
                raise
            response = error_response(
                code=error.code, message=str(object=error), request_id=request_id
            )
            allowed_origin = headers.get("origin")
            if allowed_origin is not None and allowed_origin in self.settings.allowed_origins:
                response.headers["Access-Control-Allow-Origin"] = allowed_origin
                response.headers["Vary"] = "Origin"
                response.headers["Access-Control-Expose-Headers"] = "X-Request-ID"
            await response(scope, receive, send)
        finally:
            if admitted:
                self.active -= 1
                self.streams -= int(streaming)
                if upload:
                    self.uploading = False
