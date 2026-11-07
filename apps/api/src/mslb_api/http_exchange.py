"""Per-request bounded body and response metadata, including streaming failures."""

from http import HTTPStatus

from mslb_application.errors import OperationError
from mslb_contracts.inputs import MAX_UPLOAD_BYTES
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Message, Receive, Scope, Send

# Upload allowance includes a bounded multipart envelope around the 10 MiB CSV.
JSON_BODY_BYTES = 64 * 1024
UPLOAD_BODY_BYTES = MAX_UPLOAD_BYTES + JSON_BODY_BYTES
CONTENT_LENGTH_DIGITS = 10  # Reject arbitrarily large integer text before conversion.


class HttpExchange:
    """Own body counters and response-start state for one ASGI request."""

    def __init__(
        self, *, scope: Scope, receive: Receive, send: Send, request_id: str, origin: str | None
    ) -> None:
        self.scope, self.receive, self.send = scope, receive, send
        self.request_id, self.origin = request_id, origin
        self.started = False

    async def dispatch(self, *, app: ASGIApp, upload: bool, headers: Headers) -> None:
        """Count streamed bytes and tag headers without buffering the body."""
        maximum = UPLOAD_BODY_BYTES if upload else JSON_BODY_BYTES
        length = headers.get("content-length")
        if length is not None and (
            not length.isascii() or not length.isdecimal() or len(length) > CONTENT_LENGTH_DIGITS
        ):
            raise OperationError(code="VALIDATION_ERROR", message="Invalid Content-Length")
        if length is not None and int(length) > maximum:
            raise OperationError(
                code="PAYLOAD_TOO_LARGE", message="Request body exceeds its byte limit"
            )
        received = 0

        async def bounded_receive() -> Message:
            nonlocal received
            message = await self.receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > maximum:
                    raise OperationError(
                        code="PAYLOAD_TOO_LARGE", message="Request body exceeds its byte limit"
                    )
            return message

        async def tagged_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                self.started = True
                response_headers = [
                    (k, v) for k, v in message.get("headers", []) if k.lower() != b"x-request-id"
                ]
                response_headers.extend(
                    [
                        (b"x-request-id", self.request_id.encode()),
                        (b"x-content-type-options", b"nosniff"),
                        (b"cache-control", b"no-store"),
                    ]
                )
                if message["status"] == HTTPStatus.SERVICE_UNAVAILABLE.value and not any(
                    key.lower() == b"retry-after" for key, _ in response_headers
                ):
                    response_headers.append((b"retry-after", b"1"))
                if self.origin is not None:
                    response_headers.extend(
                        [
                            (b"access-control-allow-origin", self.origin.encode()),
                            (b"vary", b"Origin"),
                            (
                                b"access-control-expose-headers",
                                b"ETag,Location,X-Request-ID,Mcp-Session-Id",
                            ),
                        ]
                    )
                message["headers"] = response_headers
            await self.send(message)

        await app(self.scope, bounded_receive, tagged_send)
