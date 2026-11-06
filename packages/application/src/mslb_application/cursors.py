"""Authenticated opaque positions bound to one collection and filter identity."""

import base64
import binascii
import hashlib
import hmac
import time
from datetime import datetime
from typing import Annotated
from uuid import UUID

from mslb_contracts.canonical import canonical_bytes
from mslb_contracts.queries import ReadModel
from pydantic import Field, ValidationError

from mslb_application.errors import OperationError


class Position(ReadModel):
    """Represent one signed cursor position without retaining a whole result collection."""

    created_at: datetime | None = None
    id: UUID | None = None
    sequence: Annotated[int, Field(ge=0, le=30_000)] = 0
    key: Annotated[str, Field(min_length=1, max_length=128)] | None = None


class _Cursor(ReadModel):
    """Bind an expiring continuation position to its collection and filter identity."""

    version: Annotated[int, Field(ge=1, le=1)] = 1
    context: Annotated[str, Field(min_length=64, max_length=64)]
    expires: int
    position: Position


class Cursors:
    """Authenticate bounded expiring cursors bound to collection/filter identity."""

    def __init__(self, secret: bytes) -> None:
        if not isinstance(secret, bytes) or not 32 <= len(secret) <= 256:
            raise OperationError(
                code="VALIDATION_ERROR", message="Cursor signing key must contain 32–256 bytes"
            )
        self.secret = secret

    def encode(self, context: object, position: Position) -> str:
        cursor = _Cursor(
            context=hashlib.sha256(data=canonical_bytes(value=context)).hexdigest(),
            expires=int(time.time()) + 24 * 60 * 60,
            position=position,
        )
        payload = cursor.model_dump_json().encode()
        signature = hmac.digest(key=self.secret, msg=payload, digest="sha256")
        return base64.urlsafe_b64encode(s=signature + payload).decode().rstrip("=")

    def decode(self, context: object, token: str | None) -> Position:
        """Verify cursor authentication, expiry and collection/filter identity before use."""
        if token is None:
            return Position()
        try:
            if type(token) is not str or not 1 <= len(token) <= 2048:
                raise ValueError("Invalid cursor length")
            raw = base64.b64decode(s=token + "=" * (-len(token) % 4), altchars=b"-_", validate=True)
            signature, payload = raw[:32], raw[32:]
            if not hmac.compare_digest(
                signature, hmac.digest(key=self.secret, msg=payload, digest="sha256")
            ):
                raise ValueError("Invalid cursor signature")
            value = _Cursor.model_validate_json(json_data=payload)
            if (
                value.context != hashlib.sha256(data=canonical_bytes(value=context)).hexdigest()
                or value.expires <= int(time.time())
                or value.expires > int(time.time()) + 24 * 60 * 60 + 1
            ):
                raise ValueError("Cursor context or lifetime differs")
            return value.position
        except (ValueError, binascii.Error, ValidationError):
            raise OperationError(
                code="VALIDATION_ERROR", message="Invalid, expired, or unrelated cursor"
            ) from None
