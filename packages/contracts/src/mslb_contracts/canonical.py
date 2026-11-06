"""Canonical byte identity shared by transport, application, and persistence."""

import json


def canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(
            obj=value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
        )
        + "\n"
    ).encode(encoding="utf-8")
