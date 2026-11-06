"""Stable application operation failures shared across transport adapters."""


class OperationError(ValueError):
    """Carry a stable safe application code across transport boundaries."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)
