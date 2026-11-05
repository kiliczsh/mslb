"""Stable application operation failures shared across transport adapters."""


class OperationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)
