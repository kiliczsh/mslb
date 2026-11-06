"""Typed persistence failures translated at application/transport boundaries."""


class StoreError(ValueError):
    """Carry a safe persistence refusal without raw SQL or uploaded content."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)
