"""Typed deterministic domain failures."""


class SimulationError(ValueError):
    """Typed deterministic failure; partial output is never a final artifact."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)
