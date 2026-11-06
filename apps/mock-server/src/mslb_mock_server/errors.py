"""Safe incomplete-work outcomes for private backend callers."""


class WorkStopped(Exception):
    """Execution stopped without a completed outcome."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)
