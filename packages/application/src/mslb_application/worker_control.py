"""Thread-safe worker interruption state and typed stop signals."""

from threading import Event


class ExecutionStopped(ValueError):
    """Signal a supervisor stop without pretending the attempt completed successfully."""


class _Control:
    """Share stop priority and an ownership deadline with the execution thread."""

    def __init__(self) -> None:
        self.reason: str | None = None
        self.stopped = Event()
        self.deadline = float("inf")

    def stop(self, reason: str) -> None:
        """Preserve cancellation and fence loss over less urgent stop reasons."""
        if not self.stopped.is_set() or reason in {"cancel", "lease_lost"}:
            self.reason = reason
            self.stopped.set()

    def check(self) -> None:
        if self.reason in {"cancel", "lease_lost"}:
            raise ExecutionStopped(self.reason)
