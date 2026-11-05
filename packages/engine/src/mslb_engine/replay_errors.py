"""Independent replay diagnostics, shared by trace and sidecar readers."""


class ReplayError(ValueError):
    """Trace or policy violation with a bounded safe diagnostic."""
