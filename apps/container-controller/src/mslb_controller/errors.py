"""Safe failures shared by controller routing and daemon adapters."""


class TrafficError(Exception):
    """Safe operational refusal/failure; never a logical simulation outcome."""

    def __init__(
        self, code: str, backend_id: str | None = None, status_code: int | None = None
    ) -> None:
        self.code = code
        self.backend_id = backend_id
        self.status_code = status_code
        super().__init__(code)


class EngineError(Exception):
    """Safe status only; daemon/transport error text can contain privileged host details."""

    def __init__(self, code: str, status: int | None = None) -> None:
        self.code, self.status = code, status
        super().__init__(code)
