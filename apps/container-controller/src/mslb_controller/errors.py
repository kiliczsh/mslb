"""Safe failures shared by controller routing, proxy and daemon adapters."""

from mslb_contracts.docker_runtime import HttpReason


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


class DispatchRefused(TrafficError):
    """The owned task reached dispatch after its admission became unusable."""

    def __init__(self, *, reason: HttpReason) -> None:
        self.reason: HttpReason = reason
        super().__init__(code=reason)


class Refusal(Exception):
    def __init__(self, status: int, code: str) -> None:
        self.status, self.code = status, code
        super().__init__(code)
