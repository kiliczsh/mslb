"""Immutable validated simulation request and server capacities."""

from dataclasses import dataclass

from mslb_engine.validation import _identifier, _integer


@dataclass(frozen=True, slots=True)
class Server:
    """Describe immutable logical CPU, memory and start-rate capacity."""

    id: str
    cpu_units_per_tick: int
    mem_mb: int
    rate_limit_per_sec: int

    def __post_init__(self) -> None:
        _identifier(self.id)
        for value in (self.cpu_units_per_tick, self.mem_mb, self.rate_limit_per_sec):
            _integer(value, 1, 1_000_000_000)


@dataclass(frozen=True, slots=True)
class Request:
    """Describe an immutable arrival, positive work and full-duration memory reservation."""

    t: int
    request_id: str
    work_units: int
    mem_mb: int

    def __post_init__(self) -> None:
        _identifier(self.request_id)
        _integer(self.t, 0, 999_999)
        _integer(self.work_units, 1, 1_000_000_000)
        _integer(self.mem_mb, 1, 1_000_000_000)
