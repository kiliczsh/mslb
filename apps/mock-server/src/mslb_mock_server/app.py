"""One bounded private backend; no daemon credentials, arbitrary URLs or physical memory jobs."""

import asyncio
import os
import re
import time
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import Self, TypedDict

from fastapi import FastAPI, Request
from mslb_contracts.traffic import BackendHealth, WorkCompleted, WorkRequest
from pydantic import ValidationError
from starlette.requests import ClientDisconnect
from starlette.responses import JSONResponse

from mslb_mock_server.work import WORK_TIMEOUT_SECONDS, CpuResult, WorkStopped, execute


@dataclass(frozen=True, slots=True)
class Settings:
    backend_id: str
    logical_admission_mem_mb: int = 1024
    max_inflight: int = 4

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,127}", self.backend_id):
            raise ValueError("Invalid backend identity")
        if (
            type(self.logical_admission_mem_mb) is not int
            or not 1 <= self.logical_admission_mem_mb <= 1_000_000_000
            or type(self.max_inflight) is not int
            or not 1 <= self.max_inflight <= 4
        ):
            raise ValueError("Invalid backend limits")

    @classmethod
    def environment(cls) -> Self:
        return cls(
            backend_id=os.environ["BACKEND_ID"],
            logical_admission_mem_mb=int(os.environ.get("LOGICAL_ADMISSION_MEM_MB", "1024")),
            max_inflight=int(os.environ.get("MAX_INFLIGHT", "4")),
        )


def _number(path: str) -> int | None:
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(129)
        return int(raw) if len(raw) <= 128 and raw.strip().isdigit() else None
    except (OSError, ValueError):
        return None


class Measurements(TypedDict):
    rss_bytes: int | None
    cgroup_memory_bytes: int | None
    cpu_usage_usec: int | None
    throttled_usec: int | None


def _observations() -> Measurements:
    rss: int | None = None
    cpu: dict[str, int] = {}
    try:
        with Path("/proc/self/statm").open("rb") as stream:
            fields = stream.read(256).split()
        rss = int(fields[1]) * os.sysconf("SC_PAGE_SIZE")
        with Path("/sys/fs/cgroup/cpu.stat").open("rb") as stream:
            for line in stream.read(4096).splitlines():
                name, value = line.split()
                if name in {b"usage_usec", b"throttled_usec"}:
                    cpu[name.decode("ascii")] = int(value)
    except (OSError, ValueError, IndexError):
        pass  # Unsupported measurement is represented by null, never fabricated zero.
    return {
        "rss_bytes": rss,
        "cgroup_memory_bytes": _number("/sys/fs/cgroup/memory.current"),
        "cpu_usage_usec": cpu.get("usage_usec"),
        "throttled_usec": cpu.get("throttled_usec"),
    }


class Backend:
    """Admission is non-queuing and remains reserved until CPU work actually stops."""

    def __init__(
        self,
        settings: Settings,
        work: Callable[[int, Event, float], CpuResult] = execute,
    ) -> None:
        self.settings = settings
        self.work = work
        self.inflight = 0
        self.reserved = 0
        self.pool = ThreadPoolExecutor(max_workers=settings.max_inflight, thread_name_prefix="work")
        self.active: dict[asyncio.Future[CpuResult], Event] = {}
        self.closed = False

    async def run(self, payload: WorkRequest) -> WorkCompleted:
        if (
            self.closed
            or self.inflight >= self.settings.max_inflight
            or self.reserved + payload.mem_mb > self.settings.logical_admission_mem_mb
        ):
            raise WorkStopped("BACKEND_BUSY")
        self.inflight += 1
        self.reserved += payload.mem_mb
        stopped = Event()
        deadline = time.monotonic() + WORK_TIMEOUT_SECONDS
        try:
            future = asyncio.get_running_loop().run_in_executor(
                self.pool, self.work, payload.work_units, stopped, deadline
            )
        except (RuntimeError, OSError):
            self.inflight -= 1
            self.reserved -= payload.mem_mb
            raise WorkStopped("RESOURCE_PRESSURE") from None
        self.active[future] = stopped

        def release(done: asyncio.Future[CpuResult]) -> None:
            if self.active.pop(done, None) is None:
                return
            self.inflight -= 1
            self.reserved -= payload.mem_mb
            if not done.cancelled():
                # Retrieve detached-caller failures without logging input.
                done.exception()

        future.add_done_callback(release)
        try:
            result = await asyncio.shield(future)
            return WorkCompleted(
                backend_id=self.settings.backend_id,
                request_id=payload.request_id,
                operations=result.operations,
                checksum=result.checksum,
                duration_ms=result.duration_ms,
            )
        except asyncio.CancelledError:
            stopped.set()
            # Retain admission until the future stops, including repeated caller cancellation.
            raise
        finally:
            if future.done():
                release(future)

    async def close(self) -> None:
        self.closed = True
        for stopped in self.active.values():
            stopped.set()
        if self.active:
            await asyncio.gather(*self.active, return_exceptions=True)
        self.pool.shutdown(wait=True, cancel_futures=True)

    def health(self) -> BackendHealth:
        return BackendHealth(
            backend_id=self.settings.backend_id,
            ready=not self.closed,
            logical_admission_mem_mb=self.settings.logical_admission_mem_mb,
            max_inflight=self.settings.max_inflight,
            inflight=self.inflight,
            reserved_mem_mb=self.reserved,
            **_observations(),
        )


def _error(code: str, status: int) -> JSONResponse:
    return JSONResponse({"error": {"code": code}}, status_code=status)


def create_app(settings: Settings, backend: Backend | None = None) -> FastAPI:
    service = backend or Backend(settings)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
        try:
            yield
        finally:
            await service.close()

    app = FastAPI(lifespan=lifespan, openapi_url=None, docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health() -> JSONResponse:
        return JSONResponse(service.health().model_dump())

    @app.post("/work")
    async def work(request: Request) -> JSONResponse:
        if request.headers.get("content-type", "").split(";")[0].lower() != "application/json":
            return _error("UNSUPPORTED_MEDIA_TYPE", 415)
        raw = bytearray()
        try:
            async with asyncio.timeout(2):
                async for chunk in request.stream():
                    if len(raw) + len(chunk) > 64 * 1024:
                        return _error("PAYLOAD_TOO_LARGE", 413)
                    raw.extend(chunk)
            payload = WorkRequest.model_validate_json(raw)
        except TimeoutError:
            return _error("BODY_TIMEOUT", 408)
        except ClientDisconnect:
            return _error("CLIENT_DISCONNECTED", 400)
        except ValidationError:
            return _error("VALIDATION_ERROR", 422)
        try:
            result = await service.run(payload)
        except WorkStopped as error:
            status = (
                429
                if error.code == "BACKEND_BUSY"
                else 503
                if error.code == "RESOURCE_PRESSURE"
                else 504
            )
            return _error(error.code, status)
        return JSONResponse(result.model_dump())

    return app
