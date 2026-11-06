"""Stream to one persisted interval with bounded server/request/scaling projections."""

from typing import BinaryIO, Literal
from uuid import UUID

from mslb_contracts.playback import PlaybackDecision, PlaybackServer, PlaybackView

from mslb_application.errors import OperationError
from mslb_application.metric_queries import TimelineFrame


def _frame(timeline: BinaryIO, tick: int) -> tuple[TimelineFrame, set[str] | None]:
    """Read the saved interval covering a tick without retaining timeline history."""
    previous_end = 0
    previous_ids: set[str] | None = None
    for _ in range(1_000_001):
        line = timeline.readline(8 * 1024**2 + 1)
        if not line:
            break
        if len(line) > 8 * 1024**2 or not line.endswith(b"\n"):
            raise OperationError("VALIDATION_FAILED", "Timeline exceeds supported frame bounds")
        frame = TimelineFrame.model_validate_json(line)
        ids = [server.id for server in frame.membership]
        end = frame.t + frame.duration_ticks
        if (
            frame.t != previous_end
            or frame.terminal != (frame.duration_ticks == 0)
            or ids != sorted(set(ids))
            or (not frame.terminal and [sample.server_id for sample in frame.servers] != ids)
        ):
            raise OperationError("VALIDATION_FAILED", "Timeline interval or membership is invalid")
        if frame.t <= tick < end or (frame.terminal and tick == frame.t):
            return frame, previous_ids
        if frame.terminal or frame.t > tick:
            raise OperationError("NOT_FOUND", "Tick is outside the completed run")
        previous_end, previous_ids = end, set(ids)
    else:
        raise OperationError("VALIDATION_FAILED", "Timeline exceeds its frame count")
    raise OperationError("NOT_FOUND", "Tick is outside the completed run")


def read_playback(
    timeline: BinaryIO,
    scaling: BinaryIO,
    run_id: UUID,
    bundle_digest: str,
    tick: int,
    server_offset: int,
    request_kind: Literal["pending", "active"],
    server_id: str | None,
    request_offset: int,
) -> PlaybackView:
    """Read bounded persisted membership/request slices without replaying engine execution."""
    frame, previous_ids = _frame(timeline, tick)
    ids = [member.id for member in frame.membership]
    samples = {sample.server_id: sample for sample in frame.servers}
    servers = []
    for member in frame.membership[server_offset : server_offset + 32]:
        sample = samples.get(member.id)
        servers.append(
            PlaybackServer(
                **member.model_dump(),
                active_count=len(sample.active_ids) if sample else 0,
                reserved_mem_mb=sample.reserved_mem_mb if sample else None,
                allocated_cpu=sample.allocated_cpu if sample else None,
                consumed_cpu=sample.consumed_cpu if sample else None,
                starts=sample.starts if sample else None,
            )
        )
    selected = samples.get(server_id) if server_id is not None else None
    requests = (
        frame.pending_ids
        if request_kind == "pending"
        else selected.active_ids
        if selected is not None
        else ()
    )
    decisions = []
    previous_tick = -1
    for index in range(1_000_001):
        line = scaling.readline(65537)
        if not line:
            break
        if index == 1_000_000:
            raise OperationError("VALIDATION_FAILED", "Scaling history exceeds its record count")
        if len(line) > 65536 or not line.endswith(b"\n"):
            raise OperationError("VALIDATION_FAILED", "Scaling record exceeds its byte bound")
        decision = PlaybackDecision.model_validate_json(line)
        if decision.t <= previous_tick:
            raise OperationError("VALIDATION_FAILED", "Scaling records are not ordered")
        previous_tick = decision.t
        if decision.t > tick:
            break
        if decision.t == tick or decision.applied_at == tick:
            decisions.append(decision)
    else:
        raise OperationError("VALIDATION_FAILED", "Scaling history exceeds its record count")
    return PlaybackView(
        run_id=run_id,
        bundle_digest=bundle_digest,
        tick=tick,
        frame_tick=frame.t,
        duration_ticks=frame.duration_ticks,
        terminal=frame.terminal,
        membership_count=len(ids),
        server_offset=server_offset,
        servers=tuple(servers),
        membership_added=tuple(sorted(set(ids) - previous_ids)) if previous_ids is not None else (),
        membership_removed=tuple(sorted(previous_ids - set(ids)))
        if previous_ids is not None
        else (),
        queue_depth=len(frame.pending_ids),
        active_count=sum(len(sample.active_ids) for sample in samples.values()),
        request_kind=request_kind,
        server_id=server_id,
        server_present=server_id in ids if server_id is not None else True,
        request_offset=request_offset,
        request_count=len(requests),
        request_ids=requests[request_offset : request_offset + 100],
        decisions=tuple(decisions),
    )
