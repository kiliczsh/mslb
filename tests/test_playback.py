import io
import json
from collections.abc import Mapping
from uuid import UUID

import pytest
from mslb_application.errors import OperationError
from mslb_application.playback import read_playback


def frame(tick: int, duration: int, terminal: bool = False) -> dict[str, object]:
    servers = [
        dict(id=f"s{i:02d}", cpu_units_per_tick=10, mem_mb=1000, rate_limit_per_sec=10)
        for i in range(64)
    ]
    return dict(
        t=tick,
        duration_ticks=duration,
        terminal=terminal,
        membership=servers,
        pending_ids=[] if terminal else [f"q{i:04d}" for i in range(501)],
        servers=[]
        if terminal
        else [
            dict(
                server_id=server["id"],
                active_ids=[f"r{i:04d}" for i in range(501)] if server["id"] == "s00" else [],
                reserved_mem_mb=501 if server["id"] == "s00" else 0,
                capacity=10,
                memory_capacity=1000,
                starts=0,
                allocated_cpu=10,
                consumed_cpu=dict(numerator="1", denominator="3"),
            )
            for server in servers
        ],
    )


def records(*values: Mapping[str, object]) -> io.BytesIO:
    return io.BytesIO(b"".join((json.dumps(value) + "\n").encode() for value in values))


def test_playback_reads_idle_coverage_and_pages_all_requests_and_server_lanes() -> None:
    run_id = UUID(int=1)
    timeline = records(frame(0, 100), frame(100, 0, True))
    result = read_playback(timeline, records(), run_id, "a" * 64, 50, 32, "active", "s00", 500)
    assert result.frame_tick == 0 and result.duration_ticks == 100 and result.tick == 50
    assert len(result.servers) == 32 and result.servers[0].id == "s32"
    assert result.active_count == 501 and result.queue_depth == 501
    assert result.request_count == 501 and result.request_ids == ("r0500",)
    timeline.seek(0)
    pending = read_playback(timeline, records(), run_id, "a" * 64, 50, 0, "pending", None, 100)
    assert pending.request_ids == tuple(f"q{i:04d}" for i in range(100, 200))
    assert pending.servers[0].consumed_cpu is not None
    assert pending.membership_added == () and pending.membership_removed == ()
    timeline.seek(0)
    terminal = read_playback(timeline, records(), run_id, "a" * 64, 100, 0, "pending", None, 0)
    assert terminal.terminal and terminal.active_count == 0
    assert terminal.servers[0].consumed_cpu is None
    timeline.seek(0)
    absent = read_playback(timeline, records(), run_id, "a" * 64, 50, 0, "active", "removed", 0)
    assert not absent.server_present and absent.request_count == 0
    timeline.seek(0)
    with pytest.raises(OperationError, match="outside"):
        read_playback(timeline, records(), run_id, "a" * 64, 101, 0, "pending", None, 0)


def test_playback_reports_membership_changes_and_decision_activation_without_reexecution() -> None:
    first, second = frame(0, 1), frame(1, 1)
    # Keep IDs canonical while changing the last member in the next interval.
    members = second["membership"]
    assert isinstance(members, list)
    members[-1]["id"] = "s64"
    samples = second["servers"]
    assert isinstance(samples, list)
    samples[-1]["server_id"] = "s64"
    decision = dict(
        t=0,
        action="scale_out",
        trigger=["cpu_mean_high"],
        applied_at=1,
        server_ids_added=["s64"],
        server_ids_removed=[],
        reason=None,
    )
    result = read_playback(
        records(first, second), records(decision), UUID(int=1), "a" * 64, 1, 0, "pending", None, 0
    )
    assert result.membership_added == ("s64",) and result.membership_removed == ("s63",)
    assert result.decisions[0].applied_at == 1
    invalid = frame(2, 1)
    with pytest.raises(OperationError, match="interval"):
        read_playback(records(invalid), records(), UUID(int=1), "a" * 64, 2, 0, "pending", None, 0)
