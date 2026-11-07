import io
import json
from collections.abc import Mapping
from uuid import UUID

import pytest
from mslb_application.errors import OperationError
from mslb_application.playback import read_playback


def frame(tick: int, duration: int, terminal: bool = False) -> dict[str, object]:
    servers = [
        {"id": f"s{i:02d}", "cpu_units_per_tick": 10, "mem_mb": 1000, "rate_limit_per_sec": 10}
        for i in range(64)
    ]
    return {
        "t": tick,
        "duration_ticks": duration,
        "terminal": terminal,
        "membership": servers,
        "pending_ids": [] if terminal else [f"q{i:04d}" for i in range(501)],
        "servers": []
        if terminal
        else [
            {
                "server_id": server["id"],
                "active_ids": [f"r{i:04d}" for i in range(501)] if server["id"] == "s00" else [],
                "reserved_mem_mb": 501 if server["id"] == "s00" else 0,
                "capacity": 10,
                "memory_capacity": 1000,
                "starts": 0,
                "allocated_cpu": 10,
                "consumed_cpu": {"numerator": "1", "denominator": "3"},
            }
            for server in servers
        ],
    }


def records(*values: Mapping[str, object]) -> io.BytesIO:
    return io.BytesIO(
        initial_bytes=b"".join((json.dumps(obj=value) + "\n").encode() for value in values)
    )


def test_playback_reads_idle_coverage_and_pages_all_requests_and_server_lanes() -> None:
    run_id = UUID(int=1)
    timeline = records(frame(tick=0, duration=100), frame(tick=100, duration=0, terminal=True))
    result = read_playback(
        timeline=timeline,
        scaling=records(),
        run_id=run_id,
        bundle_digest="a" * 64,
        tick=50,
        server_offset=32,
        request_kind="active",
        server_id="s00",
        request_offset=500,
    )
    assert result.frame_tick == 0 and result.duration_ticks == 100 and result.tick == 50
    assert len(result.servers) == 32 and result.servers[0].id == "s32"
    assert result.active_count == 501 and result.queue_depth == 501
    assert result.request_count == 501 and result.request_ids == ("r0500",)
    timeline.seek(0)
    pending = read_playback(
        timeline=timeline,
        scaling=records(),
        run_id=run_id,
        bundle_digest="a" * 64,
        tick=50,
        server_offset=0,
        request_kind="pending",
        server_id=None,
        request_offset=100,
    )
    assert pending.request_ids == tuple(f"q{i:04d}" for i in range(100, 200))
    assert pending.servers[0].consumed_cpu is not None
    assert pending.membership_added == () and pending.membership_removed == ()
    timeline.seek(0)
    terminal = read_playback(
        timeline=timeline,
        scaling=records(),
        run_id=run_id,
        bundle_digest="a" * 64,
        tick=100,
        server_offset=0,
        request_kind="pending",
        server_id=None,
        request_offset=0,
    )
    assert terminal.terminal and terminal.active_count == 0
    assert terminal.servers[0].consumed_cpu is None
    timeline.seek(0)
    absent = read_playback(
        timeline=timeline,
        scaling=records(),
        run_id=run_id,
        bundle_digest="a" * 64,
        tick=50,
        server_offset=0,
        request_kind="active",
        server_id="removed",
        request_offset=0,
    )
    assert not absent.server_present and absent.request_count == 0
    timeline.seek(0)
    with pytest.raises(expected_exception=OperationError, match="outside"):
        read_playback(
            timeline=timeline,
            scaling=records(),
            run_id=run_id,
            bundle_digest="a" * 64,
            tick=101,
            server_offset=0,
            request_kind="pending",
            server_id=None,
            request_offset=0,
        )


def test_playback_reports_membership_changes_and_decision_activation_without_reexecution() -> None:
    first, second = frame(tick=0, duration=1), frame(tick=1, duration=1)
    # Keep IDs canonical while changing the last member in the next interval.
    members = second["membership"]
    assert isinstance(members, list)
    members[-1]["id"] = "s64"
    samples = second["servers"]
    assert isinstance(samples, list)
    samples[-1]["server_id"] = "s64"
    decision = {
        "t": 0,
        "action": "scale_out",
        "trigger": ["cpu_mean_high"],
        "applied_at": 1,
        "server_ids_added": ["s64"],
        "server_ids_removed": [],
        "reason": None,
    }
    result = read_playback(
        timeline=records(first, second),
        scaling=records(decision),
        run_id=UUID(int=1),
        bundle_digest="a" * 64,
        tick=1,
        server_offset=0,
        request_kind="pending",
        server_id=None,
        request_offset=0,
    )
    assert result.membership_added == ("s64",) and result.membership_removed == ("s63",)
    assert result.decisions[0].applied_at == 1
    invalid = frame(tick=2, duration=1)
    with pytest.raises(expected_exception=OperationError, match="interval"):
        read_playback(
            timeline=records(invalid),
            scaling=records(),
            run_id=UUID(int=1),
            bundle_digest="a" * 64,
            tick=2,
            server_offset=0,
            request_kind="pending",
            server_id=None,
            request_offset=0,
        )
