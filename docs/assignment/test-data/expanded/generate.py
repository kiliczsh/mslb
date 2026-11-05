#!/usr/bin/env python3
"""Generate isolated mslb fixtures. Python standard library only."""

import argparse
import csv
import hashlib
import io
import json
import random
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SEED = 20261105
HEADER = ["t", "request_id", "work_units", "mem_mb"]
BASE_CONFIG = {
    "scheduling_strategy": "first_fit",
    "execution_mode": "concurrent",
    "queue_timeout_ticks": 0,
    "real_time_ticks": False,
    "autoscale_enabled": False,
}


def server(name, cpu, memory, rate):
    return dict(id=name, cpu_units_per_tick=cpu, mem_mb=memory, rate_limit_per_sec=rate)


def request(t, name, work, memory):
    return [t, name, work, memory]


def csv_text(rows, header=HEADER):
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return stream.getvalue()


def scenarios():
    cases = []

    def add(slug, title, purpose, servers, rows, checks, *, dropped=0, config=None,
            exact=None, variants=None, phases=None, relation=None):
        cases.append(dict(
            id=slug, title=title, purpose=purpose, servers=servers, rows=rows,
            checks=checks, config={**BASE_CONFIG, **(config or {})},
            expected=dict(arrived=len(rows), completed=len(rows)-dropped, dropped=dropped),
            exact_lifecycle=exact or {}, variants=variants or {}, phases=phases or [],
            relation=relation,
        ))

    add("01_idle_gaps", "Traffic resuming after idle gaps",
        "The first request arrives late; the simulation must continue across long idle intervals.",
        [server("idle-a", 8, 256, 2)],
        [request(t, f"idle-{i:02d}", 8, 64) for i, t in enumerate([5, 6, 25, 26, 90, 91, 250, 251], 1)],
        ["The first event must occur at t=5.", "The final request must finish at t=252.", "An empty queue must not cancel future arrivals."],
        exact={f"idle-{i:02d}": dict(start=t, finish=t+1, server="idle-a")
               for i, t in enumerate([5, 6, 25, 26, 90, 91, 250, 251], 1)})

    add("02_exact_memory_release", "Reusing fully occupied memory within the same tick",
        "Two 128 MB reservations fill the 256 MB capacity exactly; completions release memory before new assignments.",
        [server("memory-a", 8, 256, 4)],
        [[0, "a-half", 8, 128], [0, "b-half", 8, 128], [1, "c-full", 8, 256], [2, "d-small", 8, 1]],
        ["The first two requests finish at t=2.", "c-full can start at t=2; d-small waits until t=3."],
        exact={"a-half": dict(start=0, finish=2), "b-half": dict(start=0, finish=2),
               "c-full": dict(start=2, finish=3), "d-small": dict(start=3, finish=4)})

    add("03_fragmented_memory", "Enough aggregate free memory, insufficient memory on each server",
        "Each of two servers has 256 MB free. A single 384 MB request cannot combine these separate capacities.",
        [server("frag-a", 8, 512, 4), server("frag-b", 8, 512, 4)],
        [[0, "a-anchor", 32, 256], [0, "b-anchor", 32, 256],
         [1, "c-large", 8, 384], [1, "d-small", 4, 128]],
        ["c-large cannot start at t=1.", "The smaller d-small request can start while c-large remains pending.",
         "A request's memory must not be split across servers."],
        config={"scheduling_strategy": "least_connections"},
        exact={"c-large": dict(start=4, finish=5, server="frag-b"),
               "d-small": dict(start=1, finish=2, server="frag-a")})

    add("04_rate_window_reset", "Resetting the admission limit each tick",
        "With ample CPU and memory, only the limit of three starts per tick creates a queue.",
        [server("rate-a", 120, 4096, 3)],
        [[0 if i < 7 else 1, f"rate-{i:02d}", 1, 1] for i in range(12)],
        ["There must be exactly three REQUEST_STARTED events at each of t=0,1,2,3.", "All requests must finish by t=4."],
        exact={f"rate-{i:02d}": dict(start=i//3, finish=i//3+1) for i in range(12)})

    add("05_equal_cpu_sharing", "Equal CPU sharing as the active request count changes",
        "Twelve CPU units are shared first among three requests, then two, then one.",
        [server("share-a", 12, 512, 8)],
        [[0, "share-a", 4, 64], [0, "share-b", 8, 64], [0, "share-c", 12, 64]],
        ["Completion ticks must be 1, 2, and 3 respectively.", "Released CPU capacity must be shared among the remaining requests on the next tick."],
        exact={"share-a": dict(start=0, finish=1), "share-b": dict(start=0, finish=2),
               "share-c": dict(start=0, finish=3)})

    add("06_midflight_arrival", "A new request joining an active request",
        "A short request arriving while a long request is running joins CPU sharing from its arrival tick.",
        [server("join-a", 12, 256, 4)], [[0, "long", 24, 64], [1, "short", 6, 64]],
        ["short must finish at t=2 and long at t=3.", "The long request's completion time must not be fixed at its start."],
        exact={"long": dict(start=0, finish=3), "short": dict(start=1, finish=2)})

    tie_servers = [server(name, 16, 256, 2) for name in ["node-2", "node-10", "node-1"]]
    tie_rows = [[i//6, name, 8+(i%3)*4, 64] for i, name in enumerate(
        ["job-1", "job-10", "job-2", "job-20", "job-A", "job-a", "job-03", "job-3", "job-Z", "job-z", "job-11", "job-9"])]
    add("07_lexical_ties", "Lexicographic tie-breaking",
        "Numeric-looking identifiers and mixed letter cases distinguish natural numeric order from lexicographic order.",
        tie_servers, sorted(tie_rows, key=lambda r: (r[0], r[1])),
        ["Use the lexicographic order node-1, node-10, node-2.", "The event trace must match scenario 08."],
        config={"scheduling_strategy": "most_available_memory"},
        relation={"same_trace_as": "08_shuffled_input"})
    shuffled = list(tie_rows)
    random.Random(SEED+8).shuffle(shuffled)
    add("08_shuffled_input", "Independence from input row and server order",
        "The same records as scenario 07 are provided in a different CSV row order and server order.",
        list(reversed(tie_servers)), shuffled,
        ["CSV rows do not have to be in chronological order.", "Events and assignments must match scenario 07 exactly."],
        config={"scheduling_strategy": "most_available_memory"},
        relation={"same_trace_as": "07_lexical_ties"})

    heterogeneous = [server("compute", 24, 256, 6), server("general", 8, 512, 3), server("memory", 4, 4096, 2)]
    add("09_heterogeneous_eligibility", "Servers with different CPU and memory capacities",
        "A fast server with little memory works alongside a slower server with more memory.",
        heterogeneous, [[i//6, f"hetero-{i:03d}", [4, 20, 36, 8][i%4], [64, 256, 512, 768, 2048, 4096][i%6]] for i in range(36)],
        ["Requests larger than 512 MB can only be assigned to the memory server.",
         "A request of exactly 4096 MB must be accepted.", "Compare both completion time and queue time when evaluating strategies."],
        config={"scheduling_strategy": "most_available_memory"})

    add("10_memory_boundary", "One below, exactly at, and one above the memory limit",
        "Requests of 1023 or 1024 MB fit; 1025 MB cannot fit on any available server.",
        [server("bound-a", 16, 1024, 4)],
        [[i*3, f"bound-{i}", 16, mem] for i, mem in enumerate([1023, 1024, 1025, 1, 1024, 1025, 512])],
        ["Only bound-2 and bound-5 must be dropped with no_server_can_fit.", "A request matching the capacity exactly must not be rejected."],
        dropped=2, exact={"bound-2": dict(drop=6, reason="no_server_can_fit"),
                          "bound-5": dict(drop=15, reason="no_server_can_fit")})

    add("11_all_unschedulable", "Every request exceeds available capacity",
        "The simulation must terminate even when no request can be started.",
        [server("small-a", 8, 512, 2), server("small-b", 16, 1024, 4)],
        [[i//2, f"nofit-{i}", 8+i, 1025+i*128] for i in range(8)],
        ["Expect eight arrivals, eight drops, zero starts, and zero completions.", "The final terminal event must occur at t=3."], dropped=8)

    add("12_timeout_boundary", "Queue timeout coinciding with memory release",
        "A request whose wait reaches the timeout is dropped before assignment, even if memory is released during the same tick.",
        [server("timeout-a", 8, 128, 1)],
        [[0, "a-running", 32, 128], [0, "b-expire", 8, 128], [1, "c-exact", 8, 128],
         [2, "d-survive", 8, 128], [7, "e-recover", 8, 128]],
        ["b-expire must be dropped at t=3 and c-exact at t=4, both with queue_timeout.",
         "d-survive must start at t=4 and finish at t=5.", "Queue timeout must not interrupt an active request."],
        dropped=2, config={"queue_timeout_ticks": 3},
        exact={"a-running": dict(start=0, finish=4), "b-expire": dict(drop=3, reason="queue_timeout"),
               "c-exact": dict(drop=4, reason="queue_timeout"), "d-survive": dict(start=4, finish=5),
               "e-recover": dict(start=7, finish=8)})

    add("13_pending_bypass", "Starting a smaller request behind a request that does not fit",
        "Stopping the queue scan at the first request that does not fit would leave usable capacity idle.",
        [server("bypass-a", 8, 256, 4)],
        [[0, "a-anchor", 32, 128], [1, "b-large", 8, 256], [1, "c-small", 4, 64], [2, "d-small", 4, 64]],
        ["c-small and d-small must be able to start before b-large.", "b-large must eventually complete without starvation."],
        exact={"c-small": dict(start=1, finish=2), "d-small": dict(start=2, finish=3),
               "b-large": dict(start=5, finish=6)})

    rounding_work = [1, 7, 8, 13, 14, 15]
    add("14_tick_rounding", "Rounding execution time to tick boundaries",
        "For a single active request, duration is ceil(work/7); completion must not be emitted on the start tick.",
        [server("round-a", 7, 256, 4)],
        [[i*10, f"round-{i}", work, 64] for i, work in enumerate(rounding_work)],
        ["Completion ticks must be 1, 11, 22, 32, 42, and 53."],
        exact={f"round-{i}": dict(start=i*10, finish=i*10+(work+6)//7) for i, work in enumerate(rounding_work)})

    add("15_finish_arrive_collision", "Multiple completions and new arrivals on the same tick",
        "Three different servers become free simultaneously, allowing the second wave to start on that same tick.",
        [server("finish-a", 8, 128, 2), server("finish-b", 12, 128, 2), server("finish-c", 16, 128, 2)],
        [[0, "a-first", 16, 128], [0, "b-first", 24, 128], [0, "c-first", 32, 128],
         [2, "d-next", 8, 128], [2, "e-next", 12, 128], [2, "f-next", 16, 128]],
        ["The first three completions must occur at t=2.", "The second wave must start at t=2 and finish at t=3.",
         "At t=2, completion events must be written before arrival events."],
        exact={name: dict(start=0 if i<3 else 2, finish=2 if i<3 else 3)
               for i, name in enumerate(["a-first", "b-first", "c-first", "d-next", "e-next", "f-next"])})

    add("16_execution_modes", "Comparing concurrent and sequential execution of the same workload",
        "Four equal requests run simultaneously with CPU sharing, or one at a time in sequential mode.",
        [server("mode-a", 8, 1024, 8)], [[0, f"mode-{i}", 8, 128] for i in range(4)],
        ["Concurrent: four starts at t=0 and four completions at t=4.", "Sequential: starts at 0,1,2,3; completions at 1,2,3,4."],
        exact={f"mode-{i}": dict(start=0, finish=4) for i in range(4)},
        variants={"sequential": dict(config={**BASE_CONFIG, "execution_mode": "sequential"},
                   exact_lifecycle={f"mode-{i}": dict(start=i, finish=i+1) for i in range(4)})})

    fleet = [server(f"fleet-{i:02d}", 64, 4096, 8) for i in range(4)]

    def traffic(slug, phases, seed, work_choices, mem_choices):
        rng = random.Random(seed)
        rows = []
        for phase in phases:
            for t in range(phase["start"], phase["end_exclusive"]):
                for _ in range(phase["arrivals_per_tick"]):
                    rows.append([t, f"{slug}-{len(rows):06d}", rng.choice(work_choices), rng.choice(mem_choices)])
        return rows

    ramp = [dict(label=label, start=i*30, end_exclusive=(i+1)*30, arrivals_per_tick=n)
            for i, (label, n) in enumerate([("warm-up", 2), ("ramp-up", 8), ("peak", 20), ("ramp-down", 8), ("normal", 2)])]
    add("17_ramp_up_down", "Gradually increasing and decreasing traffic",
        "Five phases show increasing and decreasing load on the same server cluster.", fleet,
        traffic("ramp", ramp, SEED+17, [4, 8, 16, 32], [32, 64, 128, 256]),
        ["Arrival counts for the five phases must be 60,240,600,240,60.", "All requests must complete with queue timeout disabled."],
        phases=ramp, config={"scheduling_strategy": "most_available_memory"})

    pulse_rows = []
    for t in range(180):
        for j in range(81 if t % 30 == 0 else 1):
            pulse_rows.append([t, f"pulse-{len(pulse_rows):05d}", [4, 8, 16][j%3], [32, 128, 256][j%3]])
    add("18_periodic_spikes", "Periodic traffic spikes",
        "A baseline of one request per tick is supplemented by 80 extra requests every 30 ticks.", fleet, pulse_rows,
        ["There must be 81 arrivals at each of ticks 0,30,60,90,120,150.", "Measure backlog and recovery between the six waves."],
        config={"scheduling_strategy": "least_connections"})

    rng = random.Random(SEED+19)
    tail = [[i//12, f"tail-{i:05d}", 1200 if i%37 == 0 else rng.choice([2, 4, 8, 16]),
             3072 if i%41 == 0 else rng.choice([32, 64, 128])] for i in range(1500)]
    add("19_long_tail_mix", "Short requests mixed with a few very long requests",
        "Forty-one long CPU jobs and 37 high-memory jobs affect the latency distribution of short requests.", fleet, tail,
        ["Inspect p95/p99 queue and completion times as well as averages.",
         "Short requests must not be lost while long jobs run; every request must have a terminal event."],
        config={"scheduling_strategy": "least_connections"})

    soak = [dict(label="steady traffic", start=0, end_exclusive=1200, arrivals_per_tick=10)]
    add("20_sustained_12k", "Sustained traffic: 12,000 requests",
        "Ten requests per tick for 1,200 ticks exercise state accumulation during long runs.", fleet,
        traffic("soak", soak, SEED+20, [4, 8, 12, 24], [32, 64, 128, 256]),
        ["Expect 12,000 unique requests and 36,000 lifecycle events.",
         "The active request set and pending queue must be empty at the end of the run."], phases=soak,
        config={"scheduling_strategy": "round_robin"})

    stress = [dict(label="high volume", start=0, end_exclusive=250, arrivals_per_tick=200)]
    add("21_stress_50k", "High volume: 50,000 requests",
        "Intensive, variable CPU and memory demand across 16 servers over 250 ticks.",
        [server(f"stress-{i:02d}", 128, 8192, 16) for i in range(16)],
        traffic("stress", stress, SEED+21, [1, 4, 8, 16, 32], [16, 64, 128, 256]),
        ["Expect 50,000 unique requests and 150,000 lifecycle events.",
         "Use real_time_ticks=false when measuring performance.", "Wall-clock performance depends on the machine; no fixed runtime is claimed."],
        phases=stress, config={"scheduling_strategy": "most_available_memory"})

    recovery = [dict(label="warm-up", start=0, end_exclusive=30, arrivals_per_tick=2),
                dict(label="overload", start=30, end_exclusive=38, arrivals_per_tick=200),
                dict(label="idle recovery", start=38, end_exclusive=158, arrivals_per_tick=0),
                dict(label="normal traffic", start=158, end_exclusive=178, arrivals_per_tick=2)]
    add("22_overload_recovery", "Overload, idle recovery, and a return to normal traffic",
        "A short intense wave creates a queue; new traffic arrives after 120 idle ticks.", fleet,
        traffic("recovery", recovery, SEED+22, [4, 8, 16], [64, 128, 256]),
        ["There must be 1,700 requests in total.", "The queue must drain during the idle interval.",
         "New traffic at t=158 must not inherit active or pending requests from the previous wave."], phases=recovery)

    late_ticks = [0, 1, 49, 50, 51, 99, 100, 101, 1000, 10000]
    add("23_sparse_checkpoint_ticks", "Sparse arrivals and checkpoint boundaries",
        "Requests arrive around ticks 49/50/51 and 99/100/101, as well as much later.",
        [server("sparse-a", 8, 256, 4)], [[t, f"sparse-{i:02d}", 8, 64] for i, t in enumerate(late_ticks)],
        ["The final completion must occur at t=10001.", "This dataset covers checkpoint boundaries; crash/resume behavior requires a separate integration test."],
        exact={f"sparse-{i:02d}": dict(start=t, finish=t+1) for i, t in enumerate(late_ticks)})

    add("24_quoted_unicode_ids", "CSV quoting and Unicode identifiers",
        "Commas, double quotes, Unicode symbols, and leading zeros are part of request identifiers.",
        [server("encoding-a", 16, 512, 8)],
        [[i*2, name, 16, 64] for i, name in enumerate(["0007", "request,comma", 'request-"quoted"', "request-☃", "request-★", "req-α"])],
        ["Read CSV with a standard parser rather than splitting lines on commas.",
         "Preserve identifiers without numeric conversion or character loss."],
        exact={name: dict(start=i*2, finish=i*2+1) for i, name in enumerate(["0007", "request,comma", 'request-"quoted"', "request-☃", "request-★", "req-α"])})
    return cases


def negative_cases():
    h = ",".join(HEADER)+"\n"
    cases = []

    def csv_case(name, content, error, reason):
        cases.append(dict(id=name, kind="csv", content=content, expected_error=error, reason=reason))

    csv_case("01_missing_memory_column", "t,request_id,work_units\n0,missing,8\n", "columns", "The required mem_mb column is missing.")
    csv_case("02_wrong_delimiter", "t;request_id;work_units;mem_mb\n0;delimiter;8;64\n", "columns", "The delimiter must be a comma.")
    csv_case("03_header_only", h, "empty", "There are no request rows after the header.")
    csv_case("04_empty_file", "", "columns", "Both the header and data rows are missing.")
    csv_case("05_non_numeric_tick", h+"tomorrow,bad-tick,8,64\n", "integer", "The arrival tick must be an integer.")
    csv_case("06_fractional_tick", h+"1.5,fractional-tick,8,64\n", "integer", "Fractional ticks are not supported.")
    csv_case("07_negative_tick", h+"-1,negative-tick,8,64\n", "arrival", "The arrival tick must not be negative.")
    csv_case("08_negative_work", h+"0,negative-work,-8,64\n", "work", "Work units must be positive.")
    csv_case("09_zero_work", h+"0,zero-work,0,64\n", "work", "This suite's contract rejects zero work units.")
    csv_case("10_negative_memory", h+"0,negative-memory,8,-64\n", "memory", "Memory must not be negative.")
    csv_case("11_zero_memory", h+"0,zero-memory,8,0\n", "memory", "This suite's contract requires positive memory.")
    csv_case("12_nan_work", h+"0,nan-work,NaN,64\n", "integer", "NaN is not a valid workload.")
    csv_case("13_infinite_work", h+"0,infinite-work,Infinity,64\n", "integer", "An infinite workload cannot complete.")
    csv_case("14_nan_memory", h+"0,nan-memory,8,NaN\n", "integer", "NaN is not a valid memory value.")
    csv_case("15_infinite_memory", h+"0,infinite-memory,8,inf\n", "integer", "Infinite memory is not a valid value.")
    csv_case("16_duplicate_id", h+"0,duplicate,8,64\n1,duplicate,16,128\n", "duplicate_id", "Request identifiers must be unique throughout the file.")
    csv_case("17_duplicate_after_trim", h+"0,trim-id,8,64\n1, trim-id ,16,128\n", "duplicate_id", "Identifiers collide after whitespace is stripped.")
    csv_case("18_blank_id", h+"0,,8,64\n", "id", "The request identifier must not be empty.")
    csv_case("19_whitespace_id", h+'0,"   ",8,64\n', "id", "An identifier containing only whitespace is invalid.")
    csv_case("20_missing_numeric_value", h+"0,missing-work,,64\n", "integer", "The work_units value is missing.")
    csv_case("21_short_row", h+"0,short-row,8\n", "row_width", "The fourth field is missing from the row.")
    csv_case("22_extra_field", h+"0,extra-field,8,64,unexpected\n", "row_width", "The row contains more fields than the header.")
    csv_case("23_duplicate_header", "t,request_id,work_units,mem_mb,mem_mb\n0,duplicate-header,8,64,128\n", "columns", "A repeated column name makes the input ambiguous.")
    csv_case("24_fractional_work", h+"0,fractional-work,0.5,64\n", "integer", "This suite uses integer work units for compatibility with the original validator.")

    valid = server("invalid-a", 8, 512, 4)
    bad_servers = [
        ("25_empty_cluster", [], 1, "empty_servers", "At least one server is required."),
        ("26_duplicate_server_id", [valid, valid], 1, "duplicate_server", "Server identifiers must be unique."),
        ("27_zero_cpu", [{**valid, "cpu_units_per_tick": 0}], 1, "server_value", "CPU capacity must be positive."),
        ("28_negative_memory", [{**valid, "mem_mb": -512}], 1, "server_value", "Memory capacity must be positive."),
        ("29_zero_rate", [{**valid, "rate_limit_per_sec": 0}], 1, "server_value", "The admission limit must be positive."),
        ("30_fractional_rate", [{**valid, "rate_limit_per_sec": 1.5}], 1, "server_value", "The admission limit must be an integer."),
        ("31_unsupported_tick", [valid], 2, "tick_seconds", "This suite and the original validator use one-second ticks."),
        ("32_blank_server_id", [{**valid, "id": " "}], 1, "server_id", "The server identifier must not be empty."),
    ]
    for name, servers, tick, error, reason in bad_servers:
        cases.append(dict(id=name, kind="servers", content=dict(tick_seconds=tick, servers=servers),
                          expected_error=error, reason=reason))
    return cases


def generate(out):
    out.mkdir(parents=True, exist_ok=True)
    hashes = {}

    def write(relative, content):
        path = out/relative
        # Only files under this selected output folder may be authored.
        if not path.resolve().is_relative_to(out.resolve()):
            raise ValueError("Output escaped target folder")
        path.parent.mkdir(parents=True, exist_ok=True)
        data = content.encode("utf-8")
        path.write_bytes(data)
        hashes[relative] = hashlib.sha256(data).hexdigest()

    def write_json(relative, obj):
        write(relative, json.dumps(obj, ensure_ascii=False, indent=2)+"\n")

    catalog = []
    for case in scenarios():
        prefix = f"scenarios/{case['id']}"
        rows = case["rows"]
        arrivals = Counter(r[0] for r in rows)
        stats = dict(request_count=len(rows), arrival_tick_min=min(arrivals), arrival_tick_max=max(arrivals),
                     peak_arrivals_per_tick=max(arrivals.values()), total_work_units=sum(r[2] for r in rows),
                     work_units_min=min(r[2] for r in rows), work_units_max=max(r[2] for r in rows),
                     mem_mb_min=min(r[3] for r in rows), mem_mb_max=max(r[3] for r in rows),
                     server_count=len(case["servers"]))
        write(prefix+"/requests.csv", csv_text(rows))
        write_json(prefix+"/servers.json", dict(tick_seconds=1, servers=case["servers"]))
        write_json(prefix+"/config.json", case["config"])
        expected = dict(basis="documented_contract_and_manual_checkpoints_not_a_production_run",
                        terminal_counts=case["expected"], exact_lifecycle=case["exact_lifecycle"],
                        checks=case["checks"], relation=case["relation"])
        write_json(prefix+"/expected.json", expected)
        for name, variant in case["variants"].items():
            write_json(prefix+f"/config.{name}.json", variant["config"])
            write_json(prefix+f"/expected.{name}.json", {**expected, "exact_lifecycle": variant["exact_lifecycle"]})
        catalog.append(dict(id=case["id"], title=case["title"], purpose=case["purpose"],
                            **stats, phases=case["phases"], variants=list(case["variants"])))
        checks = "\n".join(f"- {text}" for text in case["checks"])
        server_table = "\n".join(f"| {s['id']} | {s['cpu_units_per_tick']} | {s['mem_mb']} | {s['rate_limit_per_sec']} |" for s in case["servers"])
        phase_notes = ""
        if case["phases"]:
            phase_notes = "\n## Traffic phases\n\n| Phase | Start | End (exclusive) | Requests/tick |\n|---|---:|---:|---:|\n"
            phase_notes += "\n".join(f"| {p['label']} | {p['start']} | {p['end_exclusive']} | {p['arrivals_per_tick']} |" for p in case["phases"])+"\n"
        write(prefix+"/README.md", f"""# {case['title']}

{case['purpose']}

{len(rows):,} requests; arrival ticks {stats['arrival_tick_min']}–{stats['arrival_tick_max']};
at most {stats['peak_arrivals_per_tick']} arrivals in a single tick.
Workload: {stats['work_units_min']}–{stats['work_units_max']} units;
memory: {stats['mem_mb_min']}–{stats['mem_mb_max']} MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
{server_table}

## Expected behavior

{checks}

Expected totals: {case['expected']['arrived']} arrivals,
{case['expected']['completed']} completions, and {case['expected']['dropped']} drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.
{phase_notes}
## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
""")

    negatives = []
    for case in negative_cases():
        extension = "csv" if case["kind"] == "csv" else "json"
        relative = f"invalid/{case['kind']}/{case['id']}.{extension}"
        content = case["content"] if extension == "csv" else json.dumps(case["content"], ensure_ascii=False, indent=2)+"\n"
        write(relative, content)
        negatives.append({k: case[k] for k in ["id", "kind", "expected_error", "reason"]} | {"path": relative})
    write_json("invalid/expected_rejections.json", dict(
        policy="suite_strict_integer_contract_not_a_claim_about_current_api_behavior", cases=negatives))
    manifest = dict(schema_version=1, seed=SEED, csv_columns=HEADER, encoding="UTF-8 without BOM",
                    tick_seconds=1, scenarios=catalog, scenario_count=len(catalog),
                    total_request_rows=sum(c["request_count"] for c in catalog),
                    negative_case_count=len(negatives), sha256=hashes)
    (out/"manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n", encoding="utf-8", newline="\n")
    print(json.dumps({k: manifest[k] for k in ["scenario_count", "total_request_rows", "negative_case_count"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT,
                        help="New fixture directory; default is this script's own folder.")
    args = parser.parse_args()
    generate(args.output.resolve())
