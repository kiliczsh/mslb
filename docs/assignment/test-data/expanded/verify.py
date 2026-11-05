#!/usr/bin/env python3
"""Read-only fixture checks; optionally exercise the existing engine's tick method offline."""

import argparse
import ast
import csv
import hashlib
import json
import math
import os
import runpy
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional

ROOT = Path(__file__).resolve().parent
HEADER = ["t", "request_id", "work_units", "mem_mb"]


class InvalidFixture(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def parse_requests(path):
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f, strict=True)
        header = next(reader, None)
        if header != HEADER:
            raise InvalidFixture("columns")
        rows, seen = [], set()
        for row in reader:
            if len(row) != 4:
                raise InvalidFixture("row_width")
            rid = row[1].strip()
            if not rid:
                raise InvalidFixture("id")
            if rid in seen:
                raise InvalidFixture("duplicate_id")
            seen.add(rid)
            try:
                t, work, mem = (int(row[i]) for i in [0, 2, 3])
            except ValueError:
                raise InvalidFixture("integer") from None
            if t < 0:
                raise InvalidFixture("arrival")
            if work <= 0:
                raise InvalidFixture("work")
            if mem <= 0:
                raise InvalidFixture("memory")
            rows.append(dict(t=t, request_id=rid, work_units=work, mem_mb=mem))
        if not rows:
            raise InvalidFixture("empty")
        return rows


def parse_servers(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("tick_seconds") != 1:
        raise InvalidFixture("tick_seconds")
    if not data.get("servers"):
        raise InvalidFixture("empty_servers")
    seen = set()
    for s in data["servers"]:
        if not isinstance(s.get("id"), str) or not s["id"].strip():
            raise InvalidFixture("server_id")
        if s["id"].strip() in seen:
            raise InvalidFixture("duplicate_server")
        seen.add(s["id"].strip())
        for key in ["cpu_units_per_tick", "mem_mb", "rate_limit_per_sec"]:
            if type(s.get(key)) is not int or s[key] <= 0:
                raise InvalidFixture("server_value")
    return data["servers"]


def load_engine(engine_root):
    """Compile actual source definitions without importing database/Redis/fcntl modules.

    No production source is rewritten. run(), checkpoint and API endpoints are not used.
    The constructor and _process_tick method remain the original source code.
    """
    base = engine_root/"worker"/"engine"
    namespace = runpy.run_path(str(base/"models.py"))
    namespace.update(dict(os=os, Dict=Dict, List=List, Optional=Optional,
                          Session=object, EventWriter=object, AutoscaleEvaluator=object))

    def read_csv(data_path):
        return [namespace["RawRequest"](**row) for row in parse_requests(Path(data_path)/"requests.csv")]

    namespace["load_requests_from_file"] = read_csv
    for filename, definition in [("strategies.py", "sort_servers"), ("simulator.py", "SimulationEngine")]:
        path = base/filename
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        selected = [node for node in tree.body if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name == definition]
        require(len(selected) == 1, f"Cannot locate {definition} in {path}")
        module = ast.Module(body=selected, type_ignores=[])
        exec(compile(module, str(path), "exec"), namespace)
    return namespace["SimulationEngine"]


class Recorder:
    def __init__(self, rows, servers, config):
        self.requests = {r["request_id"]: r for r in rows}
        self.servers = {s["id"]: s for s in servers}
        self.config = config
        self.events = []
        self.arrived, self.terminal = set(), set()
        self.active, self.started, self.finished, self.dropped = {}, {}, {}, {}
        self.memory = Counter()
        self.start_counts = Counter()

    def write(self, event):
        rid, t, kind = event["request_id"], event["t"], event["event"]
        require(rid in self.requests, f"Unknown request: {rid}")
        require(not self.events or t >= self.events[-1]["t"], "Events are not chronological")
        spec = self.requests[rid]
        if kind == "REQUEST_ARRIVED":
            require(rid not in self.arrived and t == spec["t"], f"Bad arrival: {rid}")
            self.arrived.add(rid)
        elif kind == "REQUEST_STARTED":
            sid = event["server_id"]
            require(rid in self.arrived and rid not in self.started and rid not in self.terminal, f"Bad start: {rid}")
            require(sid in self.servers, f"Unknown server: {sid}")
            require(t >= spec["t"], f"Start before arrival: {rid}")
            if self.config["execution_mode"] == "sequential":
                require(sid not in self.active.values(), f"Sequential overlap: {sid}")
            self.active[rid] = sid
            self.started[rid] = event
            self.memory[sid] += spec["mem_mb"]
            self.start_counts[t, sid] += 1
            require(self.memory[sid] <= self.servers[sid]["mem_mb"], f"Memory exceeded: {sid}")
            require(self.start_counts[t, sid] <= self.servers[sid]["rate_limit_per_sec"], f"Rate exceeded: {sid}")
        elif kind == "REQUEST_FINISHED":
            sid = event["server_id"]
            require(self.active.get(rid) == sid and rid not in self.terminal, f"Bad finish: {rid}")
            start = self.started[rid]["t"]
            lower_bound = math.ceil(spec["work_units"]/self.servers[sid]["cpu_units_per_tick"])
            require(t-start >= lower_bound, f"Finish earlier than CPU bound: {rid}")
            if self.config["execution_mode"] == "sequential":
                require(t-start == lower_bound, f"Sequential duration differs: {rid}")
            del self.active[rid]
            self.memory[sid] -= spec["mem_mb"]
            self.finished[rid] = event
            self.terminal.add(rid)
        elif kind == "REQUEST_DROPPED":
            require(rid in self.arrived and rid not in self.started and rid not in self.terminal, f"Bad drop: {rid}")
            if event.get("reason") == "no_server_can_fit":
                require(spec["mem_mb"] > max(s["mem_mb"] for s in self.servers.values()), f"Incorrect no-fit: {rid}")
                require(t == spec["t"], f"No-fit drop delayed: {rid}")
            elif event.get("reason") == "queue_timeout":
                timeout = self.config["queue_timeout_ticks"]
                require(timeout > 0 and t-spec["t"] == timeout, f"Incorrect timeout: {rid}")
            else:
                raise AssertionError(f"Unknown drop reason: {event}")
            self.dropped[rid] = event
            self.terminal.add(rid)
        else:
            raise AssertionError(f"Unexpected event: {kind}")
        self.events.append(event)


def execute_case(engine_class, folder, config_name="config.json", expected_name="expected.json"):
    rows, servers = parse_requests(folder/"requests.csv"), parse_servers(folder/"servers.json")
    config = json.loads((folder/config_name).read_text(encoding="utf-8"))
    expected = json.loads((folder/expected_name).read_text(encoding="utf-8"))
    engine = engine_class("offline-fixture-check", servers, str(folder), None,
                          strategy=config["scheduling_strategy"], queue_timeout_ticks=config["queue_timeout_ticks"],
                          execution_mode=config["execution_mode"])
    writer = Recorder(rows, servers, config)
    last_arrival = max(r["t"] for r in rows)
    # Finite guard even if a regression prevents progress.
    limit = last_arrival + sum(r["work_units"] for r in rows)//min(s["cpu_units_per_tick"] for s in servers) + len(rows) + 100
    pending_at_recovery = None
    for tick in range(limit+1):
        engine._process_tick(tick, writer)
        if folder.name == "22_overload_recovery" and tick == 157:
            pending_at_recovery = len(engine.pending_queue) + sum(len(s.active_requests) for s in engine.server_states.values())
        if tick >= last_arrival and not engine.pending_queue and not any(s.active_requests for s in engine.server_states.values()):
            break
    else:
        raise AssertionError(f"Tick limit exceeded: {folder.name}")

    counts = dict(arrived=len(writer.arrived), completed=len(writer.finished), dropped=len(writer.dropped))
    require(counts == expected["terminal_counts"], f"Terminal counts mismatch: {folder.name}: {counts}")
    require(writer.terminal == set(writer.requests) and not writer.active, f"Unfinished request: {folder.name}")
    require(not any(writer.memory.values()), "Reserved memory leaked")
    for rid, point in expected["exact_lifecycle"].items():
        for key, collection in [("start", writer.started), ("finish", writer.finished), ("drop", writer.dropped)]:
            if key in point:
                require(rid in collection and collection[rid]["t"] == point[key],
                        f"{folder.name}: {rid} expected {key}={point[key]}, got {collection.get(rid)}")
        if "server" in point:
            require(writer.started[rid]["server_id"] == point["server"], f"Wrong server: {rid}")
        if "reason" in point:
            require(writer.dropped[rid]["reason"] == point["reason"], f"Wrong reason: {rid}")
    if pending_at_recovery is not None:
        require(pending_at_recovery == 0, "Overload did not clear before recovery traffic")
    if folder.name == "15_finish_arrive_collision":
        events_at_two = [e["event"] for e in writer.events if e["t"] == 2]
        require(events_at_two == ["REQUEST_FINISHED"]*3+["REQUEST_ARRIVED"]*3+["REQUEST_STARTED"]*3,
                "Finish/arrival/start order changed")

    # Project to the assignment's lifecycle fields: ancillary numeric formatting is irrelevant.
    trace = [{k: e[k] for k in ["t", "event", "request_id", "server_id", "reason"] if k in e} for e in writer.events]
    digest = hashlib.sha256(json.dumps(trace, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()
    return dict(case=folder.name, config=config_name, **counts, last_event_tick=tick,
                event_count=len(trace), checked_exact_lifecycles=len(expected["exact_lifecycle"]),
                lifecycle_sha256=digest)


def verify(root, engine_root=None):
    manifest = json.loads((root/"manifest.json").read_text(encoding="utf-8"))
    for relative, expected in manifest["sha256"].items():
        actual = hashlib.sha256((root/relative).read_bytes()).hexdigest()
        require(actual == expected, f"Changed generated file: {relative}")
    total = 0
    for case in manifest["scenarios"]:
        folder = root/"scenarios"/case["id"]
        rows, servers = parse_requests(folder/"requests.csv"), parse_servers(folder/"servers.json")
        arrivals = Counter(row["t"] for row in rows)
        require(len(rows) == case["request_count"], f"Wrong count: {case['id']}")
        require(max(arrivals.values()) == case["peak_arrivals_per_tick"], f"Wrong peak: {case['id']}")
        require(sum(r["work_units"] for r in rows) == case["total_work_units"], f"Wrong workload: {case['id']}")
        require(len(servers) == case["server_count"], f"Wrong server count: {case['id']}")
        for phase in case["phases"]:
            for tick in range(phase["start"], phase["end_exclusive"]):
                require(arrivals[tick] == phase["arrivals_per_tick"], f"Wrong phase count: {case['id']} tick={tick}")
        total += len(rows)
    require(total == manifest["total_request_rows"], "Wrong total count")
    negative = json.loads((root/"invalid/expected_rejections.json").read_text(encoding="utf-8"))["cases"]
    for case in negative:
        try:
            (parse_requests if case["kind"] == "csv" else parse_servers)(root/case["path"])
        except InvalidFixture as exc:
            require(str(exc) == case["expected_error"], f"Wrong rejection: {case['id']}: {exc}")
        else:
            raise AssertionError(f"Invalid fixture accepted by suite checker: {case['id']}")

    report = dict(status="passed", fixture_scenarios=len(manifest["scenarios"]), request_rows=total,
                  rejected_by_suite_contract=len(negative), generated_files_hashed=len(manifest["sha256"]),
                  engine_runs=[], limitations=[
                      "Negative cases checked by this suite's strict contract, not the live API.",
                      "No Docker, database, Redis, HTTP, checkpoint persistence or crash/resume verification.",
                      "Concurrent CPU timing is checked at manual checkpoints and a lower bound; not a complete independent CPU replay.",
                  ])
    if engine_root:
        engine_class = load_engine(engine_root)
        for case in manifest["scenarios"]:
            folder = root/"scenarios"/case["id"]
            report["engine_runs"].append(execute_case(engine_class, folder))
            for variant in case["variants"]:
                report["engine_runs"].append(execute_case(engine_class, folder, f"config.{variant}.json", f"expected.{variant}.json"))
        indexed = {r["case"]: r for r in report["engine_runs"] if r["config"] == "config.json"}
        require(indexed["07_lexical_ties"]["lifecycle_sha256"] == indexed["08_shuffled_input"]["lifecycle_sha256"],
                "Permutation changed trace")
        repeated = execute_case(engine_class, root/"scenarios/07_lexical_ties")
        require(repeated == indexed["07_lexical_ties"], "Repeated input changed output")
        report["determinism_checks"] = ["input_and_server_permutation", "identical_repeat"]
        report["engine_scope"] = "Original constructor and _process_tick compiled from AST; local in-memory event sink. run() not called."
        report["engine_source_sha256"] = {name: hashlib.sha256((engine_root/"worker/engine"/name).read_bytes()).hexdigest()
                                            for name in ["models.py", "strategies.py", "simulator.py"]}
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--engine", type=Path, help="Existing sample application's root. Read-only offline tick checks.")
    parser.add_argument("--report", type=Path, help="Optional JSON report output; no files are written without this option.")
    args = parser.parse_args()
    result = verify(args.root.resolve(), args.engine.resolve() if args.engine else None)
    if args.report:
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2)+"\n", encoding="utf-8", newline="\n")
    print(json.dumps({k: v for k, v in result.items() if k not in ["engine_runs", "engine_source_sha256"]}, ensure_ascii=False))
    print(f"Offline engine configurations checked: {len(result['engine_runs'])}")
