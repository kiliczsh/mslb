"""Exercise generated fixtures through an explicitly isolated real logical installation."""

import asyncio
import hashlib
import io
import json
import os
import time
from collections import Counter
from datetime import UTC, datetime
from http import HTTPStatus
from pathlib import Path
from urllib.parse import quote
from uuid import UUID

import httpx
from mslb_application.artifacts import ARTIFACT_NAMES, verify_bundle
from mslb_contracts.capacity_profiles import parse_capacity_profile, request_limit
from mslb_contracts.inputs import InputError, read_servers

FIXTURES = Path("/fixtures/expanded")
OUTPUT = Path("/data/exports")


def require(*, condition: bool, message: str) -> None:
    """Stop acceptance on a mismatch rather than turn missing evidence into success."""
    if not condition:
        raise RuntimeError(message)


def save(*, path: Path, value: object) -> None:
    """Write bounded readable provenance outside strict generated bundle directories."""
    path.write_text(
        data=json.dumps(obj=value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def source_provenance() -> dict[str, str]:
    """Identify actual source bytes embedded in the immutable acceptance image."""
    paths = [Path("scripts/check_scenarios.py")]
    for root in (Path("packages"), Path("apps/api/src"), Path("apps/worker/src")):
        paths.extend(sorted(root.rglob(pattern="*.py")))
    require(condition=len(paths) <= 512, message="Source inventory exceeds its bound")
    return {path.as_posix(): hashlib.sha256(data=path.read_bytes()).hexdigest() for path in paths}


async def installation(*, client: httpx.AsyncClient, profile: str, identity: str) -> None:
    """Verify fixed private DNS, UUID/profile marker and an unused isolated catalog."""
    response = await client.get(url="health/live")
    response.raise_for_status()
    require(
        condition=response.json()
        == {"live": True, "capacity_profile": profile, "isolation_id": identity},
        message="Refusing a different or unmarked installation",
    )
    for collection in ("datasets", "simulations"):
        response = await client.get(url=collection)
        response.raise_for_status()
        require(condition=response.json()["items"] == [], message="Installation is not fresh")


async def replace_servers(*, client: httpx.AsyncClient, source: Path) -> None:
    """Replace only this already-verified disposable installation's managed servers."""
    response = await client.get(url="servers", params={"limit": 500})
    response.raise_for_status()
    require(condition=response.json()["next_cursor"] is None, message="Unexpected catalog size")
    for server in response.json()["items"]:
        response = await client.delete(
            url="servers/" + quote(string=server["id"], safe=""),
            headers={"If-Match": '"' + str(object=server["version"]) + '"'},
        )
        response.raise_for_status()
    with source.open(mode="rb") as stream:
        servers = read_servers(stream=stream)
    for server in servers:
        response = await client.post(url="servers", json=server.model_dump(mode="json"))
        response.raise_for_status()


async def upload(*, client: httpx.AsyncClient, path: Path) -> httpx.Response:
    """Send actual fixture bytes within the unchanged upload byte envelope."""
    status = await asyncio.to_thread(path.stat)
    require(condition=status.st_size <= 10 * 1024**2, message="Fixture upload too large")
    with path.open(mode="rb") as source:
        return await client.post(url="datasets", files={"file": (path.name, source, "text/csv")})


async def download(*, client: httpx.AsyncClient, run_id: str, root: Path, name: str) -> None:
    """Stream exact published artifacts under finite per-file and response limits."""
    size = 0
    async with client.stream(
        method="GET", url=f"simulations/{run_id}/artifact", params={"name": name}
    ) as response:
        response.raise_for_status()
        expected = int(response.headers["content-length"])
        require(condition=expected <= 256 * 1024**2, message="Artifact exceeds byte envelope")
        with (root / name).open(mode="xb") as destination:
            async for block in response.aiter_bytes(chunk_size=64 * 1024):
                size += len(block)
                require(condition=size <= expected, message="Artifact length exceeded")
                destination.write(block)
        require(condition=size == expected, message="Artifact truncated")


def expectations(*, bundle: Path, expected_path: Path) -> None:
    """Compare saved lifecycle output with manually derived fixture expectations."""
    expected = json.loads(s=expected_path.read_text(encoding="utf-8"))
    counts: Counter[str] = Counter()
    timings: dict[str, dict[str, list[int]]] = {}
    selected = expected.get("exact_lifecycle", {})
    with (bundle / "run.jsonl").open(mode="rb") as stream:
        for line in stream:
            event = json.loads(s=line)
            counts[event["event"]] += 1
            identifier = event["request_id"]
            if identifier in selected:
                timings.setdefault(identifier, {}).setdefault(event["event"], []).append(event["t"])
    for label, kind in (
        ("arrived", "REQUEST_ARRIVED"),
        ("completed", "REQUEST_FINISHED"),
        ("dropped", "REQUEST_DROPPED"),
    ):
        require(
            condition=counts[kind] == expected["terminal_counts"][label],
            message=f"Fixture {label} expectation differs",
        )
    metrics = json.loads(s=(bundle / "metrics.json").read_text(encoding="utf-8"))
    require(
        condition=metrics["finished"] == expected["terminal_counts"]["completed"]
        and metrics["dropped"] == expected["terminal_counts"]["dropped"],
        message="Saved metric terminal counts differ from fixture expectations",
    )
    for identifier, lifecycle in selected.items():
        for label, kind in (
            ("start", "REQUEST_STARTED"),
            ("finish", "REQUEST_FINISHED"),
            ("drop", "REQUEST_DROPPED"),
        ):
            if label in lifecycle:
                require(
                    condition=timings.get(identifier, {}).get(kind, []) == [lifecycle[label]],
                    message=f"Fixture {label} lifecycle differs",
                )


async def run_case(
    *, client: httpx.AsyncClient, folder: Path, variant: bool = False, repeat: bool = False
) -> dict[str, object]:
    """Submit real durable work, stream its matching bundle and independently replay it."""
    started = time.monotonic()
    suffix = ".sequential" if variant else ""
    name = folder.name + suffix + (".repeat" if repeat else "")
    case = OUTPUT / name
    bundle = case / "bundle"
    bundle.mkdir(parents=True, exist_ok=False)
    await replace_servers(client=client, source=folder / "servers.json")
    config_path = folder / ("config" + suffix + ".json")
    expected_path = folder / ("expected" + suffix + ".json")
    configuration = json.loads(s=config_path.read_text(encoding="utf-8"))
    require(condition=configuration["autoscale_enabled"] is False, message="Unsupported fixture")
    dataset = await upload(client=client, path=folder / "requests.csv")
    dataset.raise_for_status()
    body = {
        "dataset_id": dataset.json()["id"],
        "strategy": configuration["scheduling_strategy"],
        "mode": configuration["execution_mode"],
        "queue_timeout_ticks": configuration["queue_timeout_ticks"] or None,
        "autoscaling": {"enabled": False},
    }
    response = await client.post(url="simulations", json=body)
    response.raise_for_status()
    run_id = response.json()["run_id"]
    deadline = time.monotonic() + 240
    while True:
        response = await client.get(url=f"simulations/{run_id}")
        response.raise_for_status()
        detail = response.json()
        require(
            condition=detail["status"] not in {"failed", "cancelled"}, message=str(object=detail)
        )
        if detail["status"] == "completed":
            break
        require(condition=time.monotonic() < deadline, message="Scenario deadline exceeded")
        await asyncio.sleep(delay=0.2)
    for artifact in sorted(ARTIFACT_NAMES | {"manifest.json"}):
        await download(client=client, run_id=run_id, root=bundle, name=artifact)
    manifest = await asyncio.to_thread(verify_bundle, directory=bundle)
    await asyncio.to_thread(expectations, bundle=bundle, expected_path=expected_path)
    for filename in ("servers.json", "requests.csv"):
        (case / ("source-" + filename)).write_bytes(data=(folder / filename).read_bytes())
    (case / "source-config.json").write_bytes(data=config_path.read_bytes())
    (case / "expected.json").write_bytes(data=expected_path.read_bytes())
    save(path=case / "submission.json", value=body)
    save(path=case / "run-detail.json", value=detail)
    result: dict[str, object] = {
        "case": name,
        "run_id": run_id,
        "requests": dataset.json()["request_count"],
        "elapsed_seconds": round(number=time.monotonic() - started, ndigits=3),
        "bundle_digest": manifest.bundle_digest,
        "trace_sha256": manifest.artifacts["run.jsonl"].sha256,
        "events": manifest.event_count,
        "original_validator": manifest.original_validator,
        "independent_replay": "passed",
        "fixture_expectations": "passed",
    }
    save(path=case / "receipt.json", value=result)
    print(json.dumps(obj=result), flush=True)
    return result


async def invalid_cases(*, client: httpx.AsyncClient) -> list[dict[str, object]]:
    """Exercise all invalid CSVs by HTTP and server documents by the shared input parser."""
    catalog = json.loads(s=(FIXTURES / "invalid/expected_rejections.json").read_text())
    results: list[dict[str, object]] = []
    for case in catalog["cases"]:
        source = FIXTURES / case["path"]
        if case["kind"] == "csv":
            response = await upload(client=client, path=source)
            require(
                condition=response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value,
                message=f"Expected actual CSV rejection: {case['id']}",
            )
            result = {
                "case": case["id"],
                "surface": "HTTP dataset upload",
                "response": response.json(),
            }
        else:
            try:
                read_servers(stream=io.BytesIO(initial_bytes=source.read_bytes()))
            except InputError as error:
                result = {
                    "case": case["id"],
                    "surface": "server document parser",
                    "field": error.field,
                    "reason": error.reason,
                }
            else:
                raise RuntimeError(f"Expected server document rejection: {case['id']}")
            document = json.loads(s=source.read_bytes())
            if case["id"] == "26_duplicate_server_id":
                statuses = []
                for entry in document["servers"]:
                    response = await client.post(url="servers", json=entry)
                    statuses.append(response.status_code)
                require(
                    condition=statuses == [HTTPStatus.CREATED.value, HTTPStatus.CONFLICT.value],
                    message="Duplicate managed server must conflict over HTTP",
                )
                result["http_statuses"] = statuses
            elif case["id"] not in {"25_empty_cluster", "31_unsupported_tick"}:
                response = await client.post(url="servers", json=document["servers"][0])
                require(
                    condition=response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value,
                    message="Invalid server fields must be rejected over HTTP",
                )
                result["http_status"] = response.status_code
            else:
                result["http_contract"] = (
                    "Whole server documents are parsed outside per-server CRUD"
                )
        result["sha256"] = hashlib.sha256(data=source.read_bytes()).hexdigest()
        results.append(result)
    require(condition=len(results) == 32, message="Incomplete invalid-case inventory")
    return results


async def main() -> None:
    """Run only against fixed internal API DNS and a fresh matching isolated identity."""
    identity = str(object=UUID(hex=os.environ["SCENARIO_INSTALLATION_ID"]))
    profile = parse_capacity_profile(value=os.environ["CAPACITY_PROFILE"])
    await asyncio.to_thread(OUTPUT.mkdir, parents=True, exist_ok=True)
    report: dict[str, object] = {
        "started_utc": datetime.now(tz=UTC).isoformat(),
        "installation_id": identity,
        "capacity_profile": profile,
        "source_sha256": await asyncio.to_thread(source_provenance),
        "request_limit": request_limit(profile=profile),
        "upload_byte_limit": 10 * 1024**2,
        "artifact_byte_limit": 256 * 1024**2,
        "bundle_byte_limit": 512 * 1024**2,
        "status": "running",
    }
    try:
        async with httpx.AsyncClient(
            base_url="http://api:8000/api/v1/",
            timeout=60,
            headers={"Authorization": "Bearer " + os.environ["AUTH_TOKEN"]},
        ) as client:
            await installation(client=client, profile=profile, identity=identity)
            if profile == "standard":
                rejected = []
                for name in ("20_sustained_12k", "21_stress_50k"):
                    response = await upload(
                        client=client, path=FIXTURES / "scenarios" / name / "requests.csv"
                    )
                    require(
                        condition=response.status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value,
                        message="Default capacity widened",
                    )
                    rejected.append({"case": name, "response": response.json()})
                report["default_rejections"] = rejected
            else:
                report["invalid_cases"] = await invalid_cases(client=client)
                completed = []
                report["completed"] = completed
                for folder in sorted((FIXTURES / "scenarios").iterdir()):
                    completed.append(await run_case(client=client, folder=folder))
                    if folder.name == "16_execution_modes":
                        completed.append(await run_case(client=client, folder=folder, variant=True))
                for name in ("07_lexical_ties", "21_stress_50k"):
                    repeated = await run_case(
                        client=client, folder=FIXTURES / "scenarios" / name, repeat=True
                    )
                    original = next(item for item in completed if item["case"] == name)
                    require(
                        condition=repeated["trace_sha256"] == original["trace_sha256"],
                        message="Repeat trace differs",
                    )
                    completed.append(repeated)
                hashes = {item["case"]: item["trace_sha256"] for item in completed}
                require(
                    condition=hashes["07_lexical_ties"] == hashes["08_shuffled_input"],
                    message="Shuffled fixture trace differs",
                )
                report["completed"] = completed
            report["status"] = "passed"
    except BaseException:
        report["status"] = "failed"
        raise
    finally:
        report["finished_utc"] = datetime.now(tz=UTC).isoformat()
        report["runner_memory_peak_bytes"] = int(
            await asyncio.to_thread(Path("/sys/fs/cgroup/memory.peak").read_text)
        )
        report["runner_memory_events"] = await asyncio.to_thread(
            Path("/sys/fs/cgroup/memory.events").read_text
        )
        save(path=OUTPUT / "report.json", value=report)


if __name__ == "__main__":
    asyncio.run(main=main())
