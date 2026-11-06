"""Exercise real HTTP submission, worker completion, playback, and bounded exports."""

import asyncio
import io
import json
import os
import time
import zipfile
from pathlib import Path

import httpx


async def main() -> None:
    headers = (
        {"Authorization": "Bearer " + os.environ["AUTH_TOKEN"]}
        if os.environ.get(key="AUTH_ENABLED") == "true"
        else {}
    )
    async with httpx.AsyncClient(
        base_url="http://api:8000/api/v1/", headers=headers, timeout=30
    ) as client:
        servers = await client.get(url="servers")
        servers.raise_for_status()
        if not servers.json()["items"]:
            raise RuntimeError("API smoke requires at least one configured logical server")
        source = await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
        dataset = await client.post(
            url="datasets", files={"file": ("requests.csv", source, "text/csv")}
        )
        dataset.raise_for_status()
        run = await client.post(
            url="simulations", json={"dataset_id": dataset.json()["id"], "strategy": "round_robin"}
        )
        run.raise_for_status()
        run_id = run.json()["run_id"]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            detail = await client.get(url=f"simulations/{run_id}")
            detail.raise_for_status()
            if detail.json()["status"] == "completed":
                break
            if detail.json()["status"] in {"failed", "cancelled"}:
                raise RuntimeError("API smoke run did not complete")
            await asyncio.sleep(delay=0.1)
        else:
            raise RuntimeError("API worker completion exceeded smoke timeout")
        metrics = await client.get(url=f"simulations/{run_id}/metrics")
        metrics.raise_for_status()
        trace = await client.get(url=f"simulations/{run_id}/artifact")
        trace.raise_for_status()
        if len(trace.content) != int(trace.headers["content-length"]):
            raise RuntimeError("HTTP artifact length mismatch")
        archive = await client.get(url=f"simulations/{run_id}/inputs")
        archive.raise_for_status()
        with zipfile.ZipFile(file=io.BytesIO(initial_bytes=archive.content)) as bundle:
            if set(bundle.namelist()) != {
                "servers.json",
                "requests.csv",
                "snapshot.json",
                "manifest.json",
            }:
                raise RuntimeError("HTTP input archive is incomplete")
        stream = await client.get(
            url=f"simulations/{run_id}/stream", headers={"Last-Event-ID": "1"}
        )
        stream.raise_for_status()
        if "id: 2\n" not in stream.text or "event: status" not in stream.text:
            raise RuntimeError(
                "HTTP SSE replay did not return committed events and terminal status"
            )
        print(
            json.dumps(
                obj={
                    "run_id": run_id,
                    "bundle_digest": detail.json()["validation"]["bundle_digest"],
                    "status": "HTTP workflow verified",
                }
            )
        )


if __name__ == "__main__":
    asyncio.run(main=main())
