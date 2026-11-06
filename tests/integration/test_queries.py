import asyncio
import io
import json
import secrets
from http import HTTPStatus
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from mslb_application.errors import OperationError
from mslb_application.files import ManagedFiles, StorageLimits
from mslb_application.queries import Download, Queries
from mslb_application.snapshots import make_snapshot
from mslb_application.submissions import Submissions
from mslb_application.worker import Worker
from mslb_contracts.queries import ComparisonView, JsonValue
from mslb_contracts.runs import LegacyRunSettings, RunSettings, ScalingSettings, TemplateSettings
from mslb_engine.models import SimulationError
from mslb_persistence.catalog import Catalog
from mslb_persistence.database import Database, StoreError
from mslb_persistence.models import Run
from pydantic import TypeAdapter
from sqlalchemy import update

from tests.integration.helpers import fixture_inputs, scratch


async def prepare(
    database: Database, root: Path, settings: tuple[RunSettings, ...], requests: bytes | None = None
) -> tuple[ManagedFiles, Submissions, tuple[UUID, ...]]:
    servers, _, _ = await asyncio.to_thread(fixture_inputs)
    catalog = Catalog(database=database)
    await catalog.seed_once(servers=servers)
    inputs = await asyncio.to_thread(
        ManagedFiles, root=root / "inputs", limits=StorageLimits(min_free_bytes=0)
    )
    outputs = await asyncio.to_thread(
        ManagedFiles, root=root / "outputs", limits=StorageLimits(min_free_bytes=0)
    )
    service = Submissions(catalog=catalog, files=inputs)
    source = (
        requests
        if requests is not None
        else await asyncio.to_thread(Path("/fixtures/core/requests.csv").read_bytes)
    )
    dataset = await service.upload(source=io.BytesIO(initial_bytes=source), filename="requests.csv")
    runs = tuple(
        [(await service.submit(dataset_id=dataset.id, settings=config)).id for config in settings]
    )
    return outputs, service, runs


def test_run_history_uses_stable_tied_timestamp_order_and_byte_budget(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            outputs, _, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),) * 5
            )
            query = Queries(
                database=database,
                outputs=outputs,
                cursor_secret=b"test-history-signing-key" * 2,
                response_bytes=3000,
            )
            async with database.sessions.begin() as session:
                oldest = await session.get(entity=Run, ident=runs[0])
                assert oldest is not None
                await session.execute(
                    statement=update(table=Run).values(created_at=oldest.created_at)
                )
            found: list[UUID] = []
            cursor = None
            while True:
                page = await query.runs(limit=5, cursor=cursor)
                assert len(page.model_dump_json().encode()) <= 3000
                assert len(page.items) < 5
                found.extend(row.id for row in page.items)
                cursor = page.next_cursor
                if cursor is None:
                    break
            assert found == sorted(runs)
            first = await query.runs(limit=1)
            with pytest.raises(expected_exception=OperationError, match="unrelated cursor"):
                await query.datasets(cursor=first.next_cursor)
            dataset = await query.datasets()
            assert len(dataset.items) == 1
            assert "storage_key" not in dataset.model_dump_json()
            assert "snapshot_key" not in (await query.run(run_id=runs[0])).model_dump_json()
            assert (await query.dataset(dataset_id=dataset.items[0].id)) == dataset.items[0]
            tiny = Queries(
                database=database,
                outputs=outputs,
                cursor_secret=b"test-history-signing-key" * 2,
                response_bytes=1024,
            )
            with pytest.raises(expected_exception=OperationError, match="One item"):
                await tiny.runs()
            for invalid in [0, 501, True]:
                with pytest.raises(expected_exception=StoreError, match="limit"):
                    await query.runs(limit=invalid)
            with pytest.raises(expected_exception=StoreError, match="does not exist"):
                await query.run(run_id=uuid4())
            with pytest.raises(expected_exception=StoreError, match="does not exist"):
                await query.dataset(dataset_id=uuid4())

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="authenticated", argvalues=[False, True])
def test_verified_playback_matches_saved_intervals_events_and_http_bounds(
    tmp_path: Path, authenticated: bool
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            outputs, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(mode="concurrent"),)
            )
            query = Queries(
                database=database, outputs=outputs, cursor_secret=b"test-playback-signing-key" * 2
            )
            with pytest.raises(expected_exception=OperationError, match="completed"):
                await query.playback(run_id=runs[0], tick=0)
            worker = Worker(
                database=database, inputs=service.files, outputs=outputs, owner="playback-test"
            )
            assert await worker.run_once()
            view = await query.playback(
                run_id=runs[0], tick=0, request_kind="active", server_id="s1"
            )
            assert view.run_id == runs[0] and view.servers[0].id == "s1"
            assert view.active_count > 0 and view.request_ids
            terminal = await query.playback(run_id=runs[0], tick=6)
            assert terminal.terminal
            with pytest.raises(expected_exception=OperationError, match="playback"):
                await query.playback(run_id=runs[0], tick=True)
            with pytest.raises(expected_exception=OperationError, match="playback"):
                await query.playback(run_id=runs[0], tick=0, request_kind="active")
            with pytest.raises(expected_exception=OperationError, match="playback"):
                await query.playback(run_id=runs[0], tick=0, server_offset=65)
            import httpx
            from mslb_api.app import create_app
            from mslb_api.settings import Settings

            token = secrets.token_urlsafe(nbytes=32)
            app = create_app(
                settings=Settings(
                    allowed_hosts=("testserver",),
                    auth_enabled=authenticated,
                    token=token,
                ),
                database=database,
                inputs=service.files,
                outputs=outputs,
                signing_key=b"p" * 32,
            )
            async with (
                app.router.lifespan_context(app),
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://testserver/api/v1/"
                ) as client,
            ):
                if authenticated:
                    assert (
                        await client.get(url=f"simulations/{runs[0]}/playback")
                    ).status_code == HTTPStatus.UNAUTHORIZED.value
                    client.headers["Authorization"] = "Bearer " + token
                response = await client.get(url=f"simulations/{runs[0]}/playback?tick=0")
                assert (
                    response.status_code == HTTPStatus.OK.value
                    and response.json()["bundle_digest"] == view.bundle_digest
                )
                assert (
                    await client.get(url=f"simulations/{runs[0]}/playback?request_offset=10001")
                ).status_code == HTTPStatus.UNPROCESSABLE_CONTENT.value
                assert (
                    await client.get(url=f"simulations/{runs[0]}/playback?tick=7")
                ).status_code == HTTPStatus.NOT_FOUND.value
            source = "t,request_id,work_units,mem_mb\n" + "".join(
                f"0,request-{i:02d}-{'x' * 80},1,1\n" for i in range(12)
            )
            dataset = await service.upload(
                source=io.BytesIO(initial_bytes=source.encode()), filename="bounded-view.csv"
            )
            crowded = await service.submit(dataset_id=dataset.id, settings=RunSettings())
            assert await worker.run_once()
            with pytest.raises(expected_exception=OperationError, match="budget"):
                await Queries(
                    database=database, outputs=outputs, cursor_secret=b"p" * 32, response_bytes=1024
                ).playback(run_id=crowded.id, tick=0)

    asyncio.run(main=scenario())


def test_scaled_playback_matches_persisted_cpu_membership_and_policy_sidecar(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        settings = RunSettings.model_validate_json(
            json_data=await asyncio.to_thread(Path("data/examples/scaling-config.json").read_bytes)
        )
        async with scratch() as database:
            outputs, service, runs = await prepare(
                database=database,
                root=tmp_path,
                settings=(settings,),
                requests=await asyncio.to_thread(
                    Path("data/examples/scaling-requests.csv").read_bytes
                ),
            )
            assert await Worker(database=database, inputs=service.files, outputs=outputs).run_once()
            query = Queries(
                database=database, outputs=outputs, cursor_secret=b"scaled-playback-key" * 2
            )
            timeline = await query.download(run_id=runs[0], name="timeline.jsonl")
            scaling = await query.download(run_id=runs[0], name="autoscaling_decisions.jsonl")
            with timeline.open() as source:
                frames = [json.loads(s=line) for line in source]
            with scaling.open() as source:
                decisions = [json.loads(s=line) for line in source]
            assert any(decision["action"] == "scale_out" for decision in decisions)
            for saved in frames:
                view = await query.playback(run_id=runs[0], tick=saved["t"])
                assert [server.id for server in view.servers] == [
                    member["id"] for member in saved["membership"]
                ]
                assert view.queue_depth == len(saved["pending_ids"])
                assert [decision.model_dump(mode="json") for decision in view.decisions] == [
                    decision
                    for decision in decisions
                    if saved["t"] in (decision["t"], decision["applied_at"])
                ]
                if not saved["terminal"]:
                    assert [
                        server.model_dump(mode="json")["consumed_cpu"] for server in view.servers
                    ] == [sample["consumed_cpu"] for sample in saved["servers"]]
            assert any(len(frame["membership"]) == 3 for frame in frames)

    asyncio.run(main=scenario())


def test_committed_event_pages_cross_501_and_bind_run_and_inclusive_filters(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            source = (
                "t,request_id,work_units,mem_mb\n"
                + "".join(f"0,r{i:04d},1,1\n" for i in range(251))
            ).encode()
            outputs, service, runs = await prepare(
                database=database,
                root=tmp_path,
                settings=(RunSettings(), RunSettings()),
                requests=source,
            )
            worker = Worker(database=database, inputs=service.files, outputs=outputs)
            assert await worker.run_once()
            queries = Queries(
                database=database, outputs=outputs, cursor_secret=b"event-signing-key" * 2
            )
            assert (await queries.run(run_id=runs[0])).progress is not None
            first = await queries.events(run_id=runs[0], limit=500)
            assert len(first.items) == 500 and first.next_cursor is not None
            remaining = await queries.events(run_id=runs[0], limit=500, cursor=first.next_cursor)
            all_events = first.items + remaining.items
            assert [row.sequence for row in all_events] == list(range(1, 754))
            with pytest.raises(expected_exception=OperationError, match="unrelated cursor"):
                await queries.events(run_id=runs[1], cursor=first.next_cursor)
            with pytest.raises(expected_exception=OperationError, match="unrelated cursor"):
                await queries.events(run_id=runs[0], cursor=first.next_cursor, start_tick=1)
            selected = await queries.events(run_id=runs[0], start_tick=0, end_tick=0, limit=500)
            assert selected.items and all(row.t == 0 for row in selected.items)
            assert not (await queries.events(run_id=runs[1])).items
            state = await queries.run(run_id=runs[0])
            assert state.progress is not None and state.progress.committed_events == 753
            assert state.completed_at is not None and state.status == "completed"
            bounded = Queries(
                database=database,
                outputs=outputs,
                cursor_secret=b"event-signing-key" * 2,
                response_bytes=3000,
            )
            sequences: list[int] = []
            cursor = None
            while True:
                page = await bounded.events(run_id=runs[0], limit=500, cursor=cursor)
                assert len(page.model_dump_json().encode()) <= 3000
                sequences.extend(event.sequence for event in page.items)
                cursor = page.next_cursor
                if cursor is None:
                    break
            assert sequences == list(range(1, 754))

    asyncio.run(main=scenario())


def test_metrics_charts_downloads_and_comparison_use_completed_verified_output(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            settings = tuple(
                RunSettings(strategy=strategy)
                for strategy in (
                    "first_fit",
                    "round_robin",
                    "least_connections",
                    "most_available_memory",
                )
            )
            outputs, service, runs = await prepare(
                database=database, root=tmp_path, settings=settings
            )
            queries = Queries(
                database=database, outputs=outputs, cursor_secret=b"metric-signing-key" * 2
            )
            with pytest.raises(expected_exception=OperationError, match="completed"):
                await queries.metrics(run_id=runs[0])
            worker = Worker(database=database, inputs=service.files, outputs=outputs)
            for _ in runs:
                assert await worker.run_once()
            report = await queries.metrics(run_id=runs[0], server_ids=("s1",))
            handle = await queries.download(run_id=runs[0], name="metrics.json")

            def original() -> dict[str, JsonValue]:
                with handle.open() as stream:
                    return TypeAdapter(type=dict[str, JsonValue]).validate_json(stream.read(65536))

            data = await asyncio.to_thread(original)
            assert report.summary == {key: value for key, value in data.items() if key != "servers"}
            servers = data["servers"]
            assert isinstance(servers, dict)
            assert report.servers == {"s1": servers["s1"]}
            server_page = await queries.server_metrics(run_id=runs[0], limit=1)
            second = await queries.server_metrics(
                run_id=runs[0], limit=1, cursor=server_page.next_cursor
            )
            assert server_page.items[0].id == "s1" and second.items[0].id == "s2"
            chart = await queries.chart(
                run_id=runs[0], metric="cpu_utilization", start_tick=0, end_tick=10, max_points=1
            )
            assert len(chart.points) == 1
            assert chart.points[0]["max"] != 0
            assert chart.points[0]["covered_ticks"] == data["terminal_tick"]
            with pytest.raises(expected_exception=OperationError, match="not in this run"):
                await queries.chart(
                    run_id=runs[0],
                    metric="active_requests",
                    start_tick=0,
                    end_tick=10,
                    server_id="absent",
                )
            with pytest.raises(expected_exception=OperationError, match="not in this run"):
                await queries.metrics(run_id=runs[0], server_ids=("absent",))
            comparison = await queries.compare(run_ids=runs)
            assert comparison.baseline_run_id == runs[0]
            assert comparison.runs[0].metrics["finished"] == {
                "baseline": 4,
                "candidate": 4,
                "difference": 0,
                "percentage_difference": 0,
            }
            assert comparison.runs[1].differing_settings == ("strategy",)
            assert (
                ComparisonView.model_validate_json(json_data=comparison.model_dump_json())
                == comparison
            )
            for name in ("run.jsonl", "requests.csv", "snapshot.json", "manifest.json"):
                download = await queries.download(run_id=runs[0], name=name)
                assert download.filename == name

                def read(handle: Download) -> bytes:
                    with handle.open() as stream:
                        return stream.read(65536)

                assert await asyncio.to_thread(read, handle=download)

            with pytest.raises(expected_exception=OperationError, match="Unknown artifact"):
                await queries.download(run_id=runs[0], name="../snapshot.json")
            with pytest.raises(expected_exception=OperationError, match="Invalid chart"):
                await queries.chart(
                    run_id=runs[0],
                    metric="pending_depth",
                    start_tick=0,
                    end_tick=10,
                    server_id="s1",
                )
            with pytest.raises(expected_exception=OperationError, match="Invalid chart"):
                await queries.chart(
                    run_id=runs[0],
                    metric="cpu_utilization",
                    start_tick=0,
                    end_tick=10,
                    max_points=2001,
                )
            with pytest.raises(expected_exception=OperationError, match="distinct"):
                await queries.compare(run_ids=(runs[0], runs[0]))

            await asyncio.to_thread(
                outputs.path(key=handle.reference.key).write_bytes, data=b"corrupted"
            )
            with pytest.raises(expected_exception=OperationError, match="identity mismatch"):
                await queries.metrics(run_id=runs[0])

    asyncio.run(main=scenario())


@pytest.mark.parametrize(
    argnames="different",
    argvalues=[RunSettings(max_simulation_ticks=10_000), RunSettings(mode="concurrent")],
)
def test_comparison_rejects_semantic_setting_and_mode_mismatches(
    tmp_path: Path, different: RunSettings
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            outputs, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(), different)
            )
            worker = Worker(database=database, inputs=service.files, outputs=outputs)
            assert await worker.run_once() and await worker.run_once()
            queries = Queries(
                database=database, outputs=outputs, cursor_secret=b"compatible-signing-key" * 2
            )
            with pytest.raises(
                expected_exception=SimulationError, match="semantic bounds differ"
            ) as failure:
                await queries.compare(run_ids=runs)
            assert failure.value.code == "CONFLICT"

    asyncio.run(main=scenario())


def test_archived_mode_executes_and_compares_without_rewriting_saved_identity(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            outputs, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),)
            )
            record = await service.catalog.get_run(run_id=runs[0])
            current = await asyncio.to_thread(
                service.files.load_snapshot,
                key=record.snapshot_key,
                expected_digest=record.snapshot_digest,
            )
            archived = make_snapshot(
                servers=current.servers,
                requests=current.requests,
                settings=LegacyRunSettings(mode="sequential_compat"),
            )
            reference = await asyncio.to_thread(service.files.save_snapshot, snapshot=archived)
            before = service.files.path(key=reference.key).read_bytes()
            servers = await service.catalog.list_servers()
            legacy_run = await service.catalog.enqueue(
                dataset_id=record.dataset_id,
                snapshot=archived,
                versions=tuple((server.id, server.version) for server in servers),
                body_digest="1" * 64,
            )
            worker = Worker(database=database, inputs=service.files, outputs=outputs)
            assert await worker.run_once() and await worker.run_once()
            queries = Queries(
                database=database, outputs=outputs, cursor_secret=b"legacy-mode-cursor" * 2
            )
            history = await queries.run(run_id=legacy_run.id)
            assert history.configuration.mode == "sequential"
            assert history.snapshot_digest == archived.snapshot_digest
            comparison = await queries.compare(run_ids=(runs[0], legacy_run.id))
            assert comparison.runs[1].differing_settings == ()
            assert service.files.path(key=reference.key).read_bytes() == before
            async with database.sessions() as session:
                row = await session.get(entity=Run, ident=legacy_run.id)
                assert row is not None and row.configuration["mode"] == "sequential_compat"

    asyncio.run(main=scenario())


def test_all_dropped_comparison_preserves_null_latency_and_zero_baseline(tmp_path: Path) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            source = b"t,request_id,work_units,mem_mb\n0,too-big,1,1000000\n"
            outputs, service, runs = await prepare(
                database=database,
                root=tmp_path,
                settings=(RunSettings(), RunSettings(strategy="round_robin")),
                requests=source,
            )
            worker = Worker(database=database, inputs=service.files, outputs=outputs)
            assert await worker.run_once() and await worker.run_once()
            queries = Queries(
                database=database, outputs=outputs, cursor_secret=b"null-metric-signing-key" * 2
            )
            result = await queries.compare(run_ids=runs)
            assert result.runs[1].metrics["waiting.mean"] == {
                "baseline": None,
                "candidate": None,
                "difference": None,
                "percentage_difference": None,
            }
            assert result.runs[1].metrics["finished"] == {
                "baseline": 0,
                "candidate": 0,
                "difference": 0,
                "percentage_difference": None,
            }

    asyncio.run(main=scenario())


@pytest.mark.parametrize(argnames="difference", argvalues=["dataset", "servers", "autoscaling"])
def test_comparison_checks_real_frozen_input_changes_and_allows_scaling_dimension(
    tmp_path: Path, difference: str
) -> None:
    async def scenario() -> None:
        async with scratch() as database:
            outputs, service, runs = await prepare(
                database=database, root=tmp_path, settings=(RunSettings(),)
            )
            run = await service.catalog.get_run(run_id=runs[0])
            dataset_id = run.dataset_id
            settings = RunSettings()
            if difference == "dataset":
                dataset = await service.upload(
                    source=io.BytesIO(initial_bytes=b"t,request_id,work_units,mem_mb\n0,new,1,1\n"),
                    filename="different.csv",
                )
                dataset_id = dataset.id
            elif difference == "servers":
                servers = await service.catalog.list_servers()
                original = servers[0]
                updated = original.model_dump(exclude={"version"})
                updated["mem_mb"] += 1
                from mslb_contracts.inputs import ServerInput

                await service.catalog.update_server(
                    server=ServerInput.model_validate(obj=updated),
                    expected_version=original.version,
                )
            else:
                settings = RunSettings(
                    autoscaling=ScalingSettings(
                        enabled=True,
                        max_servers=3,
                        server_template=TemplateSettings(
                            cpu_units_per_tick=10, mem_mb=1024, rate_limit_per_sec=2
                        ),
                    )
                )
            second = await service.submit(dataset_id=dataset_id, settings=settings)
            worker = Worker(database=database, inputs=service.files, outputs=outputs)
            assert await worker.run_once() and await worker.run_once()
            queries = Queries(
                database=database, outputs=outputs, cursor_secret=b"input-signing-key" * 2
            )
            if difference == "autoscaling":
                comparison = await queries.compare(run_ids=(runs[0], second.id))
                assert comparison.runs[1].differing_settings == ("autoscaling",)
            else:
                with pytest.raises(
                    expected_exception=SimulationError, match="semantic bounds differ"
                ) as failure:
                    await queries.compare(run_ids=(runs[0], second.id))
                assert failure.value.code == "CONFLICT"

    asyncio.run(main=scenario())
