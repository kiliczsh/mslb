"""Bounded daemon transport and hostile full-inspection configurations."""

import asyncio
import json
from uuid import uuid4

import httpx
import pytest
from mslb_contracts.docker_runs import CreationIntent
from mslb_controller.engine import (
    NETWORK,
    PROJECT,
    ContainerInfo,
    Engine,
    EngineError,
    ImageInfo,
    NetworkInfo,
)
from mslb_controller.inspection import MASKED, READONLY, dynamic, mapping, strings


def fixture(
    intent: CreationIntent | None = None,
) -> tuple[CreationIntent, ImageInfo, NetworkInfo, ContainerInfo]:
    intent = intent or CreationIntent(
        deployment_id=uuid4(),
        run_id=uuid4(),
        intent_id=uuid4(),
        creator_fence=1,
        slot=2,
        image_id="sha256:" + "a" * 64,
        network_id="b" * 64,
        logical_admission_mem_mb=1024,
        max_inflight=4,
        memory_mb=128,
        nano_cpus=500_000_000,
    )
    image = ImageInfo(Id=intent.image_id, Config={"Env": ["PATH=/app/.venv/bin"], "Labels": {}})
    net = NetworkInfo(
        Id=intent.network_id,
        Name=NETWORK,
        Driver="bridge",
        Internal=True,
        Scope="local",
        Labels={
            "com.docker.compose.project": PROJECT,
            "com.docker.compose.network": "docker-traffic",
        },
    )
    body = intent.daemon_body()
    config = {
        key: value for key, value in body.items() if key not in {"HostConfig", "NetworkingConfig"}
    }
    config.update(Hostname="c" * 12, Domainname="", Tty=False, OpenStdin=False, AttachStdin=False)
    config["Env"] = ["PATH=/app/.venv/bin", *strings(body["Env"])]
    host = mapping(body["HostConfig"])
    host.update(
        RestartPolicy={"Name": "no", "MaximumRetryCount": 0},
        IpcMode="private",
        CgroupnsMode="private",
        Runtime="runc",
        ShmSize=67108864,
        MaskedPaths=sorted(MASKED),
        ReadonlyPaths=sorted(READONLY),
        ConsoleSize=[0, 0],
    )
    info = ContainerInfo(
        Id="c" * 64,
        Image=image.Id,
        Name="/" + intent.name,
        Config=config,
        HostConfig=host,
        State={"Running": False},
        Mounts=[],
        NetworkSettings={"Networks": {NETWORK: {"NetworkID": net.Id, "Aliases": [intent.name]}}},
    )
    return intent, image, net, info


def test_inspection_requires_configuration_not_only_labels() -> None:
    intent, image, net, info = fixture()
    dynamic(info, image, net, intent)
    for section, field, value in (
        ("HostConfig", "Privileged", True),
        ("HostConfig", "Binds", ["/:/host"]),
        ("HostConfig", "Devices", [{"PathOnHost": "/dev/sda"}]),
        ("HostConfig", "NetworkMode", "host"),
        ("HostConfig", "MemorySwap", -1),
        ("HostConfig", "CapAdd", ["SYS_ADMIN"]),
        ("HostConfig", "PortBindings", {"8080/tcp": []}),
        ("HostConfig", "ReadonlyRootfs", False),
        ("HostConfig", "FuturePrivilege", "enabled"),
        ("Config", "User", "root"),
        ("Config", "Entrypoint", ["sh"]),
        ("Config", "Env", ["BACKEND_ID=spoofed"]),
        ("Config", "Healthcheck", {"Test": ["CMD", "sh"]}),
    ):
        raw = info.model_dump()
        raw[section][field] = value
        with pytest.raises(EngineError, match="OWNERSHIP_CONFLICT"):
            dynamic(ContainerInfo.model_validate(raw), image, net, intent)
    with pytest.raises(EngineError):
        dynamic(info.model_copy(update={"Mounts": [{"Source": "/host"}]}), image, net, intent)
    with pytest.raises(EngineError):
        dynamic(info, image, net.model_copy(update={"Internal": False}), intent)


def test_disconnected_endpoint_requires_pinned_network_and_stopped_state() -> None:
    intent, image, net, info = fixture()
    pending = info.model_copy(
        update={
            "State": {"Running": False, "Status": "created"},
            "NetworkSettings": {
                "Networks": {net.Name: {"NetworkID": "", "Aliases": [intent.name]}}
            },
        }
    )
    dynamic(pending, image, net, intent)
    for changed in (
        {"State": {"Running": True, "Status": "running"}},
        {"HostConfig": pending.HostConfig | {"NetworkMode": net.Name}},
    ):
        with pytest.raises(EngineError, match="OWNERSHIP_CONFLICT"):
            dynamic(pending.model_copy(update=changed), image, net, intent)


def test_fixed_engine_paths_and_no_automatic_retry() -> None:
    async def run() -> None:
        requests: list[httpx.Request] = []

        def respond(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if request.url.path.endswith("/create"):
                return httpx.Response(
                    201,
                    stream=httpx.ByteStream(
                        json.dumps({"Id": "c" * 64, "Warnings": ["retain ID"]}).encode()
                    ),
                )
            return httpx.Response(204)

        engine = Engine(httpx.MockTransport(respond))
        intent, _image, _net, _info = fixture()
        try:
            created = await engine.create(intent)
            assert created.Id == "c" * 64 and created.Warnings == ["retain ID"]
            assert json.loads(requests[0].content) == intent.daemon_body()
            assert requests[0].url.params["name"] == intent.name
            await engine.start(created.Id)
            await engine.stop(created.Id)
            await engine.remove(created.Id)
            assert requests[-1].url.query == b"force=false&v=false"
            for bad in ("../all", "backend-a", "abc", "c" * 64 + "?force=true"):
                with pytest.raises(EngineError, match="INVALID_REFERENCE"):
                    await engine.remove(bad)
                with pytest.raises(EngineError, match="INVALID_REFERENCE"):
                    await engine.inspect(bad)
            assert len(requests) == 4
        finally:
            await engine.close()

    asyncio.run(run())


@pytest.mark.parametrize(
    "status,body,headers,code",
    [
        (302, b"private details", {"location": "http://elsewhere"}, "ENGINE_STATUS"),
        (500, b"private details", {}, "ENGINE_STATUS"),
        (200, b"x" * (256 * 1024 + 1), {}, "ENGINE_RESPONSE"),
        (200, b"{}", {"content-encoding": "br"}, "ENGINE_RESPONSE"),
        (200, b'{"Id":true}', {}, "ENGINE_RESPONSE"),
    ],
)
def test_daemon_errors_are_bounded_and_do_not_echo_host_details(
    status: int, body: bytes, headers: dict[str, str], code: str
) -> None:
    async def run() -> None:
        count = 0

        def respond(_request: httpx.Request) -> httpx.Response:
            nonlocal count
            count += 1
            return httpx.Response(status, stream=httpx.ByteStream(body), headers=headers)

        engine = Engine(httpx.MockTransport(respond))
        try:
            with pytest.raises(EngineError, match=code) as error:
                await engine.image()
            assert str(error.value) == code and count == 1
        finally:
            await engine.close()

    asyncio.run(run())
