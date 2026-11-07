"""Fail-closed inspection of the complete approved work-container template."""

from mslb_contracts.docker_inventory import DeploymentSpec, StaticBackend
from mslb_contracts.docker_ownership import CreationIntent

from mslb_controller.engine import IMAGE, NETWORK, PROJECT
from mslb_controller.engine_models import ContainerInfo, ImageInfo, NetworkInfo
from mslb_controller.errors import EngineError

TMP_PATH = "/run/mslb-tmp"
TMP_OPTIONS = "rw,noexec,nosuid,nodev,size=16777216,uid=1000,gid=1000"
LOGGING = {
    "Type": "json-file",
    "Config": {"max-size": "5m", "max-file": "2", "mode": "non-blocking", "max-buffer-size": "1m"},
}
MASKED = {
    "/proc/acpi",
    "/proc/asound",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/sched_debug",
    "/proc/scsi",
    "/proc/timer_list",
    "/proc/timer_stats",
    "/sys/devices/virtual/powercap",
    "/sys/firmware",
}
READONLY = {"/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"}


def require(condition: bool) -> None:
    """Reject daemon metadata that does not satisfy an enrollment invariant."""
    if not condition:
        raise EngineError(code="OWNERSHIP_CONFLICT")


def mapping(value: object) -> dict[str, object]:
    """Require a string-keyed object before inspecting untrusted daemon metadata."""
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise EngineError(code="ENGINE_RESPONSE")
    return {str(object=key): item for key, item in value.items()}


def strings(value: object) -> tuple[str, ...]:
    """Require a bounded string collection in daemon metadata."""
    if (
        not isinstance(value, list)
        or len(value) > 64
        or any(not isinstance(item, str) or len(item) > 4096 for item in value)
    ):
        raise EngineError(code="ENGINE_RESPONSE")
    return tuple(str(object=item) for item in value)


def environment(value: object) -> dict[str, str]:
    """Parse backend environment entries while rejecting ambiguous duplicate keys."""
    result: dict[str, str] = {}
    for item in strings(value=value):
        key, separator, content = item.partition("=")
        require(condition=bool(separator) and bool(key) and key not in result)
        result[key] = content
    return result


def labels(value: object) -> dict[str, str]:
    """Read a bounded string label map; enrollment checks ownership separately."""
    raw = mapping(value=value or {})
    if len(raw) > 64 or any(not isinstance(item, str) or len(item) > 4096 for item in raw.values()):
        raise EngineError(code="ENGINE_RESPONSE")
    return {key: str(object=item) for key, item in raw.items()}


def network(info: NetworkInfo) -> None:
    """Verify private bridge metadata and Compose ownership for the approved network."""
    require(
        condition=info.Name == NETWORK
        and info.Internal
        and info.Driver == "bridge"
        and info.Scope == "local"
    )
    require(
        condition=info.Labels.get("com.docker.compose.project") == PROJECT
        and info.Labels.get("com.docker.compose.network") == "docker-traffic"
    )


def _tmpfs(value: object) -> None:
    raw = mapping(value=value)
    require(condition=set(raw) == {TMP_PATH} and isinstance(raw[TMP_PATH], str))
    options = str(object=raw[TMP_PATH]).replace("size=16m", "size=16777216").split(sep=",")
    require(condition=len(options) == 7 and set(options) == set(TMP_OPTIONS.split(sep=",")))


def _common(
    info: ContainerInfo, image: ImageInfo, net: NetworkInfo, *, name: str, memory: int, cpus: int
) -> None:
    """Verify the complete approved container security and resource template, failing closed."""
    network(info=net)
    require(condition=info.Image == image.Id and info.Name == "/" + name)
    config, host = info.Config, info.HostConfig
    require(condition=config.get("User") == "1000:1000" and config.get("WorkingDir") == "/app")
    require(condition=config.get("Hostname") == info.Id[:12] and config.get("Domainname") == "")
    require(condition=config.get("StopSignal") == image.Config.get("StopSignal"))
    require(
        condition=not config.get("Volumes")
        and config.get("Tty") is False
        and config.get("OpenStdin") is False
        and config.get("AttachStdin") is False
    )
    require(
        condition=info.Mounts == []
        and host.get("Memory") == memory * 1024**2
        and host.get("MemorySwap") == memory * 1024**2
    )
    require(
        condition=host.get("NanoCpus") == cpus
        and host.get("PidsLimit") == 32
        and host.get("ReadonlyRootfs") is True
    )
    require(
        condition=host.get("CapDrop") == ["ALL"]
        and host.get("SecurityOpt") == ["no-new-privileges:true"]
    )
    require(
        condition=host.get("LogConfig") == LOGGING
        and host.get("RestartPolicy") == {"Name": "no", "MaximumRetryCount": 0}
    )
    require(condition=host.get("NetworkMode") in {net.Id, net.Name})
    require(
        condition=host.get("IpcMode") == "private"
        and host.get("CgroupnsMode") == "private"
        and host.get("Runtime") == "runc"
        and host.get("ShmSize") == 64 * 1024**2
    )
    require(
        condition=set(strings(value=host.get("MaskedPaths"))) == MASKED
        and set(strings(value=host.get("ReadonlyPaths"))) == READONLY
    )
    _tmpfs(value=host.get("Tmpfs"))
    allowed = {
        "Memory",
        "MemorySwap",
        "NanoCpus",
        "PidsLimit",
        "ReadonlyRootfs",
        "CapDrop",
        "SecurityOpt",
        "LogConfig",
        "RestartPolicy",
        "NetworkMode",
        "IpcMode",
        "CgroupnsMode",
        "Runtime",
        "ShmSize",
        "MaskedPaths",
        "ReadonlyPaths",
        "Tmpfs",
        "ConsoleSize",
    }
    require(condition=host.get("ConsoleSize") == [0, 0])
    for key, value in host.items():
        if key not in allowed:
            require(
                condition=value is None
                or value is False
                or value == 0
                or value == ""
                or value == []
                or value == {}
            )
    networks = mapping(value=info.NetworkSettings.get("Networks"))
    require(condition=set(networks) == {net.Name})
    endpoint = mapping(value=networks[net.Name])
    attached = endpoint.get("NetworkID") == net.Id
    pending = (
        endpoint.get("NetworkID") == ""
        and host.get("NetworkMode") == net.Id
        and info.State.get("Running") is False
        and info.State.get("Status") in {"created", "exited", "dead"}
        and not endpoint.get("EndpointID")
        and not endpoint.get("IPAddress")
        and not endpoint.get("GlobalIPv6Address")
    )
    # Docker resolves the pinned endpoint's NetworkID only when a new container starts.
    # A disconnected container still requires the exact immutable HostConfig network ID.
    require(condition=attached or pending)
    require(
        condition=not endpoint.get("IPAMConfig")
        and not endpoint.get("DriverOpts")
        and not endpoint.get("Links")
    )
    require(
        condition=type(endpoint.get("GwPriority", 0)) is int and endpoint.get("GwPriority", 0) == 0
    )


def static(
    *,
    info: ContainerInfo,
    image: ImageInfo,
    net: NetworkInfo,
    name: str,
    require_running: bool = True,
) -> StaticBackend:
    """Bind an inspected static backend to trusted deployment image/network/configuration."""
    require(condition=name in {"backend-a", "backend-b"})
    _common(
        info=info, image=image, net=net, name=f"{PROJECT}-{name}-1", memory=128, cpus=500_000_000
    )
    config = info.Config
    require(
        condition=config.get("Image") == IMAGE
        and config.get("Cmd") == ["python", "-m", "mslb_mock_server"]
        and config.get("Entrypoint") == image.Config.get("Entrypoint")
    )
    expected_env = environment(value=image.Config.get("Env")) | {
        "BACKEND_ID": name,
        "LOGICAL_ADMISSION_MEM_MB": "1024",
        "MAX_INFLIGHT": "4",
        "TMPDIR": TMP_PATH,
    }
    require(condition=environment(value=config.get("Env")) == expected_env)
    actual = labels(value=config.get("Labels"))
    require(
        condition=actual.get("com.docker.compose.project") == PROJECT
        and actual.get("com.docker.compose.service") == name
        and actual.get("com.docker.compose.container-number") == "1"
        and actual.get("com.docker.compose.oneoff") == "False"
    )
    require(
        condition=actual.get("com.mslb.component") == "http-backend"
        and actual.get("com.mslb.backend-kind") == "static"
    )
    require(
        condition=all(
            key.startswith("com.docker.compose.")
            or key in {"com.mslb.component", "com.mslb.backend-kind"}
            or key in labels(value=image.Config.get("Labels"))
            for key in actual
        )
    )
    require(
        condition=info.State.get("Running") is True
        or (
            not require_running
            and info.State.get("Running") is False
            and info.State.get("Status") in ("exited", "dead")
        )
    )
    endpoint = mapping(value=mapping(value=info.NetworkSettings["Networks"])[net.Name])
    require(condition=set(strings(value=endpoint.get("Aliases"))) == {f"{PROJECT}-{name}-1", name})
    require(condition=config.get("StopTimeout") == 5)
    require(
        condition=config.get("Healthcheck")
        == {
            "Test": [
                "CMD",
                "python",
                "-c",
                "import urllib.request; "
                "urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2).close()",
            ],
            "Interval": 5_000_000_000,
            "Timeout": 3_000_000_000,
            "Retries": 15,
        }
    )
    return StaticBackend(
        name="backend-a" if name == "backend-a" else "backend-b", container_id=info.Id
    )


def dynamic(
    info: ContainerInfo, image: ImageInfo, net: NetworkInfo, intent: CreationIntent
) -> None:
    """Verify a dynamic container against the complete persisted creation intent."""
    require(condition=intent.image_id == image.Id and intent.network_id == net.Id)
    _common(
        info=info,
        image=image,
        net=net,
        name=intent.name,
        memory=intent.memory_mb,
        cpus=intent.nano_cpus,
    )
    body = intent.daemon_body()
    config = info.Config
    require(
        condition=config.get("Image") == intent.image_id
        and config.get("Cmd") in (None, [])
        and config.get("Entrypoint") == body["Entrypoint"]
    )
    require(
        condition=environment(value=config.get("Env"))
        == environment(value=image.Config.get("Env")) | environment(value=body["Env"])
    )
    require(
        condition=labels(value=config.get("Labels"))
        == labels(value=image.Config.get("Labels")) | labels(value=body["Labels"])
    )
    require(condition=config.get("Healthcheck") == image.Config.get("Healthcheck"))
    endpoint = mapping(value=mapping(value=info.NetworkSettings["Networks"])[net.Name])
    aliases = strings(value=endpoint.get("Aliases"))
    require(condition=intent.name in aliases and set(aliases) <= {intent.name, info.Id[:12]})


def deployment(
    image: ImageInfo, net: NetworkInfo, first: ContainerInfo, second: ContainerInfo
) -> DeploymentSpec:
    """Bind inspected containers to one validated installation and immutable image."""
    return DeploymentSpec(
        image_id=image.Id,
        network_id=net.Id,
        static=(
            static(info=first, image=image, net=net, name="backend-a"),
            static(info=second, image=image, net=net, name="backend-b"),
        ),
    )
