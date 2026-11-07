"""One manifest source for MCP discovery and deterministic DTO-derived schemas."""

from dataclasses import dataclass

from mslb_contracts.catalog import ServerView
from mslb_contracts.docker_artifacts import HttpRecord, RuntimeRecord
from mslb_contracts.docker_views import (
    DockerEventArguments,
    DockerRunCommand,
    DockerRunDetail,
    DockerRunView,
    DockerSimulationCreate,
)
from mslb_contracts.durability import PersistedEvent
from mslb_contracts.fractions import FractionValue
from mslb_contracts.http import (
    CompareRequest,
    ErrorEnvelope,
    RunDetail,
    SimulationAccepted,
    SimulationCreate,
    StrategyView,
)
from mslb_contracts.inputs import ServerInput
from mslb_contracts.mcp import (
    CompareStrategiesArguments,
    EventArguments,
    EventResource,
    MetricArguments,
    NoArguments,
    PageArguments,
    RunCommand,
    RunReference,
    ServerDelete,
    ServerDeleted,
    ServerReference,
    ServerResource,
    ServerUpdate,
    WaitArguments,
)
from mslb_contracts.metric_views import ChartView, ComparisonView, MetricsView
from mslb_contracts.pagination import Page
from mslb_contracts.run_views import RunView, WaitResult
from pydantic import BaseModel

DIALECT = "https://json-schema.org/draft/2020-12/schema"
CONTRACT_VERSION = "2.3.0"


@dataclass(frozen=True, slots=True, kw_only=True)
class ToolContract:
    """Bind one MCP tool name to its strict arguments, output schema and annotations."""

    name: str
    description: str
    input: type[BaseModel]
    outputs: tuple[type[BaseModel], ...]
    read_only: bool = True


TOOLS = (
    ToolContract(
        name="list_servers",
        description="Read a bounded ID-ordered server page.",
        input=PageArguments,
        outputs=(Page[ServerView],),
    ),
    ToolContract(
        name="get_server",
        description="Read one normalized server and version.",
        input=ServerReference,
        outputs=(ServerView,),
    ),
    ToolContract(
        name="create_server",
        description="Create a logical server for future runs.",
        input=ServerInput,
        outputs=(ServerView,),
        read_only=False,
    ),
    ToolContract(
        name="update_server",
        description="Update capacities using the expected server version.",
        input=ServerUpdate,
        outputs=(ServerView,),
        read_only=False,
    ),
    ToolContract(
        name="delete_server",
        description="Delete a version-matched server from future configuration.",
        input=ServerDelete,
        outputs=(ServerDeleted,),
        read_only=False,
    ),
    ToolContract(
        name="list_strategies",
        description="Read all four logical scheduling strategies.",
        input=NoArguments,
        outputs=(Page[StrategyView],),
    ),
    ToolContract(
        name="run_simulation",
        description=(
            "Durably enqueue a logical run and return promptly; batch IDs in wait_for_simulations."
        ),
        input=RunCommand,
        outputs=(SimulationAccepted,),
        read_only=False,
    ),
    ToolContract(
        name="wait_for_simulations",
        description=(
            "Read-only wait for 1–8 unique runs, up to 25 seconds (default 20). "
            "Returns compact persisted statuses when all are terminal or the wait expires. "
            "Opt into MCP progress for persisted changes; expiry/disconnect never cancels runs. "
            "Repeat a timed-out wait; reserve get_simulation_status for recovery "
            "or explicit checks."
        ),
        input=WaitArguments,
        outputs=(WaitResult,),
    ),
    ToolContract(
        name="get_simulation_status",
        description="Read persisted status and completed validation metadata.",
        input=RunReference,
        outputs=(RunDetail,),
    ),
    ToolContract(
        name="get_simulation_metrics",
        description="Read exact metrics or a bounded chart range.",
        input=MetricArguments,
        outputs=(MetricsView, ChartView),
    ),
    ToolContract(
        name="get_simulation_events",
        description="Page committed events with inclusive tick filters.",
        input=EventArguments,
        outputs=(Page[PersistedEvent],),
    ),
    ToolContract(
        name="compare_simulations",
        description="Compare compatible completed runs against the first baseline.",
        input=CompareRequest,
        outputs=(ComparisonView,),
    ),
    ToolContract(
        name="cancel_simulation",
        description="Request cancellation through the durable state machine.",
        input=RunReference,
        outputs=(RunView,),
        read_only=False,
    ),
)
TOOLS = (
    *TOOLS,
    ToolContract(
        name="run_docker_simulation",
        description=(
            "Durably queue bounded real HTTP traffic. Physical scaling uses real seconds "
            "and actual containers; results are operationally nondeterministic."
        ),
        input=DockerRunCommand,
        outputs=(SimulationAccepted,),
        read_only=False,
    ),
    ToolContract(
        name="list_docker_simulations",
        description="Read a bounded operational-only history page.",
        input=PageArguments,
        outputs=(Page[DockerRunView],),
    ),
    ToolContract(
        name="get_docker_simulation",
        description="Read durable lifecycle, admission and verified final HTTP results.",
        input=RunReference,
        outputs=(DockerRunDetail,),
    ),
    ToolContract(
        name="get_docker_events",
        description="Page verified completed HTTP observations with real elapsed seconds.",
        input=DockerEventArguments,
        outputs=(Page[HttpRecord],),
    ),
    ToolContract(
        name="cancel_docker_simulation",
        description="Request cancellation and retain reservations through owned cleanup.",
        input=RunReference,
        outputs=(DockerRunDetail,),
        read_only=False,
    ),
)
TOOL_BY_NAME = {tool.name: tool for tool in TOOLS}

RESOURCES = (
    ("servers", "mslb://servers", ServerResource),
    ("strategies", "mslb://strategies", Page[StrategyView]),
    ("run-summary", "mslb://runs/{run_id}/summary", RunDetail),
    ("run-events", "mslb://runs/{run_id}/events", EventResource),
    ("docker-summary", "mslb://docker-runs/{run_id}/summary", DockerRunDetail),
)
PROMPTS = (
    ("analyze_performance", "Analyze a persisted run using actual tool results.", RunReference),
    (
        "compare_strategies",
        "Propose four comparable runs for an existing dataset.",
        CompareStrategiesArguments,
    ),
    (
        "suggest_autoscaling",
        "Propose simulated scaling settings using measured run results.",
        RunReference,
    ),
)
COMMON = {
    "error": ErrorEnvelope,
    "pagination": PageArguments,
    "server": ServerView,
    "run": RunDetail,
    "run-create": SimulationCreate,
    "metrics": MetricsView,
    "fraction": FractionValue,
    "event": PersistedEvent,
    "docker-run": DockerRunDetail,
    "docker-runtime": RuntimeRecord,
    "docker-create": DockerSimulationCreate,
    "docker-event": HttpRecord,
}


def schema(model: type[BaseModel]) -> dict[str, object]:
    """Produce the JSON input schema for a registered typed MCP argument model."""
    return {"$schema": DIALECT, **model.model_json_schema()}


def output_schema(tool: ToolContract) -> dict[str, object]:
    # Merge definitions generated from the same DTO classes; reject accidental
    # name collisions instead of silently publishing ambiguous references.
    """Advertise successful result variants and the bounded MCP error envelope."""
    definitions: dict[str, object] = {}
    variants: list[dict[str, object]] = []
    for model in (*tool.outputs, ErrorEnvelope):
        generated = model.model_json_schema()
        for key, value in generated.pop("$defs", {}).items():
            if key in definitions and definitions[key] != value:
                raise ValueError("Conflicting generated schema definitions")
            definitions[key] = value
        variants.append(generated)
    return {"$schema": DIALECT, "type": "object", "anyOf": variants, "$defs": definitions}
