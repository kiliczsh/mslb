"""One manifest source for MCP discovery and deterministic DTO-derived schemas."""

from dataclasses import dataclass

from mslb_contracts.catalog import ServerView
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
)
from mslb_contracts.queries import ChartView, ComparisonView, MetricsView, Page, RunView
from pydantic import BaseModel

DIALECT = "https://json-schema.org/draft/2020-12/schema"
CONTRACT_VERSION = "1.0.0"


@dataclass(frozen=True, slots=True)
class ToolContract:
    name: str
    description: str
    input: type[BaseModel]
    outputs: tuple[type[BaseModel], ...]
    read_only: bool = True


TOOLS = (
    ToolContract(
        "list_servers", "Read a bounded ID-ordered server page.", PageArguments, (Page[ServerView],)
    ),
    ToolContract(
        "get_server", "Read one normalized server and version.", ServerReference, (ServerView,)
    ),
    ToolContract(
        "create_server",
        "Create a logical server for future runs.",
        ServerInput,
        (ServerView,),
        False,
    ),
    ToolContract(
        "update_server",
        "Update capacities using the expected server version.",
        ServerUpdate,
        (ServerView,),
        False,
    ),
    ToolContract(
        "delete_server",
        "Delete a version-matched server from future configuration.",
        ServerDelete,
        (ServerDeleted,),
        False,
    ),
    ToolContract(
        "list_strategies",
        "Read all four logical scheduling strategies.",
        NoArguments,
        (Page[StrategyView],),
    ),
    ToolContract(
        "run_simulation",
        "Durably enqueue a logical run; poll status separately.",
        RunCommand,
        (SimulationAccepted,),
        False,
    ),
    ToolContract(
        "get_simulation_status",
        "Read persisted status and completed validation metadata.",
        RunReference,
        (RunDetail,),
    ),
    ToolContract(
        "get_simulation_metrics",
        "Read exact metrics or a bounded chart range.",
        MetricArguments,
        (MetricsView, ChartView),
    ),
    ToolContract(
        "get_simulation_events",
        "Page committed events with inclusive tick filters.",
        EventArguments,
        (Page[PersistedEvent],),
    ),
    ToolContract(
        "compare_simulations",
        "Compare compatible completed runs against the first baseline.",
        CompareRequest,
        (ComparisonView,),
    ),
    ToolContract(
        "cancel_simulation",
        "Request cancellation through the durable state machine.",
        RunReference,
        (RunView,),
        False,
    ),
)
TOOL_BY_NAME = {tool.name: tool for tool in TOOLS}

RESOURCES = (
    ("servers", "mslb://servers", ServerResource),
    ("strategies", "mslb://strategies", Page[StrategyView]),
    ("run-summary", "mslb://runs/{run_id}/summary", RunDetail),
    ("run-events", "mslb://runs/{run_id}/events", EventResource),
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
}


def schema(model: type[BaseModel]) -> dict[str, object]:
    return {"$schema": DIALECT, **model.model_json_schema()}


def output_schema(tool: ToolContract) -> dict[str, object]:
    # Merge definitions generated from the same DTO classes; reject accidental
    # name collisions instead of silently publishing ambiguous references.
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
