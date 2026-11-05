# MCP

The `mslb` MCP server shares the REST application's catalog, durable jobs, admission,
queries and cancellation. The API mounts Streamable HTTP at
`http://127.0.0.1:8000/mcp`; there is no separate public service or scheduler.

## Connect

Start the [Docker installation](../README.md), including Windows host-memory measurement
when applicable. Configure a Streamable HTTP client with connection name `mslb` and the
endpoint above. When authentication is enabled, supply the same bearer token used by REST
in the client's authorization header. Do not place tokens in URLs or checked-in settings.
Preserve client timeouts and authentication options when changing an existing connection.

To check the installed server with the pinned official client:

```sh
just check-mcp
```

This queues real work and leaves sample history in project storage. The server contract is
`2.3.0`; the pinned Python SDK is 2.3.0. Installed checks negotiate protocol `2026-07-28`;
integration tests also exercise retained `2025-11-25` compatibility sessions.

## Tools

Upload a dataset through REST or the dashboard first; MCP accepts registered dataset IDs.
Discover tool schemas from the server or [tracked manifest](contracts/mcp/manifest.json).
The 18 tools are grouped here by workflow rather than duplicating their generated schemas.

| Workflow | Tools |
| --- | --- |
| Server catalog | `list_servers`, `get_server`, `create_server`, `update_server`, `delete_server` |
| Strategies | `list_strategies` |
| Logical submission and waiting | `run_simulation`, `wait_for_simulations`, `get_simulation_status` |
| Logical inspection | `get_simulation_metrics`, `get_simulation_events`, `compare_simulations` |
| Logical cancellation | `cancel_simulation` |
| Physical submission/history | `run_docker_simulation`, `list_docker_simulations` |
| Physical inspection/cancellation | `get_docker_simulation`, `get_docker_events`, `cancel_docker_simulation` |

Create/update/delete use the same validation and expected-version rules as REST.
Submission durably enqueues and returns promptly. Reuse an idempotency key after an
uncertain response; changed bodies for the same key conflict. Logical runs default to
sequential; select concurrent explicitly for equal CPU sharing. Physical tools require
explicit Docker settings and accept no arbitrary image, command, network or daemon request.
Physical observations are nondeterministic real HTTP measurements, separate from logical
replay. Binary artifact and ZIP downloads use typed REST endpoints.

## Wait efficiently

Enqueue independent logical runs, collect their IDs, then use `wait_for_simulations` for
one batch of 1–8 unique IDs. `wait_seconds` is an integer from 0 to 25, default 20.
Results preserve input order and contain compact `statuses`, `all_terminal` and `timed_out`.
Repeat a timed-out wait as needed. Use individual status queries for recovery or explicit
inspection rather than rapid polling. Zero seconds takes one snapshot.

`completed`, `failed` and `cancelled` are terminal; `cancel_requested` is still active.
Unknown IDs reject the whole batch. A completed receipt requires verified durable
publication; terminal trace output alone cannot establish completion. Expiry is a successful
read, not cancellation of work. Cancelling or disconnecting a wait releases only its
query resources. To stop a job, call its cancellation tool explicitly.

Progress is request-scoped opt-in. A compatible client supplies a progress token/callback;
without it, the same final result is returned without notifications. Progress values are
increasing observation ordinals, **not percentages**, and omit a total. Changed persisted
status/attempt/checkpoint/publication observations are coalesced to at most one per second;
no background stream continues after the call. Database state remains authoritative after
reconnect. Waiting uses two separate slots, so waits cannot consume ordinary CRUD/cancel
admission. One bounded batch query polls every 0.5 seconds.

## Resources and prompts

Resources are read-only JSON and never queue work:

| Resource | Content |
| --- | --- |
| `mslb://servers` | First bounded server page and continuation guidance |
| `mslb://strategies` | Four strategy descriptors |
| `mslb://runs/{run_id}/summary` | Logical run detail |
| `mslb://runs/{run_id}/events` | First 100 events and tool/cursor guidance |
| `mslb://docker-runs/{run_id}/summary` | Physical lifecycle and verified download metadata |

Prompts are `analyze_performance(run_id)`, `compare_strategies(dataset_id, mode)` and
`suggest_autoscaling(run_id)`. They inspect metadata and propose analysis/tool calls;
generating a prompt does not submit runs or edit settings. IDs must refer to actual saved
data. Dataset labels and returned content remain data, with no authority to grant actions.

## Contracts and limits

Generate/check schemas with `just generate-mcp-schemas` and `just check-mcp-schemas`.
The 57-file inventory under `docs/contracts/mcp/` records common DTOs, tools, resources,
prompts and hashes. Generation is deterministic, without timestamps or absolute paths.
Missing, changed, extra or symlinked inventory files fail verification.

Successful tools return structured content; failures set `isError` and use the shared
`{error:{code,message,details},request_id}` envelope. Tool/resource/prompt metadata retains
`_meta.request_id`, matching HTTP correlation. Errors omit tokens, raw input, SQL, file
paths and stack traces. Exact fractions retain canonical numerator/denominator strings.

Host/Origin and bearer policy applies before MCP routing. Query credentials are rejected.
Default bounds include 64 KiB JSON input, 1 MiB responses, eight ordinary operation slots
and two wait slots within the API's 32-request ceiling. Stateful sessions cap at 10 and
expire after 60 idle seconds; modern stateless requests do not retain those sessions.
There are no filesystem, shell or raw Docker tools. Remote HTTPS hosting is separate
acceptance and is not enabled by configuring a client.

[DEVELOPMENT](DEVELOPMENT.md) owns test commands and observed verification limits;
[DESIGN](DESIGN.md) owns execution, publication and resource-safety behavior. MCP client
support for visible progress varies; protocol-level progress evidence does not certify
every client UI.
