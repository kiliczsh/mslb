# Development

Read [AGENTS](../AGENTS.md), the [design](DESIGN.md) and relevant source before changing
behavior. The [assignment](assignment/requirements.md) owns the original requirements.
All code, comments, documentation and commit messages use English.

## Tools and setup

Runtime, builds, migrations and checks run in Linux containers. Install Docker with
Compose 2.24.4 or newer. Just is the developer command interface; Make delegates to it.
Full verification, isolated browser and fault runners additionally need PowerShell 7
(`pwsh`). Runtime startup needs no native host-memory script.
Host Python and Node are unnecessary for Docker development.

```sh
just build
just up
just status
```

The same `just up`/`just down` workflow works on Windows Docker Desktop and Linux Engine.
At least 8 GiB of reported engine memory and sufficient available Docker/Linux memory are
required. The guard monitors Docker capacity, service limits, working sets and OOM events;
it does not independently monitor Windows physical RAM. Do not change Docker Desktop
settings or prune unrelated resources.

For optional IDE navigation, install uv and run `just ide`. This creates ignored `.venv`
from the locked Python 3.13 workspace, installs editable packages and checks dependencies.
Recreate it after moving the checkout; do not copy an existing environment. No editor
interpreter configuration is maintained. Native execution does not replace Docker checks.

## Commands

Run `just --list` for the complete catalog. Source-copied tooling images rebuild before
static/offline checks so they inspect current code. Build jobs run sequentially.

| Task | Command |
| --- | --- |
| Resolve/preflight configuration | `just config`, `just check-deployment` |
| Build/start/stop | `just build`, `just up`, `just down` |
| Status/bounded logs | `just status`, `just logs api` |
| Lint/check/apply formatting | `just lint`, `just format-check`, `just format` |
| Source documentation/size policy | `just check-source-quality` |
| Python type checks | `just ty`, `just pyright` |
| Complete offline Python checks | `just test-python` |
| Focused offline Python tests | `just test-unit checkpoints`; omit the filter for all offline tests |
| Frontend lint/types/Node/Vitest | `just test-frontend` |
| Combined offline checks | `just check` |
| Real PostgreSQL/Redis integration | `just test-persistence` |
| Apply committed migrations | `just migrate` |
| Installed runtime/worker/REST/MCP | `just check-runtime`, `just check-worker`, `just check-api`, `just check-mcp` |
| Physical traffic/control/public workflow | `just check-traffic`, `just check-docker-proxy`, `just check-docker-public` |
| Check generated contracts | `just check-openapi-schemas`, `just check-mcp-schemas` |
| Regenerate contracts | `just generate-openapi-schemas`, `just generate-mcp-schemas` |
| Generate/verify canonical scaled bundle | `just bundle`, `just verify-bundle` |
| Chromium/project interruptions | `just check-browser`, `just check-controller-faults` |
| Separate standard/stress campaign | `just check-scenarios` |
| Read-only orphan maintenance plan | `just plan-output-maintenance` |
| Full Docker gate | `just verify` |

`check-python-local`, `check-windows`, `check-linux`, `lint-local`, `ty-local` and
`pyright-local` are optional host/static recipes. They require the local environment;
local Pyright also needs Node/npm. Default acceptance uses the Docker toolchain.

For a direct focused Docker test, rebuild its copied source first:

```sh
docker compose --profile tools build python-checks
docker compose --profile tools run --rm --no-deps python-checks pytest tests/unit/application/test_checkpoints.py
```

Schema generation writes `docs/contracts/openapi/` or `docs/contracts/mcp/`. Review the
complete diff; inventories must contain exactly their owned files. Source formatters
exclude assignment assets, supplied validators, fixture files and locked metadata.

## Frontend development

After building and starting the stack, run `just dev` and open
[the Vite UI](http://127.0.0.1:5173). Source is mounted read-only; locked dependencies stay
in the image. The watcher polls Windows mounts every 150 ms. API/docs are proxied on the
same origin, with only approved loopback development origins translated. Stop with
`just dev-stop`. Rebuild the frontend image when dependencies or their locks change.

## Coding conventions

Use UTF-8, LF, final newlines, double quotes and a 100-column target. Python uses four
spaces; TypeScript uses two spaces and semicolons. Ruff owns Python formatting/imports
and lint; Biome owns frontend formatting/lint. Do not suppress diagnostics to pass checks.

Python modules/functions/variables use snake_case, classes PascalCase and constants
UPPER_SNAKE_CASE. Annotate function parameters and returns, including tests. Use modern
built-in generics, explicit package imports and narrow exceptions. Make new application
APIs keyword-only and pass named arguments wherever the runtime callable supports them.
Positional-only built-ins, variadic APIs and protocol-required calls are exceptions.
Use named `HTTPStatus` values for HTTP behavior and units in operational constant names.

Use immutable dataclasses for domain values, Pydantic for transport validation and
SQLAlchemy models inside persistence. Keep CPU simulation outside the API event loop;
async handles I/O. Shared packages must not import application adapters. Principal service
classes have descriptive dedicated modules; related small DTOs may share a subject module.
Avoid import-time side effects and cycles; preserve established public imports when moving code.

Public modules, classes, functions and methods require meaningful Google-style docstrings.
Explain purpose, failure behavior, units and invariants. Comments explain non-obvious
reasoning. `check_source_quality.py` limits functions to 120 code lines and classes to 400,
excluding blank/comment/docstring lines and immutable historical migrations. Nested units
are checked independently. Extract responsibilities rather than cosmetic wrappers.

React uses function components, named exports and explicit props. Keep fetching/retries,
state and rendering responsibilities clear. Components/types use PascalCase; hooks use
`useSomething`; local variables/functions use camelCase. Wire DTOs keep snake_case.
New TypeScript APIs with multiple options take a named options object. Strict TypeScript,
including unchecked-index and exact-optional checks, validates the frontend. Parse external
unknown data through Zod. Preserve exact fraction components as strings/BigInt; charts may
approximate for display. Avoid routine any, unchecked casts and non-null assertions.

## Deployment and access

Root `docker-compose.yml` includes `compose.docker-simulation.yaml` and `compose.dev.yaml`.
Browser/scenario Compose files are explicit isolated overlays. Default services run the
product; `tools` selects verification jobs and `dev` selects Vite. PostgreSQL, Redis and
control services expose no host ports. Only the restricted proxy mounts the Docker socket.

The shared Python Dockerfile separates locked dependency, check and runtime stages.
Runtime roles do not contain compilers/check tools. Frontend/browser check images are
separate from the web image. Effective deployment preflight enforces role completeness,
private mounts/credentials, loopback ports, approved RAM/no-swap caps, CPU/PID bounds and
container isolation. PostgreSQL retains its upstream volume-initialization exception.

After rebuilding runtime code, recreate a coherent installation:

```sh
docker compose up --no-build --force-recreate -d --wait
```

Local demo authentication defaults to disabled. For authenticated local evaluation, copy
`.env.example` to ignored `.env`, set `AUTH_ENABLED=true` and a random URL-safe 32–256-character
`AUTH_TOKEN`, then recreate the services. Credentials belong only in bearer headers, never
URLs, source or logs. Host/Origin allowlists apply before parsing; there is no wildcard
CORS or forwarded-header trust. Public deployment additionally requires an HTTPS gateway
and separate deployment acceptance; it is not performed by this repository workflow.

[ReDoc](http://127.0.0.1:8080/api/docs) is served locally with locked assets and branding.
The raw `/api/v1/openapi.json` schema requires authentication when enabled. The docs token
loader uses a single request and does not save credentials. Documentation alone permits
its required computed styles/embedded fonts; the dashboard retains its stricter CSP.
REST details are maintained in ReDoc rather than repeated as a Markdown endpoint catalog.

## Tests

| Location | Boundary |
| --- | --- |
| `tests/unit/engine/` | Exact arithmetic, lifecycle, strategies, scaling, replay and process determinism |
| `tests/unit/contracts/` | Inputs, transport/schema inventories and documentation access |
| `tests/unit/application/` | Files, checkpoints, artifacts, modes, queries and downloads |
| `tests/unit/platform/` | Resource/control policy, runtime/source boundaries and fake transports |
| `tests/integration/` | Actual PostgreSQL/Redis, transactions, leases/fences, recovery and HTTP/MCP |
| `apps/web/src/**/*.test.*` | React components, state, validation and transport utilities |
| `apps/web/tests/` | Node/download-worker behavior |
| `apps/web/browser/` | Real Chromium workflows through gateway/API/worker/database |
| `scripts/check_*.py` | Installed services, physical traffic, control and artifact verification |

Bare pytest selects offline tests; integration is named explicitly. Integration fixtures
use disposable owned schemas and temporary files. Isolated browsers own separate projects,
volumes and networks and clean up in finally. Guarded physical browser checks use the
standard installation. Mocks prove local contracts; only actual service/browser checks
prove their respective integrated boundaries.

`just verify` runs `scripts/verify.ps1`: six builds, deployment/runtime isolation,
offline/integration/live REST/MCP, four-strategy traffic/scaling, project faults, Redis
fallback, maintenance planning and canonical bundle verification. Browser acceptance runs
last. Nonzero exits and unexpected warning/deprecation diagnostics fail the gate. Local
receipts under ignored `.cache/verification/` are diagnostic evidence, not tracked source.
The scenario campaign is separate from the normal gate.

## Scenario delivery artifacts

The tracked [artifact index](../data/test-artifacts/manifest.json) identifies the
24 primary synthetic scenarios and their actual HTTP/worker executions. Each scenario
directory contains source inputs/configuration, expectations, submission/completion
receipts and a complete independently verified bundle under bundle/. The sequential
variant is nested under 16_execution_modes/sequential/. The top-level index also records
two repeat/hash checks, 32 invalid inputs and standard-profile rejection of the 12k/50k
workloads. Checksums cover every exported file; artifact bytes are excluded from Git
newline conversion and Docker build contexts.

These are curated delivery examples, not files loaded automatically into the dashboard.
They were produced in fresh isolated Compose projects without published ports or shared
production data. The stress installation admits 50,000 requests and intentionally disables
the production resource guard; finite cgroup measurements are recorded separately. This
campaign does not establish production guard, physical traffic or full-gate acceptance.
The November 7 delivery campaign passed all 27 durable executions, including the
sequential variant and two repeats, all 32 invalid cases and both standard-capacity
refusals. No campaign container OOM kills or restarts were observed. Exported file
checksums were rechecked and the copied sequential bundle passed Docker replay and
the unchanged supplied validator. No full test suite or Docker gate was run.
Raw logs and effective configurations remain ignored in .cache/scenarios/.

Regenerate the isolated campaign with just check-scenarios; it prints a fresh evidence
directory under .cache/scenarios. Review the new successful report and bundle hashes
before replacing a curated snapshot; never mix receipts from different campaigns.

To verify a delivered bundle without placing artifacts in the tools image:

```sh
docker compose --profile tools run --rm --no-deps --volume ./data/test-artifacts:/evidence:ro python-checks python scripts/run_bundle.py verify /evidence/05_equal_cpu_sharing/bundle
```

## Verification record

The November 8, 2026 host-memory removal passed 54 focused platform unit tests and 27
PostgreSQL integration tests covering resource evidence, admission, publisher recovery and
readiness. Python lint/format, ty/Pyright, frontend Biome/TypeScript, schema inventories,
source boundaries and effective Compose preflight passed. On Windows Docker Desktop,
`just up`, repeated `just up`, `just down` and restart worked without a host feeder;
all existing volumes and a completed HTTP simulation's bundle digest survived restart.
The HTTP upload/run/download/SSE workflow and live read-only probe checks passed. Runtime
containers were healthy with no OOM kills or restarts and no warnings/errors in the sampled
API/worker/guard/controller logs. One readiness request returned 503 after the extra live
diagnostic probe; the following 12 one-second samples returned 200. Sampling/TTL tuning
remains deferred. No full test suite, browser campaign or full gate was run for this change.

The last completed maintenance gate on November 7, 2026 passed all 32 phases in 431.926
seconds on Windows Docker Desktop/Linux containers with retained user volumes: 487 offline
plus 157 integration Python tests, 4 Node, 59 Vitest and 8 Chromium workflows. The final
Windows empty-filter Just fix separately passed all 487 offline tests; its repeat full
gate was interrupted by user instruction. Earlier runtime code also passed a distinct
Ubuntu Engine on WSL2 32-phase gate in 457.09 seconds; that result predates the maintenance.
The earlier Windows scenario campaign covered 24 primary scenarios, one sequential variant,
32 invalid inputs and two repeats: 27 actual durable executions with downloaded/replayed
bundles. Its separate isolated stress checks exercised 12,000 and 50,000 requests, repeat
hashes, production-profile refusals and measured container peaks without campaign OOM/restarts.
That finite campaign was not rerun on Ubuntu or as part of this documentation change.
The canonical scaled bundle digest was
`6cfcb6adb8153f018903e0441c406c9d74e1acf54865cef3240f1f3c7de7d220`.
The documentation/path consolidation has not received another full test/gate run, per the
current explicit request for fast local changes.

These results do not imply an instrumented coverage percentage, Firefox/WebKit coverage,
formal WCAG conformance, multi-hour soak, bare-metal Linux or public HTTPS acceptance.
Broader security/resource/recovery acceptance and CI/hooks remain follow-up work.
Observability is deferred; Grafana is excluded.

## Troubleshooting and recovery

- **New work refused, saved data readable:** inspect `just logs resource-guard` and
  `/api/v1/health/ready`. Check advancing Docker measurements, service health
  and available Docker/Linux memory. Fresh cool samples are required before admission resumes.
- **Upload rejected:** check CSV shape, normalized unique IDs, 10 MiB and 10,000-row limits.
  The isolated stress campaign selects its own 50,000-request profile; do not widen production
  limits to make an individual input pass.
- **Redis unavailable:** PostgreSQL remains authoritative; Redis provides disposable hints.
  Redis pressure/OOM still participates in resource safety.
- **Saved files/data:** `docker compose down` retains named volumes. Do not run `down -v`
  as routine troubleshooting. Never release reservations through ad hoc SQL.
- **Unexpected API failure:** use `X-Request-ID` and bounded container diagnostics. Errors
  exclude secrets, raw input, internal paths and stack traces from public responses.

After fixing the installation, retry an expired stalled physical cleanup with:

```sh
docker compose --profile tools run --rm --no-deps docker-recovery python -m mslb_controller.recovery --run-id RUN_UUID
```

Active ownership is rejected. A new fence and exact resource identity checks protect cleanup;
reservations remain held until it resolves. `just plan-output-maintenance` is read-only.
Explicit orphan removal is bounded, requires idle execution and protects published history;
inspect its plan before any operator application.

## Local changes and delivery

Work on the current local `main` branch. Preserve requirements, supplied evaluator and core
fixture bytes. Keep changes scoped, review staged diffs and never commit secrets, dependencies,
`.venv`, `.cache` or generated runtime artifacts. `backlog.md` remains a local work note.
Update the owning current document when behavior changes; avoid recreating duplicate guides.
`docs/AI-USAGE.md` is the tracked one-time delivery narrative, not a per-commit journal.

Normal behavior changes require relevant Docker checks and the warning-free full gate unless
the user explicitly narrows verification. The current fast-change instruction forbids another
full test run; record exactly what was checked without inferring a pass. Make only local
commits. Do not push, publish or deploy.
