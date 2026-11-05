# mslb implementation plan and consolidated build brief

Development began with an initial plan prepared before application implementation:
understand the assignment, define scope and constraints, establish the Docker foundation,
and then implement and verify the product in stages. The plan was refined as implementation,
review and verification exposed concrete trade-offs.

This document presents the updated plan as a self-contained build brief. It combines the
initial direction with later accepted decisions; it is not a verbatim copy of the initial
prompt, or a claim that every final architectural detail was decided at the start.
[QUESTIONNAIRE](QUESTIONNAIRE.md) records adopted choices and their origins,
[DESIGN](DESIGN.md) describes the implemented system, and [AI-USAGE](AI-USAGE.md)
explains the planning, direction, reviewed alternatives and verification sequence.

The acceptance instructions below describe how to build and verify the agreed project;
they do not override an active user's instruction to defer tests or remote operations.

---

Build **mslb**, a fully Dockerized deterministic load-balancer simulation and functional
server-management dashboard. Use the unchanged assignment at
`docs/assignment/requirements.md` as the original requirements authority. Preserve the
supplied PDF, core JSON/CSV/trace and validator byte-for-byte. Do not change supplied
data to make tests pass. Implement the mandatory task, all four chosen bonuses and the
engineering additions below as one integrated project. Use English for UI, code,
comments, documentation and commits.

## Product scope

Provide heterogeneous-server management, bounded CSV ingestion, selectable strategies,
durable simulation submission/cancellation/history, exact metrics, saved playback,
compatible-run comparison and reproducible artifact downloads. Include a guided demo
using actual application runs rather than fabricated results. Add MCP access to the same
application services, a guarded real HTTP/Docker workspace and bounded resource safety.
Observability is deferred; exclude Grafana and do not add placeholder telemetry services.
Public hosting, Kubernetes, multi-host orchestration and third-party identity providers
are outside the requested deliverable. Keep all work local unless explicitly authorized
otherwise; never push, publish or deploy on the user's behalf.

## Stack, organization and startup

Use Python 3.13 with a locked uv workspace, FastAPI, Pydantic, SQLAlchemy/asyncpg and
Alembic. Use PostgreSQL as durable truth and Redis only for ephemeral notifications.
Use React, strict TypeScript, Vite, pnpm, Biome, TanStack Query, Zustand and Zod for the
dashboard. Use Ruff for Python lint/format, ty and Pyright for types, pytest for Python,
Vitest/Node for frontend utilities/components and Playwright Chromium for browser flows.
Pin dependencies/images and preserve frozen-lock builds.

Build application code at the repository root:

- `apps/api`, `apps/worker`, `apps/container-controller`, `apps/mock-server` and `apps/web`.
- `packages/engine`, `packages/contracts`, `packages/application` and `packages/persistence`.
- `infra/` for Docker builds, `scripts/` for operational/check adapters and `tests/` for
  grouped offline Python and actual PostgreSQL/Redis integration checks.
- Root `docker-compose.yml` with explicit physical/developer/check overlay files.

Use `mslb-*` distributions/images and `mslb_*` imports in one eight-package workspace.
Shared packages must not import application adapters; the engine is standard-library-only.
Separate composition, transport, immutable domain values, orchestration and persistence.
Give principal classes descriptive modules; group related DTOs and avoid placeholders.

Docker and Compose are the runtime/build/test prerequisites; no host Python, Node or
manual dependency installation is required. Default startup includes PostgreSQL, Redis,
API, logical worker, Nginx dashboard, two private backends, restricted Docker proxy,
physical controller and resource guard. Bound migrations/credential initialization as
one-shot tasks. Publish only loopback application/API ports. Support Windows Docker
Desktop with Linux containers and Linux Docker Engine. An ignored locked `.venv` is
optional for IDE navigation; add no VS Code interpreter configuration.

## Input contract and server management

Retain server fields `id`, `cpu_units_per_tick`, `mem_mb`, `rate_limit_per_sec` and request
CSV columns `t`, `request_id`, `work_units`, `mem_mb`. Enforce strict positive bounded
integer capacities/work/memory, nonnegative arrivals and unique normalized identifiers.
Normalize IDs with trimming and NFC; reject NUL/dot-segment identifiers and normalized
collisions. Sort servers lexically and requests by `(t, request_id)`.

Parse bounded CSV incrementally with UTF-8/BOM/CRLF, quoting, unsorted rows and extra
columns. Reject malformed widths, missing/duplicate headers and invalid encoding without
echoing values. Preserve original provenance and normalized digests separately. Limits:
1–64 servers, 10,000 requests, 256 KiB server JSON, 10 MiB CSV, 2 KiB rows, 256-byte fields.

Implement dashboard/API server view/add/edit/delete and atomic whole-cluster replacement.
Use optimistic versions and stale-write conflicts; retain history across delete/recreate.
Changes apply only to future runs. Freeze dataset identity, server versions/capacities,
fully resolved settings and engine identity at submission. Dataset display names must
never select managed filesystem paths. MCP consumes registered dataset IDs.

## Deterministic execution contract

Implement two explicit modes: default `sequential` permits one active request per server;
`concurrent` shares each server's CPU equally across its active requests. Keep the
unchanged supplied evaluator as the sequential static compatibility check. Independently
replay concurrent and scaled execution; label validator applicability correctly rather
than claiming the serial evaluator accepted a shared-CPU trace. Preserve reading of
legacy `sequential_compat` snapshots without rewriting their bytes/hashes; reject that
legacy spelling in new public submissions.

One tick equals one second. Boundary events at `t` describe the start of `[t,t+1)`.
At each boundary apply pending scale membership; complete prior work/release memory;
append arrivals; drop impossible/expired work; schedule; drop residual queue overflow;
evaluate scaling and account the next interval. Execute no extra terminal interval.

Maintain stable pending order with blocked-head bypass and surviving-order preservation.
Permanently impossible memory drops immediately, accounting for possible future template
capacity. An optional timeout expires at `arrival + timeout` before scheduling. After
immediate starts, optional queue capacity retains oldest pending requests and drops
newest overflow; zero capacity still permits immediate starts. Null disables each optional
queue policy within hard installation bounds.

Reserve memory throughout execution. Per-server successful starts at a boundary must not
exceed `rate_limit_per_sec`; reset counters once per tick. Never run one request on two
servers. Enforce memory, start-rate and mode eligibility before choosing a server.
Implement interchangeable selectors:

- First Fit: first eligible lexical ID.
- Round Robin: first eligible ID after the last successful-start anchor, wrapping;
  unsuccessful selection does not advance it.
- Least Connections: fewest active requests, then ID.
- Most Available Memory: greatest unreserved memory, then ID.

Use exact rational arithmetic for CPU and completion. Freeze `capacity / active_count`
for each interval; cap consumption by remaining work and do not redistribute unused
shares inside that interval. Observe completion at the next boundary. Fast-forward idle
periods analytically while retaining capacity/time integrals and horizon accounting.

Serialize only the four assignment request event types into compact UTF-8/LF
`run.jsonl`. At each boundary order finishes, arrivals, drops, starts, then request/server
IDs. Keep operational IDs, timestamps and explanations out of canonical request lines.
Same normalized inputs/settings/engine version must produce the same bytes across input
ordering, process hash seeds and repeat execution. Single-file output must preserve an
existing good target on failure and replace atomically only after verification.

Bound horizon to 100,000 by default/1,000,000 maximum, samples to 1,000,000, fractions
to 4,096 bits and historical metric series to 10,000. Exceeding bounds fails explicitly.

## Metrics, scaling and results

Accumulate exact cluster/per-server counts, useful/allocated CPU, integrated capacity and
memory, utilization, queue peak/integral, wait/execution/response distributions, makespan,
throughput and drop fraction. Use nearest-rank P50/P95, null empty statistics and zero
throughput for zero makespan. Include initial idle capacity in utilization; do not count
dropped work as successful latency. Transport exact noninteger/unsafe-sized values as
reduced ASCII decimal-string numerator/denominator objects; validate/parse using BigInt
in JavaScript. Only presentation charts may approximate them.

Stream charts into at most 2,000 buckets with covered duration, exact means and min/max;
preserve membership gaps. Compare 2–8 completed runs against their explicit first baseline,
matching dataset/initial cluster/mode/engine/noncompared settings; allow strategy/scaling
differences. Zero baselines yield null percentage change; declare no universal winner.

Implement optional deterministic simulated scaling with a frozen positive-capacity
template. Defaults: five completed samples, CPU out 4/5/in 1/5, nonempty queue threshold
1, drop-ratio threshold 1/10, three-tick cooldown and one-server steps. Resolve min to
initial count and max to `min(64, initial + 3)`. Evaluate completed CPU/drop observations
and current post-scheduling queue; scale-out takes precedence with ordered CPU/queue/drop
triggers. Scale-in requires low CPU, empty queue, no recent drops and idle dynamic
members. Never remove originals/busy servers. Apply changes next boundary and wait until
`decision_tick + cooldown + 1` for the next decision. Use monotonic deterministic
`autoscale-000001` IDs and highest-ID-first removals. Finish pending changes and terminate
without endlessly scaling an empty workload. Store/replay a separate canonical decision
sidecar; do not pollute request trace events.

Publish normalized inputs, snapshot, trace, metrics, timeline, drop details, optional
decision sidecar and manifest together. Bind lengths/digests and reproduce identical
bundle content. Independent replay must reconstruct lifecycle, queue, resource and scaling
semantics without invoking runtime scheduling, CPU updates or the autoscaler. Use separate
hand-calculated strategy and metric examples as regression oracles.

## Durable services and bounded storage

Atomically register queued work, immutable snapshot reference, scoped 24-hour idempotency
and outbox. Same key/body returns the same registration; changed body conflicts. Keep
PostgreSQL authoritative despite Redis loss, duplicates or expired hints. Bound queues,
pools, payloads and polling. Have one globally claimed logical execution slot, 30-second
database-time leases, five-second renewals and increasing attempts/fences. Every mutation
must recheck run/owner/attempt/fence/state and current database lease validity.

Supervise cancellation/deadline/renewal/resource stopping independently of CPU work. Allow
three infrastructure attempts; do not automatically retry deterministic failures.
Checkpoint completed intervals and verified prefixes every 100 executed intervals/1,000
events or before a 256 KiB buffer overflows. Fsync before fenced reference commits.
Restore only trusted-digest, identity, semantic and prefix-verified state; ignore uncommitted
suffixes. Seal terminal state separately, renew through validation and publish an atomic
fenced pointer only after final cancellation/pressure checks.

Use server-derived private paths with no-follow/symlink protections. Bound snapshot and
checkpoint to 32 MiB, artifacts to 256 MiB and physical per-run storage across attempts
to 512 MiB. Managed-root defaults are 5 GiB quota, 2 GiB free-space reserve and 100,000
entries. Count spools/orphans in admission. Offer locked reference-aware idle maintenance,
dry-run by default, removing at most 500 recognized unreferenced files older than one
hour. Protect whole published scopes and committed references; retain history under finite
quotas rather than silently expiring it.

Use authenticated, filter/identity-bound opaque cursors with 24-hour expiry, bounded
pages and 1 MiB general response ceiling. Authorize before shared reads. Resolve only
allowlisted artifact names, verify length/hash before sending success headers, and stream
bounded file/ZIP chunks with cancellation cleanup. Do not expose storage keys or ORM rows.

## Real Docker traffic and resource safety

Expose explicit `docker` mode through REST/MCP and a separate browser workspace. Use
wall-clock HTTP arrivals, private capped CPU-work backends, four strategies, bounded FIFO
and finite tasks. Limits: 10,000 requests, four replicas, sixteen inflight calls, 120 seconds.
Separate admission memory from actual memory; never retry uncertain execution. Account for
every outcome; bound stop/drain and retain reservations when transport cannot settle.

Implement optional real-second physical scaling with sustained pending/idle observations,
cooldown/min/max and idle-owned-dynamic removal. Only a restricted proxy may access the
daemon. Give controller and read-only guard separate private credentials. Authorize fixed
semantic operations against enrolled immutable image/network/container definitions;
reject arbitrary daemon routes, commands, mounts and caller-selected deployment JSON.
Persist a single-use create intent before sending; inspect and persist immutable IDs
before start. Treat lost receipts as uncertain and reconcile exact identity; absence by
name alone cannot release an uncertain create. Clean interrupted physical work instead
of replaying it. Successful publication requires resolved owned resources.

Write separate operational events/lifecycle/summary artifacts. Independently verify hashes,
scope, identities, ordering, terminal counts, reservation peaks and cleanup. Expose sampled
RSS/cgroup/CPU/throttle windows and fence/lease/snapshot-bound live frames expiring after
three seconds; missing/reset observations remain unavailable.

Default resource consumers must fail closed on stale/missing unsafe evidence. Sample only
fixed project identities; publish fenced latest evidence to PostgreSQL and recheck it
transactionally during increasing work and final publication. Hold admission at 70% of
service caps, stop at 85% or observed OOM and recover after three distinct samples below
60%. Evidence expires after three seconds. Require at least 8 GiB engine memory,
`max(2 GiB, 25% engine RAM)` reserve and remaining potential project demand. Apply the
same Docker/Linux measurements on Desktop and Linux Engine; do not require a host-side
Windows memory feeder. This revises the earlier independent-host-evidence approach after
review of Docker-only startup. Do not equate VM capacity with Windows physical headroom,
modify Desktop settings, prune unrelated resources or claim unlimited no-OOM safety.

## Interface, access and maintainability

Build English server/dataset/run/history/result/demo/Docker workflows with keyboard focus,
labels, reduced motion, narrow reflow and bounded lists. Playback reads saved data; running
another strategy is explicit. Keep credentials in memory, native downloads streamed/verified
and a restrained white/serif/blue interface populated from actual API values.

Serve local and authenticated API paths with strict Host/Origin, URL-credential and
request/response bounds. Produce bounded redacted error codes/correlation metadata;
never leak SQL, tokens, uploaded values or raw dependency exceptions. Preserve cancellation
and cleanup semantics across error boundaries. Bundle ReDoc locally at `/api/docs`, guard
its schema as configured, avoid external asset/telemetry calls and credential persistence,
and scope its CSP independently of the dashboard. Generate OpenAPI/MCP inventories under
`docs/contracts/`; keep endpoint details there instead of duplicating them in prose.

Use Python keyword calls and keyword-only new APIs, retaining narrow interoperability
exceptions; use named TypeScript options. Require strict types, descriptive names, useful
Google-style docstrings and named unit-aware budgets. Production Python limits are 120
function/400 class code lines, excluding comments/docstrings and historical migrations.
Extract by responsibility; never suppress diagnostics or add count-only wrappers.

## Developer interface, verification and delivery

Expose Docker-first lifecycle, format/lint/types, focused/offline/integration/browser,
schema, migration, bundle, maintenance and hot-reload commands through Just with Make
delegates. Support `docker compose up --build -d --wait`; expose the full ordered gate
through `scripts/verify.ps1`. Host IDE support stays optional. Exclude assets/locks from
formatting and test tools/caches from runtime images; use locked isolated check images.

Verify Docker/container boundaries before application acceptance. Exercise exact tick
boundaries, overload, four strategies, resource invariants, malformed input, cross-process
determinism, scaling, replay tampering, stale writes, idempotency, lease loss, checkpoints,
process-crash recovery and verified downloads. Run real PostgreSQL/Redis integration;
test Redis-outage fallback. Exercise actual HTTP/MCP, physical scaling and scoped
controller/backend/proxy faults, cancellation and cleanup. Run browser acceptance last.
Do not mutate supplied validator/test data, relax assertions or suppress warnings to pass.

Record observed checks with platform/workload/revision scope; distinguish test layers and
avoid invented coverage, all-browser/WCAG, bare-metal, soak or cold-build claims. A separate
server-owned stress installation may admit 50,000 logical requests; clients cannot raise
production limits. Keep unperformed CI/hooks, remote HTTPS/security and extended recovery
acceptance explicit. Respect gate deferrals; never infer an interrupted run passed.

Deliver a product-oriented root README, short project AGENTS, and a compact `docs/`
containing index, DEVELOPMENT, DESIGN, PLAN, QUESTIONNAIRE, AI-USAGE, MCP and TREE.
Keep generated contracts under `docs/contracts/` and preserved assignment/test assets
under `docs/assignment/`. Consolidate obsolete duplicate documentation rather than adding
an archive or source-folder README maze. Write a tracked, one-time AI-USAGE narrative
with real examples, human decisions and observed verification; do not publish private
raw chats or require a new entry for every future change. Prepare matching generated
examples and local review artifacts with clear validator applicability. Inspect intended
diffs, preserve protected bytes and keep commits/source local; external delivery remains
the user's action.
