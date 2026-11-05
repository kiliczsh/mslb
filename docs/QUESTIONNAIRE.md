# Planning questionnaire

I used an initial planning phase to establish scope and constraints before application
implementation, then reviewed further questions and alternatives as development progressed.
This questionnaire consolidates the adopted answers from that sequence. Assignment
requirements, my directions and engineering proposals have different origins; later
refinements are identified below. The consolidated record does not imply that every
answer was known before implementation.

[PLAN.md](PLAN.md) presents the updated plan as a self-contained implementation brief.
[DESIGN.md](DESIGN.md) explains its implemented consequences, and [AI-USAGE.md](AI-USAGE.md)
describes my direction, reviewed decisions and verification steps.

## Scope and delivery

| Question | Adopted answer and reason | Origin |
| --- | --- | --- |
| What are we building? | A deterministic load-balancer simulator for heterogeneous servers, with a server-management dashboard, request execution and downloadable traces. | Assignment |
| Which requirements are authoritative? | Preserve the supplied [assignment](assignment/requirements.md); additional product choices do not redefine it. | Assignment and user |
| Which language is used for the product and repository? | English for UI, code, comments, documentation and commit messages. Planning conversations may be in Turkish. | User |
| What is the product called? | mslb; use consistent mslb distribution, import and Docker identities. Application source lives at the repository root. | User; layout refined later |
| Which optional features are included? | Playback/visualization, four interchangeable strategies, per-server/cluster metrics and simulated autoscaling. All four bonuses were selected. | User |
| What was added beyond the assignment? | MCP, durable asynchronous jobs/recovery, Redis notifications, actual Docker HTTP traffic, physical scaling and bounded resource safety. | Agreed additions |
| Is observability included? | Operational telemetry infrastructure is deferred; Grafana is excluded. Health, safe errors, measured resource evidence and admission enforcement remain active. | Later user scope decision |
| Is public deployment part of this work? | No. Prepare a locally reviewable project with authentication and deployment controls; do not publish, deploy or push repository contents. | User |
| Does feature completion prove unlimited acceptance? | No. State tested workloads, platforms and failure cases explicitly. Keep broader browser, security, stress and recovery acceptance distinct. | Engineering and user |
| Where does documentation live? | A small central docs set, generated contracts under docs/contracts, original assets under docs/assignment and no archive directory. | Final user refinement |
| Is AI usage documented? | Yes. Track the curated AI-USAGE.md as a one-time project account; it is not an append-only development log. Keep private raw conversations out of the published document. | Assignment and final user refinement |

## Execution model and determinism

| Question | Adopted answer and reason | Origin |
| --- | --- | --- |
| What does a tick represent? | One simulated second. Events at boundary t describe the preceding interval's completion and the next interval's starting state. | Assignment |
| How are both supplied validation and concurrent CPU sharing supported? | Offer sequential and concurrent modes. Sequential is the user-selected default and supports the unchanged supplied evaluator; explicit concurrent mode implements equal CPU sharing and independent replay. Both are delivered capabilities. | Engineering, then user-confirmed resolution |
| What does sequential mode do? | At most one active request per server; duration is the integer ceiling of work divided by capacity. This provides the supplied evaluator's expected semantics. | Compatibility decision |
| What does concurrent mode do? | Divide a server's CPU equally among its frozen active set for an interval, cap consumption at remaining work and expose completion at the next boundary. Do not redistribute unused shares within that interval. | Assignment and engineering precision |
| Is real Docker traffic deterministic? | No. It is a separate operational mode with measured wall-clock behavior and separate artifacts; never label physical timing as deterministic simulation output. | Engineering |
| Why exact arithmetic? | Use Fraction for logical work and policy comparisons to avoid floating-point drift. Lossless numerator/denominator decimal strings cross JSON; the browser uses BigInt where needed. | Engineering |
| How are ties resolved? | Normalize and order identities, define stable strategy ties and scheduler ordering, and serialize the current tick's events canonically. Runtime IDs and timestamps stay out of canonical trace bytes. | Assignment and engineering |
| Which strategies are available? | First Fit, Round Robin, Least Connections and Most Available Memory, all sharing the same eligibility and resource checks. | Selected bonus |
| What happens if a request cannot start immediately? | Queue it in stable order, allow feasible work to bypass a blocked head, and apply documented impossible-memory, timeout and residual-overflow drops. | Assignment permits queue/drop; engineering policy |
| What does the rate limit count? | REQUEST_STARTED events per server per tick. It is independent of CPU consumption and active-request count. | Assignment |
| How long is memory reserved? | From start until completion. Logical mem_mb is accounting metadata, not a host allocation of that size. | Assignment and resource-safety decision |
| Are empty datasets accepted? | No. Reject them consistently; valid nonempty all-dropped runs still have defined terminal accounting and zero/null metrics. | Compatibility and engineering |
| How are repeated executions identified? | Freeze inputs/settings and retain content identities; successful equivalent logical runs regenerate identical artifacts. Operational runs still receive separate durable IDs. | Assignment and engineering |
| What does replay validate? | Independently reconstruct lifecycle, CPU timing, reservations, start rates, canonical events, drops and dynamic membership. Do not use the runtime scheduler/work updater as its own oracle. | Engineering |
| When is the supplied evaluator applicable? | Static sequential runs with autoscaling disabled. Other logical runs use independent replay and explicitly report original-evaluator applicability. | Engineering |
| When does a simulation stop? | After terminal accounting and pending membership changes, without creating new idle scaling cycles. Limits, cancellation or invalid publication produce controlled failure rather than fabricated success. | Engineering refinement |

## Data, storage and recovery

| Question | Adopted answer and reason | Origin |
| --- | --- | --- |
| Can supplied files be edited to make tests pass? | No. Preserve assignment assets, core fixtures and supplied validator byte-for-byte; keep them outside automatic formatting. | User |
| Can interview data differ from the examples? | Yes. Parse and validate the original field schema, reject malformed or oversized input explicitly, and test synthetic workloads as well as supplied examples. | Assignment |
| Do server edits change running or historical simulations? | No. CRUD changes apply to future runs; submissions freeze server versions, inputs and effective settings. | Assignment and engineering |
| What prevents stale dashboard edits? | Versioned/ETag mutations, explicit stale-update errors and deliberate reload rather than silently overwriting a newer cluster. | Engineering |
| Which store is authoritative? | PostgreSQL owns catalog state, jobs, leases, fences and committed artifact pointers. Redis provides bounded notification hints; its absence does not invalidate durable results. | Engineering |
| Why a separate worker? | CPU-heavy simulation must not block the API event loop; the worker can supervise leases, cancellation, recovery and validated publication. | Engineering |
| What makes submission retry safe? | Idempotency records bind retries to their original immutable submission; changing the live cluster later cannot change that run. | Engineering |
| What prevents a stale attempt from publishing? | Database-time leases, fencing tokens, ownership checks and a separately committed terminal seal. Publication requires validated identities and artifacts. | Engineering |
| Where can logical recovery resume? | At a validated completed-interval checkpoint and committed prefix. A partially executed interval is not silently treated as committed. | Engineering |
| Is physical traffic replayed after a crash? | Do not replay side effects or claim success from partial physical execution. Reconcile ownership and clean up exactly the resources the run owns. | Engineering |
| Is history deleted automatically? | Published history is retained within finite quotas. Bounded orphan maintenance protects SQL references and whole published scopes; dry-run is the default. | Engineering |
| How are downloads trusted? | Publish only complete validated bundles; expose fixed artifact selectors and verify file sizes/digests. Use bounded streaming rather than assembling entire large downloads in browser memory. | Engineering |

## Platform, safety and access

| Question | Adopted answer and reason | Origin |
| --- | --- | --- |
| What is required on a developer machine? | Docker and Docker Compose for runtime/build/checks. Just offers convenient tasks; Make delegates matching tasks. No mandatory host Python/Node setup. | Assignment and user |
| Which Python stack is used? | Python 3.13, uv workspace/lock, FastAPI, Pydantic, SQLAlchemy/asyncpg and Alembic, with Ruff, ty and Pyright checks. | Agreed foundation and implementation |
| Which frontend stack is used? | React, TypeScript and Vite, pnpm, Biome, TanStack Query, Zustand, Zod and Recharts. Strict typing and bounded views support maintainability. | Agreed foundation |
| Is the IDE environment supported? | Yes: optional ignored .venv from the locked workspace. It supports navigation and does not replace Docker verification. No VS Code interpreter configuration is needed. | Later user refinement |
| Which environments were targeted? | Windows Docker Desktop with Linux containers and Linux Docker Engine. Recorded distinct Ubuntu Engine checks ran on WSL2, not bare-metal Linux. | User and observed verification |
| What is the default access model? | Local loopback demo without mandatory bearer entry; authentication-enabled operation is implemented and checked separately. Docker control always has a separate internal credential. | User and engineering |
| Where may credentials travel? | Authorization headers, never query parameters or canonical artifacts. Streaming fetch supports authenticated SSE. Documentation loading must not retain the schema token. | Engineering |
| Should API docs need the public internet at runtime? | No. Serve locally bundled pinned ReDoc at /api/docs; keep schema access subject to the existing authentication rules. | Later user choice |
| Can an application hold the unrestricted Docker socket? | Ordinary app services cannot. A restricted control path checks authorized actions, ownership, labels, immutable intents and resource bounds. | Engineering |
| What does memory safety promise? | Bounded accepted workloads and fail-closed admission under missing/stale/unsafe evidence, not a guarantee against arbitrary host exhaustion. Never allocate simulated request memory literally. | User goal and engineering interpretation |
| How is growth limited? | Shared hard limits cover inputs, concurrency, queues, arithmetic, artifacts, responses, storage and Docker replicas. Recalculate deployment budgets rather than allowing unbounded overrides. | Engineering |
| Are Windows host and Docker VM memory interchangeable? | No. The earlier independent Windows feeder was removed after reviewing automatic feeder management versus Docker-only monitoring. Current admission uses Docker/Linux headroom, container limits and OOM evidence; it does not independently measure Windows physical RAM. Do not alter Desktop settings or unrelated workloads. | Engineering approach revised by user direction on November 8, 2026; local commit c0e0206 |
| How much default parallel work is allowed? | One active logical job, one active physical run and at most four physical backends under the current bounded profile. Higher throughput needs an explicitly reviewed budget. | Engineering |
| Are resource failures allowed to change deterministic output? | No. Operational pressure may reject or stop an attempt; a successful logical run must retain its frozen scheduling semantics. | Engineering |

## Maintainability and verification

| Question | Adopted answer and reason | Origin |
| --- | --- | --- |
| How are adapters kept consistent? | Shared application services and contracts serve REST, MCP and workers; shared packages do not import application adapters. | Engineering |
| Which calling convention is preferred? | Named Python arguments wherever supported, keyword-only new application APIs and named TypeScript options objects for multiple options. Preserve narrow runtime/protocol exceptions. | User |
| How should names and comments read? | Descriptive behavior names, English docstrings, conventional Python/TypeScript casing and constants/comments that explain units and policy. Avoid invented tracking codes and comments restating syntax. | User and coding standards |
| Are oversized classes and methods limited? | Production Python checks require public documentation and limit functions to 120 code lines and classes to 400; excluded historical migrations remain immutable. Extraction follows responsibility rather than artificial wrappers. | Later maintenance decision |
| How are tests organized? | Python offline tests by engine/contracts/application/platform, service-backed integration tests separately, frontend component tests near source, Node tests and Playwright browser workflows in dedicated frontend folders. | Later maintenance decision |
| What counts as evidence? | Actual command outcomes and exact scopes. Unit tests do not establish full E2E coverage; Chromium does not establish all-browser or formal WCAG acceptance. No unmeasured coverage percentage is claimed. | Engineering and user |
| What if a check emits warnings? | Fix the cause; do not suppress a diagnostic or weaken assertions to declare a pass. Preserve failed attempts separately from later passing runs. | User |
| Must the full gate run for every fast edit? | The user explicitly paused full tests/gates for the current rapid delivery-preparation edits. Document-only checks must not be presented as a fresh application gate. | Current user instruction |
| How is work shared externally? | It is not pushed or deployed during this session. Review intended diffs and keep commits local on the user-selected main branch. | User; branch choice refined later |
