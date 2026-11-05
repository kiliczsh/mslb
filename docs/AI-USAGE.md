# AI usage

AI assistance was required by the [assignment](assignment/requirements.md). I prepared
an initial implementation plan before application development, then developed mslb
iteratively with Codex. I set priorities and constraints, directed the work, evaluated
trade-offs, accepted or changed decisions, and required verification. Codex helped refine
proposals, implemented code, ran local tools and investigated failures under that direction.

This is a curated delivery account, initially prepared to clarify the planning and review
sequence. It is not an exhaustive transcript or a file that must be updated after every
future change.
It was derived from the actual local assistance record, Git history and recorded
command outcomes. Private raw conversations, credentials, machine identifiers and
large runtime logs are not included. The recorded tool is Codex; the historical log
does not reliably identify an exact model version, so none is asserted.

## From initial plan to verified iterations

The development sequence was **initial planning -> staged implementation -> review and
verification -> revised decisions -> delivery documentation**.

1. **Prepare the initial plan.** I started from the assignment, defined the intended
   scope and constraints, and required the Docker foundation to work before application
   features were implemented. This established a starting plan, not a prediction of
   every detail that would emerge during implementation.
2. **Direct implementation.** I used focused instructions to move through the engine,
   validation, persistence, API and dashboard work, and to refine scope and usability.
   Codex proposed and executed implementation steps within those constraints.
3. **Evaluate choices.** I reviewed concrete trade-offs as they arose. For example,
   I accepted separate sequential and concurrent modes to address the supplied
   evaluator and the assignment's different execution expectations.
4. **Reject or revise decisions.** I excluded Grafana and deferred observability.
   Later, I reconsidered the Windows host-memory feeder: instead of automating its
   lifecycle, I chose to remove that dependency while retaining Docker memory checks.
5. **Require verification.** I required checks at the affected boundaries and reviewed
   reported outcomes. Codex executed the commands; the resulting traces, receipts and
   failure diagnostics informed the next iteration. A proposal or passing unit test
   alone was not treated as proof that the complete application worked.

The current [PLAN](PLAN.md) consolidates the plan after these iterations. It is not a
verbatim copy of the initial planning conversation. [QUESTIONNAIRE](QUESTIONNAIRE.md)
separates requirements, preferences and later refinements; [DESIGN](DESIGN.md) describes
the implemented behavior.

## How the collaboration worked

| Stage | My direction | AI contribution | How the result was checked |
| --- | --- | --- | --- |
| Initial planning | Prepare an initial plan from the assignment, establish priorities and require a Docker-first implementation sequence. | Help refine requirements and constraints into proposed implementation steps. | Review the starting plan against the assignment before application implementation; refine detailed decisions during later stages. |
| Platform foundation | Make Docker work before building application features; require local commits and no push. | Create locked Python/frontend workspaces, pinned images, Compose services, bounded tool runners and developer commands. | Build actual images, inspect effective Compose configuration and connect to real PostgreSQL/Redis containers. |
| Logical simulation | Keep deterministic behavior, resource constraints and all selected strategies. | Implement exact work sharing, stable scheduling, canonical events, queue/drop policies, metrics and simulated autoscaling. | Hand-calculated cases, boundary/invariant tests, shuffled-input/fresh-process determinism checks and repeated artifact comparisons. |
| Validation and recovery | Preserve the supplied validator/data; support sequential compatibility and concurrent task semantics. | Implement both execution modes, a separate replay implementation, immutable snapshots, checkpoints, leases/fences and atomic publication. | Run the unchanged evaluator where applicable; independently replay logical traces; inject invalid output and actual worker interruptions. |
| API and dashboard | Provide server CRUD and runnable, inspectable simulations; refine the real UI through feedback. | Connect shared services to REST/MCP, build bounded React workflows, playback, comparison, streamed downloads and guided demos. | Real HTTP/MCP clients, PostgreSQL integration checks, component tests and actual Chromium interactions/download hash checks. |
| Physical Docker additions | Include real HTTP traffic and scaling while protecting host resources and unrelated projects. | Implement restricted control, owned creation intents/reconciliation, bounded routing, admission evidence and cleanup. | Actual routing/scaling, controller/backend/control-service faults, cancellation, resource inspection and cleanup checks. |
| Maintenance | Add ReDoc, prepare optional IDE support, improve names/docstrings/constants and split large responsibilities. | Bundle offline API docs, refactor focused modules/components, organize tests and improve Just/Compose workflows. | Containerized static checks, unchanged-test-body comparisons, protected-file hashes and the observed complete maintenance gate. |
| Delivery preparation | Consolidate documentation, keep everything English and local, pause full tests and make this document tracked. | Prepare central documentation with separate behavior, implementation prompt, decision questionnaire, generated contracts and concise agent rules. | Documentation/reference/integrity review only; no fresh application gate is implied by this preparation. |

I retained control of scope and acceptance while Codex generally chose the next
dependency-safe implementation step within the authorized iteration. My later directions
also shaped product behavior and priorities, including
the default sequential mode, optional concurrent mode, naming, dashboard interactions,
scope deferrals, local-only operations and the final documentation layout.

## Representative prompts

These are excerpts of actual instructions, not fabricated examples. English instructions
are reproduced as recorded; Turkish instructions are translated to English and labeled.
They summarize the steering that mattered rather than reproducing private conversation.

| Recorded instruction | Resulting constraint or work |
| --- | --- |
| "first setup and make sure everything is working on docker and then start implementing" | Verify the container foundation before application features. |
| "before commiting make sure we have no warnings or errors and project can be build?" | Introduce and expand the Docker verification gate; fix warning causes rather than suppressing diagnostics. |
| "never push something, everything stays local" | Keep commits and runtime work local; no source publication or deployment. |
| "meanwhile I want to skip Grafana" | Exclude Grafana; the later instruction to skip observability deferred the wider telemetry implementation. |
| English translation: "First fix the OpenAPI schema difference and the failing UI/browser tests, then run the full Docker gate. Do not touch the supplied validator or test data; keep changes local and do not push." | Repair actual contract/test drift, retain the original inputs/assertions and run a fresh complete gate. |
| English translation of the maintenance request: "Prepare .venv locally, use ReDoc, fill missing explanations, clean up magic values, review long methods/classes and naming, make Docker and Just sufficient for developers, organize tests and Compose, and leave browser testing until the end." | Carry out the maintenance increment with explicit developer/readability goals and browser acceptance last. |
| English translation: "The code supplied is sequential and the task asks for concurrent; we implemented both." | Treat the two supported modes as the accepted solution and document evaluator applicability precisely. |
| English translation: "Do not run full tests anymore; I will request quick changes for a while." | Stop further full-gate execution; preserve previous results with their actual scope. |
| English translation: "Start with three subagents, organize the agreed structure, track AI-USAGE.md and write it once; do not run full tests, keep everything local, we are preparing delivery, and everything will be in English." | Delegate distinct documentation ownership and integrate the central delivery set without claiming a new runtime test pass. |

[QUESTIONNAIRE.md](QUESTIONNAIRE.md) records the adopted answers and their origins.
[PLAN.md](PLAN.md) presents the updated implementation plan and consolidated build brief.
The project began with planning and then evolved through the iterations described above;
the current document combines initial direction with later accepted refinements.

## A concrete reviewed decision: Windows host-memory evidence

I initially asked for the Windows feeder to start and stop through `just up/down`, then
paused that work before implementation. On November 8, I asked whether the feeder could
be removed altogether. We compared two approaches: retain independent Windows RAM
measurement and automate its lifecycle, or use Docker-only measurements and remove the
host-side dependency. I chose the latter because it simplified Docker-only startup while
preserving the resource checks needed for the project's bounded Docker execution.

The resulting local commit, `c0e0206`, removes the feeder and writer service while keeping
Docker capacity/headroom checks, container memory limits, OOM checks and the existing
pressure thresholds. This deliberately gives up independent Windows physical-RAM
measurement; Docker/VM memory is not presented as the same measurement.

Verification included 54 focused platform unit tests, 27 PostgreSQL integration tests,
static/type/schema checks, a real HTTP simulation, and `just up/down/up` with the saved
result digest preserved. One readiness response returned 503 after an extra diagnostic
probe; the following 12 samples returned 200. No full suite or gate was rerun, and
sampling/TTL changes remained deferred. The [verification record](DEVELOPMENT.md#verification-record)
keeps that scope separate from earlier gate results.

This example records my direction, the alternatives actually discussed, a changed
decision and its verification. It does not attribute every implementation detail or
proposal to me, or imply that this later decision was part of the initial plan.

## What verification added beyond generated code

Generated implementation was not treated as evidence of correctness. Verification used
multiple boundaries:

- Exact engine examples and an independently written replay checked request lifecycle,
  CPU completion, memory reservations, start rates, drops and membership changes.
- The supplied validator remained unchanged. Static sequential output passed it;
  concurrent output was checked by independent replay. Its manifest does not claim
  that the supplied sequential evaluator validated concurrent sharing.
- Cross-process determinism and repeated/recovered bundles compared canonical bytes
  and digests rather than visual similarity.
- Real PostgreSQL/Redis integration exercised leases, stale owners, idempotency,
  snapshots, cancellation, publication and Redis-outage fallback.
- REST/MCP checks used real transports; Chromium exercised actual dashboard workflows,
  authenticated access, native downloads, narrow layouts and cleanup.
- Fault checks interrupted owned worker/controller/backend/control-service resources;
  protected-file checks verified that supplied assets were unchanged.
- Static analysis, source-policy checks, image builds and effective deployment
  inspection covered maintainability and container configuration.

The latest completed maintenance gate passed all 32 phases in 431.926 seconds on
Windows Docker Desktop with Linux containers. It included 487 offline plus 157
PostgreSQL/Redis integration Python tests, four Node tests, 59 Vitest tests and eight
Chromium workflows, along with six builds, schema checks and actual service/fault flows.
This result predates the final Windows Just optional-filter correction. That correction
was separately verified by the actual unfiltered command passing all 487 offline tests
in 24.82 seconds; a subsequent full rerun was stopped at the user's instruction and is
not recorded as passed.

Earlier runtime revision e91d7dd also passed a distinct Ubuntu Docker Engine gate on
WSL2: 32 phases in 457.09 seconds. Its 464 offline/150 integration Python, four Node,
59 Vitest and seven Chromium results belong to that earlier revision, not the later
maintenance snapshot. These are Linux-container/WSL2 observations, not bare-metal Linux
or an empty image-cache claim. An earlier isolated workload campaign completed 27
durable executions, including bounded 12k/50k stress profiles and repeat/hash checks;
the normal production dataset ceiling remained 10k.

## Failures that changed the implementation

AI assistance also produced mistakes and incomplete assumptions. The following recorded
cases show why actual execution and review mattered:

| Observed failure | Correction and evidence |
| --- | --- |
| The initial standalone uv image could not discover/download a Python runtime. | Generate locks using the Python tooling base instead; then build/check the locked workspace in containers. |
| A browser selector no longer matched the actual scheduling control. | Inspect the real accessible DOM, use the combobox's current name and retain an explicit selected-value assertion. The failed gate stayed a failed attempt. |
| Offline ReDoc emitted blocked embedded-font and external-branding requests. | Allow the required embedded font only in the docs policy and bundle digest-pinned local branding. The focused authenticated browser check then passed without external requests or CSP/console errors. |
| A formatter returned zero while emitting an I/O diagnostic during broad bind-mount traversal. | Restrict formatting to the owned source inventory and rerun without diagnostics; do not infer success from exit status alone. |
| Windows PowerShell dropped an empty native argument for unfiltered just test-unit. | Omit -k entirely unless a filter is supplied; the actual default recipe then passed 487 cases. |
| Some generated docstrings described behavior too broadly. | Review actual bodies, units, return values and side effects; correct 51 descriptions rather than relying on syntax checks alone. |
| Windows-mounted Linux build context yielded an empty configuration file. | Reject a temporary dependency-lock workaround, preserve committed pins and build the exact tracked archive from native Linux storage. |

The maintenance corrections retained behavioral assertions and preserved the supplied
examples and evaluator. Failed attempts remained separate from later successful results.
Corrections were checked at the affected boundary before subsequent broader verification
where authorized.

## Boundaries and human responsibility

I prepared the starting plan, set scope, and accepted or revised the sequential/concurrent
solution, UI changes, resource-safety approach and documentation. Codex's proposals remained
subject to that direction. Codex executed local code/tooling changes and reported observed
outcomes; I retained responsibility for reviewing the result and deciding what to deliver.
An implementation commit demonstrates a recorded code change, not authorship of every
underlying idea or proof that the final design existed before development began.

Recorded results cover finite workloads and named platforms. They do not establish
formal WCAG conformance, all-browser behavior, multi-hour soak acceptance, remote HTTPS
operation, arbitrary host memory safety or unlimited history retention. No instrumented
line/branch coverage percentage is claimed. Observability remains deferred and Grafana
excluded.

This document deliberately separates implemented behavior, observed checks and broader
acceptance. During the current rapid delivery-preparation edits, the user has explicitly
paused full tests/gates. Documentation changes therefore do not create a new application
verification result. Repository content remains local; preparing tracked documentation
does not imply that any source was pushed, published or externally submitted.
