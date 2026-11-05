# Repository map

Application code lives at the repository root. This map shows ownership rather than
listing every source file; use the source tree to inspect individual modules.

```text
mslb/
├── README.md                    Product overview and Docker quick start
├── AGENTS.md                    Short contributor-agent working agreements
├── apps/
│   ├── api/                     REST, MCP and local ReDoc adapters
│   ├── worker/                  Durable logical-job execution
│   ├── container-controller/    Guard, restricted control and real HTTP traffic
│   ├── mock-server/             Bounded CPU-work backend
│   └── web/                     React dashboard and frontend/browser tests
├── packages/
│   ├── application/             Use cases, jobs, files and resource policies
│   ├── contracts/               Validated transport and domain contracts
│   ├── engine/                  Deterministic simulation and independent replay
│   └── persistence/             PostgreSQL repositories and Alembic migrations
├── infra/
│   ├── postgres/                PostgreSQL image
│   ├── tooling/                 Python, frontend, browser, Pyright and ReDoc builds
│   └── web/                     Production web image and Nginx configuration
├── scripts/                     Schema, artifact, operational and acceptance tools
├── tests/
│   ├── unit/                    Engine, contracts, application and platform checks
│   └── integration/             Real database and service-boundary tests
├── data/
│   ├── examples/                 Small example inputs/configuration
│   └── test-artifacts/           Curated scenario bundles and observed receipts
├── docs/
│   ├── README.md                Reading order
│   ├── DEVELOPMENT.md           Developer commands, standards and troubleshooting
│   ├── DESIGN.md                Implemented architecture and execution rules
│   ├── PLAN.md                  Self-contained implementation prompt
│   ├── QUESTIONNAIRE.md         Decisions, answers and rationale
│   ├── AI-USAGE.md              Curated account of actual AI collaboration
│   ├── MCP.md                   MCP connection and usage
│   ├── TREE.md                  This map
│   ├── contracts/
│   │   ├── openapi/             Generated local/authenticated OpenAPI inventories
│   │   └── mcp/                 Generated MCP schemas and manifest
│   └── assignment/
│       ├── requirements.md      Original assignment
│       ├── requirements.pdf     Original PDF
│       └── test-data/
│           ├── core/            Supplied inputs, trace and validator
│           └── expanded/        Scenarios, invalid fixtures and dataset metadata
├── docker-compose.yml           Main deployment and check services
├── compose.*.yaml               Included services and isolated test overlays
├── justfile                     Docker-first tasks
├── Makefile                     Matching task aliases
├── pyproject.toml               Python workspace and tool configuration
├── pyrightconfig.json           Additional Python type checks
└── uv.lock                      Locked Python dependencies
```

Frontend unit/component tests are colocated in apps/web/src; Node tests live in
apps/web/tests and Playwright workflows in apps/web/browser. The browser download
worker has its own TypeScript configuration under apps/web/worker. Frontend package,
lock and tool configuration files live under apps/web.

Dataset-local README files explain fixture provenance and expected scenarios and
travel unchanged with the fixtures. Other contributor documentation is centralized
here and in the root README; it is not repeated in source directories.

Ignored .venv, .cache, dependencies, credentials, generated run.jsonl and exported
bundles are local/runtime files. Persistent service data uses Docker volumes.
They are intentionally absent from the tracked source map.

See [the documentation index](README.md) and [developer workflows](DEVELOPMENT.md).
