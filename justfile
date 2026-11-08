set windows-shell := ["powershell.exe", "-NoProfile", "-Command"]
npx := if os() == "windows" { "npx.cmd" } else { "npx" }
ty_platform := if os() == "windows" { "win32" } else { "linux" }
pyright_platform := if os() == "windows" { "Windows" } else { "Linux" }
pyright_command := if path_exists("/opt/mslb-pyright/node_modules/.bin/pyright") == "true" { "/opt/mslb-pyright/node_modules/.bin/pyright" } else { npx + " --yes --package=pyright@1.1.414 pyright" }

default:
    @just --list

# Optional IDE navigation only; application acceptance always runs in Docker.
ide:
    uv sync --locked --all-packages --group dev --python 3.13
    uv pip check

# Rebuild source-copied tooling so checks cannot silently inspect an old checkout.
build-python:
    docker compose --profile tools build python-checks

build-frontend:
    docker compose --profile tools build frontend-checks

lint: build-python build-frontend
    docker compose --profile tools run --rm --no-deps python-checks ruff check apps packages scripts tests
    docker compose --profile tools run --rm --no-deps frontend-checks pnpm exec biome ci --error-on-warnings . /documentation

format-check: build-python build-frontend
    docker compose --profile tools run --rm --no-deps python-checks ruff format --check apps packages scripts tests
    docker compose --profile tools run --rm --no-deps frontend-checks pnpm exec biome format . /documentation

# Only owned source/config directories are writable; immutable supplied assets stay outside /work.
format: build-python build-frontend
    docker run --rm --network none --read-only --tmpfs /tmp --workdir /work --mount 'type=bind,source={{justfile_directory()}}/apps,target=/work/apps' --mount 'type=bind,source={{justfile_directory()}}/packages,target=/work/packages' --mount 'type=bind,source={{justfile_directory()}}/scripts,target=/work/scripts' --mount 'type=bind,source={{justfile_directory()}}/tests,target=/work/tests' --mount 'type=bind,source={{justfile_directory()}}/pyproject.toml,target=/work/pyproject.toml,readonly' mslb-python-tools:local ruff format apps packages scripts tests
    docker run --rm --network none --read-only --tmpfs /tmp --mount 'type=bind,source={{justfile_directory()}}/apps/web,target=/work' --mount 'type=bind,source={{justfile_directory()}}/apps/api/src/mslb_api/static,target=/documentation' mslb-frontend-tools:local pnpm exec biome format --write --config-path /app/biome.json /work/src /work/browser /work/tests /work/worker /work/index.html /work/package.json /work/biome.json /work/tsconfig.json /work/vite.config.ts /work/playwright.config.ts /documentation

ty: build-python
    docker compose --profile tools run --rm --no-deps python-checks ty check --python-platform linux

pyright: build-python
    docker compose --profile tools run --rm --no-deps python-checks /opt/mslb-pyright/node_modules/.bin/pyright --project pyrightconfig.json --pythonplatform Linux --warnings

check: test-python test-frontend

# Optional host checks: first prepare .venv using the locked workspace.
lint-local:
    uv run --no-sync ruff check apps packages scripts tests

format-check-local:
    uv run --no-sync ruff format --check apps packages scripts tests

format-python-local:
    uv run --no-sync ruff format apps packages scripts tests

ty-local platform=ty_platform:
    uv run --no-sync ty check --python-platform {{ quote(platform) }}

# Runs locally, targeting the actual host; Linux runtime checks run in Docker.
pyright-local platform=pyright_platform:
    {{ pyright_command }} --project pyrightconfig.json --pythonplatform {{ quote(platform) }} --warnings

check-python-local: lint-local format-check-local ty-local pyright-local
    python scripts/check_source_quality.py

check-windows: lint-local format-check-local
    just ty-local win32
    just pyright-local Windows

check-linux: lint-local format-check-local
    just ty-local linux
    just pyright-local Linux

# Optional runner alias. Direct Docker-only commands remain in the documentation.
verify:
    pwsh -NoProfile -File scripts/verify.ps1

# Generation writes only repository-owned inventories, never assignment fixtures.
generate-mcp-schemas: build-python
    docker compose --profile tools run --rm --no-deps --volume '{{justfile_directory()}}/docs/contracts/mcp:/app/docs/contracts/mcp' python-checks python scripts/mcp_schemas.py generate

check-mcp-schemas: build-python
    docker compose --profile tools run --rm --no-deps python-checks python scripts/mcp_schemas.py check

generate-openapi-schemas: build-python
    docker compose --profile tools run --rm --no-deps --volume '{{justfile_directory()}}/docs/contracts/openapi:/app/docs/contracts/openapi' python-checks python scripts/openapi_schemas.py generate

check-openapi-schemas: build-python
    docker compose --profile tools run --rm --no-deps python-checks python scripts/openapi_schemas.py check

# Docker tasks use built images; build explicitly after changing source.
build:
    docker compose --profile tools build postgres
    docker compose --profile tools build python-checks
    docker compose --profile tools build api
    docker compose --profile tools build frontend-checks
    docker compose --profile tools build web
    docker compose --profile tools build browser-checks

up:
    docker compose up --build -d --wait

down:
    docker compose down

test-python: build-python
    docker compose --profile tools run --rm --no-deps python-checks

test-frontend: build-frontend
    docker compose --profile tools run --rm --no-deps frontend-checks

test-persistence: build-python
    docker compose --profile tools run --rm --no-deps persistence-checks

check-runtime:
    docker compose --profile tools run --rm --no-deps runtime-check

check-api:
    docker compose --profile tools run --rm --no-deps api-check

check-mcp:
    docker compose --profile tools run --rm --no-deps mcp-check

check-worker:
    docker compose --profile tools run --rm --no-deps worker-check

check-traffic:
    docker compose --profile tools run --rm --no-deps traffic-check

check-docker-proxy:
    docker compose --profile tools run --rm --no-deps docker-proxy-check

check-docker-public:
    docker compose --profile tools run --rm --no-deps docker-public-check

check-browser:
    pwsh -NoProfile -File scripts/check-browser.ps1

check-scenarios:
    pwsh -NoProfile -File scripts/check-scenarios.ps1

# Project interruptions and read-only maintenance planning; explicit apply stays an operator command.
check-controller-faults:
    pwsh -NoProfile -File scripts/check-controller-faults.ps1

plan-output-maintenance:
    docker compose --profile tools run --rm --no-deps output-maintenance

# Start/stop the source-mounted Vite service against the existing backend.
dev:
    docker compose --profile dev up --no-build --no-deps -d --wait web-dev

dev-stop:
    docker compose --profile dev rm --stop --force web-dev

# Common operational and focused development tasks.
config:
    docker compose --profile tools config --quiet

status:
    docker compose ps --all

logs service="api":
    docker compose logs --tail 100 {{quote(service)}}

migrate:
    docker compose run --rm --no-deps migrate

test-unit filter="": build-python
    # Windows PowerShell drops empty native arguments; omit -k for the unfiltered suite.
    docker compose --profile tools run --rm --no-deps python-checks pytest tests/unit {{if filter == "" { "" } else { "-k " + quote(filter) }}}

bundle:
    docker compose --profile tools run --rm --no-deps bundle-runner

verify-bundle:
    docker compose --profile tools run --rm --no-deps bundle-runner verify /data/exports/scaled-example

check-source-quality: build-python
    docker compose --profile tools run --rm --no-deps python-checks python scripts/check_source_quality.py

check-deployment: build-python
    docker compose --profile tools config --format json | docker run --rm -i --network none --read-only --tmpfs /tmp --memory 128m --cpus 1 --pids-limit 32 mslb-python-tools:local python scripts/check_deployment.py
