set windows-shell := ["powershell.exe", "-NoProfile", "-Command"]
npx := if os() == "windows" { "npx.cmd" } else { "npx" }
ty_platform := if os() == "windows" { "win32" } else { "linux" }
pyright_platform := if os() == "windows" { "Windows" } else { "Linux" }
pyright_command := if path_exists("/opt/mslb-pyright/node_modules/.bin/pyright") == "true" { "/opt/mslb-pyright/node_modules/.bin/pyright" } else { npx + " --yes --package=pyright@1.1.414 pyright" }

# List all available recipes and their descriptions.
default:
    @just --list

# Prepare the optional locked Python environment for IDE navigation.
ide:
    uv sync --locked --all-packages --group dev --python 3.13
    uv pip check

# Rebuild Python tooling with the current source.
build-python:
    docker compose --profile tools build python-checks

# Rebuild frontend tooling with the current source.
build-frontend:
    docker compose --profile tools build frontend-checks

# Run Python and frontend lint checks in Docker.
lint: build-python build-frontend
    docker compose --profile tools run --rm --no-deps python-checks ruff check apps packages scripts tests
    docker compose --profile tools run --rm --no-deps frontend-checks pnpm exec biome ci --error-on-warnings . /documentation

# Check Python and frontend formatting in Docker.
format-check: build-python build-frontend
    docker compose --profile tools run --rm --no-deps python-checks ruff format --check apps packages scripts tests
    docker compose --profile tools run --rm --no-deps frontend-checks pnpm exec biome format . /documentation

# Format owned source/config files in Docker, preserving supplied assets.
format: build-python build-frontend
    docker run --rm --network none --read-only --tmpfs /tmp --workdir /work --mount 'type=bind,source={{justfile_directory()}}/apps,target=/work/apps' --mount 'type=bind,source={{justfile_directory()}}/packages,target=/work/packages' --mount 'type=bind,source={{justfile_directory()}}/scripts,target=/work/scripts' --mount 'type=bind,source={{justfile_directory()}}/tests,target=/work/tests' --mount 'type=bind,source={{justfile_directory()}}/pyproject.toml,target=/work/pyproject.toml,readonly' mslb-python-tools:local ruff format apps packages scripts tests
    docker run --rm --network none --read-only --tmpfs /tmp --mount 'type=bind,source={{justfile_directory()}}/apps/web,target=/work' --mount 'type=bind,source={{justfile_directory()}}/apps/api/src/mslb_api/static,target=/documentation' mslb-frontend-tools:local pnpm exec biome format --write --config-path /app/biome.json /work/src /work/browser /work/tests /work/worker /work/index.html /work/package.json /work/biome.json /work/tsconfig.json /work/vite.config.ts /work/playwright.config.ts /documentation

# Check Python types with ty in Docker.
ty: build-python
    docker compose --profile tools run --rm --no-deps python-checks ty check --python-platform linux

# Check Python types with Pyright in Docker.
pyright: build-python
    docker compose --profile tools run --rm --no-deps python-checks /opt/mslb-pyright/node_modules/.bin/pyright --project pyrightconfig.json --pythonplatform Linux --warnings

# Run the combined offline Python and frontend checks in Docker.
check: test-python test-frontend

# Lint Python locally using the optional locked environment.
lint-local:
    uv run --no-sync ruff check apps packages scripts tests

# Check Python formatting locally using the optional locked environment.
format-check-local:
    uv run --no-sync ruff format --check apps packages scripts tests

# Format Python locally using the optional locked environment.
format-python-local:
    uv run --no-sync ruff format apps packages scripts tests

# Check Python types locally with ty for the selected platform.
ty-local platform=ty_platform:
    uv run --no-sync ty check --python-platform {{ quote(platform) }}

# Check Python types locally with Pyright for the selected platform.
pyright-local platform=pyright_platform:
    {{ pyright_command }} --project pyrightconfig.json --pythonplatform {{ quote(platform) }} --warnings

# Run local Python lint, format, type and source-quality checks.
check-python-local: lint-local format-check-local ty-local pyright-local
    python scripts/check_source_quality.py

# Run local Python lint, format and type checks targeting Windows.
check-windows: lint-local format-check-local
    just ty-local win32
    just pyright-local Windows

# Run local Python lint, format and type checks targeting Linux.
check-linux: lint-local format-check-local
    just ty-local linux
    just pyright-local Linux

# Run the full Docker verification gate; requires PowerShell 7.
verify:
    pwsh -NoProfile -File scripts/verify.ps1

# Regenerate the repository-owned MCP schema inventory in Docker.
generate-mcp-schemas: build-python
    docker compose --profile tools run --rm --no-deps --volume '{{justfile_directory()}}/docs/contracts/mcp:/app/docs/contracts/mcp' python-checks python scripts/mcp_schemas.py generate

# Check the committed MCP schema inventory in Docker.
check-mcp-schemas: build-python
    docker compose --profile tools run --rm --no-deps python-checks python scripts/mcp_schemas.py check

# Regenerate the repository-owned OpenAPI schema inventory in Docker.
generate-openapi-schemas: build-python
    docker compose --profile tools run --rm --no-deps --volume '{{justfile_directory()}}/docs/contracts/openapi:/app/docs/contracts/openapi' python-checks python scripts/openapi_schemas.py generate

# Check the committed OpenAPI schema inventory in Docker.
check-openapi-schemas: build-python
    docker compose --profile tools run --rm --no-deps python-checks python scripts/openapi_schemas.py check

# Build runtime and verification images sequentially.
build:
    docker compose --profile tools build postgres
    docker compose --profile tools build python-checks
    docker compose --profile tools build api
    docker compose --profile tools build frontend-checks
    docker compose --profile tools build web
    docker compose --profile tools build browser-checks

# Build and start the application, waiting for healthy services.
up:
    docker compose up --build -d --wait

# Stop the application while retaining named volumes.
down:
    docker compose down

# Run the complete offline Python checks in Docker.
test-python: build-python
    docker compose --profile tools run --rm --no-deps python-checks

# Run frontend lint, types, Node and Vitest checks in Docker.
test-frontend: build-frontend
    docker compose --profile tools run --rm --no-deps frontend-checks

# Run integration checks against real PostgreSQL and Redis services.
test-persistence: build-python
    docker compose --profile tools run --rm --no-deps persistence-checks

# Check installed runtime boundaries using built Docker images.
check-runtime:
    docker compose --profile tools run --rm --no-deps runtime-check

# Check the running REST API through the Docker verification service.
check-api:
    docker compose --profile tools run --rm --no-deps api-check

# Check the running MCP interface through the Docker verification service.
check-mcp:
    docker compose --profile tools run --rm --no-deps mcp-check

# Check the running worker through the Docker verification service.
check-worker:
    docker compose --profile tools run --rm --no-deps worker-check

# Check physical HTTP traffic and scaling against Docker backends.
check-traffic:
    docker compose --profile tools run --rm --no-deps traffic-check

# Check the restricted Docker proxy contract and access controls.
check-docker-proxy:
    docker compose --profile tools run --rm --no-deps docker-proxy-check

# Check the public API workflow for physical Docker traffic.
check-docker-public:
    docker compose --profile tools run --rm --no-deps docker-public-check

# Run isolated Chromium acceptance checks; requires PowerShell 7.
check-browser:
    pwsh -NoProfile -File scripts/check-browser.ps1

# Run the separate standard/stress scenario campaign; requires PowerShell 7.
check-scenarios:
    pwsh -NoProfile -File scripts/check-scenarios.ps1

# Check project controller fault recovery; requires PowerShell 7.
check-controller-faults:
    pwsh -NoProfile -File scripts/check-controller-faults.ps1

# Produce a read-only maintenance plan for orphaned output files.
plan-output-maintenance:
    docker compose --profile tools run --rm --no-deps output-maintenance

# Start the source-mounted Vite service against the existing backend.
dev:
    docker compose --profile dev up --no-build --no-deps -d --wait web-dev

# Stop and remove the Vite development service.
dev-stop:
    docker compose --profile dev rm --stop --force web-dev

# Validate the effective Docker Compose configuration.
config:
    docker compose --profile tools config --quiet

# Show all application containers and their status.
status:
    docker compose ps --all

# Show the last 100 log lines for the selected service.
logs service="api":
    docker compose logs --tail 100 {{quote(service)}}

# Apply committed database migrations in Docker.
migrate:
    docker compose run --rm --no-deps migrate

# Run offline Python unit tests with an optional pytest keyword filter.
test-unit filter="": build-python
    # Windows PowerShell drops empty native arguments; omit -k for the unfiltered suite.
    docker compose --profile tools run --rm --no-deps python-checks pytest tests/unit {{if filter == "" { "" } else { "-k " + quote(filter) }}}

# Generate the canonical scaled simulation bundle in Docker.
bundle:
    docker compose --profile tools run --rm --no-deps bundle-runner

# Verify the canonical scaled simulation bundle in Docker.
verify-bundle:
    docker compose --profile tools run --rm --no-deps bundle-runner verify /data/exports/scaled-example

# Check source documentation, size limits and boundaries in Docker.
check-source-quality: build-python
    docker compose --profile tools run --rm --no-deps python-checks python scripts/check_source_quality.py

# Check effective Compose deployment policies and isolation in Docker.
check-deployment: build-python
    docker compose --profile tools config --format json | docker run --rm -i --network none --read-only --tmpfs /tmp --memory 128m --cpus 1 --pids-limit 32 mslb-python-tools:local python scripts/check_deployment.py
