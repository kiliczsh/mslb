set windows-shell := ["powershell.exe", "-NoProfile", "-Command"]
npx := if os() == "windows" { "npx.cmd" } else { "npx" }
ty_platform := if os() == "windows" { "win32" } else { "linux" }
pyright_platform := if os() == "windows" { "Windows" } else { "Linux" }
pyright_command := if path_exists("/opt/mslb-pyright/node_modules/.bin/pyright") == "true" { "/opt/mslb-pyright/node_modules/.bin/pyright" } else { npx + " --yes --package=pyright@1.1.414 pyright" }

default:
    @just --list

# Optional host checks: first prepare .venv using the locked workspace.
lint:
    uv run --no-sync ruff check apps packages scripts tests

format-check:
    uv run --no-sync ruff format --check apps packages scripts tests

format:
    uv run --no-sync ruff format apps packages scripts tests

ty platform=ty_platform:
    uv run --no-sync ty check --python-platform {{ quote(platform) }}

# Runs locally, targeting the actual host; Linux runtime checks run in Docker.
pyright platform=pyright_platform:
    {{ pyright_command }} --project pyrightconfig.json --pythonplatform {{ quote(platform) }} --warnings

check: lint format-check ty pyright

check-windows: lint format-check
    just ty win32
    just pyright Windows

check-linux: lint format-check
    just ty linux
    just pyright Linux

# Optional runner alias. Direct Docker-only commands remain in the documentation.
verify:
    pwsh -NoProfile -File scripts/verify.ps1

# These schema recipes run inside the locked Python tooling container.
generate-mcp-schemas:
    python scripts/mcp_schemas.py generate

check-mcp-schemas:
    python scripts/mcp_schemas.py check

generate-openapi-schemas:
    python scripts/openapi_schemas.py generate

check-openapi-schemas:
    python scripts/openapi_schemas.py check

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

test-python:
    docker compose --profile tools run --rm --no-deps python-checks

test-frontend:
    docker compose --profile tools run --rm --no-deps frontend-checks

test-persistence:
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
