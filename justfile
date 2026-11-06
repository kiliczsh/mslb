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
