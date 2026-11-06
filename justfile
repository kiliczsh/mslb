set windows-shell := ["powershell.exe", "-NoProfile", "-Command"]
npx := if os() == "windows" { "npx.cmd" } else { "npx" }

default:
    @just --list

# Optional host checks: first prepare .venv using the locked workspace.
lint:
    uv run --no-sync ruff check apps packages scripts tests

format-check:
    uv run --no-sync ruff format --check apps packages scripts tests

format:
    uv run --no-sync ruff format apps packages scripts tests

ty platform="linux":
    uv run --no-sync ty check --python-platform {{ quote(platform) }}

# Runs locally, targeting the Linux container platform by default.
pyright platform="Linux":
    {{ npx }} --yes --package=pyright@1.1.414 pyright --project pyrightconfig.json --pythonplatform {{ quote(platform) }}

check: lint format-check ty pyright

# These schema recipes run inside the locked Python tooling container.
generate-mcp-schemas:
    python scripts/mcp_schemas.py generate

check-mcp-schemas:
    python scripts/mcp_schemas.py check
