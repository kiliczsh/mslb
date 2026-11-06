# Run inside the locked Python tooling container; no host Python is required.
generate-mcp-schemas:
    python scripts/mcp_schemas.py generate

check-mcp-schemas:
    python scripts/mcp_schemas.py check
