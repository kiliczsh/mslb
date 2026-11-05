# Working agreements

- Read README.md and docs/DEVELOPMENT.md before changing code; use docs/DESIGN.md for behavior and docs/assignment/requirements.md for the original task.
- Keep code, comments, documentation and commit messages in English. Prefer descriptive names and small, cohesive units.
- Use Python keyword arguments wherever supported and keyword-only new application APIs; use named options objects for TypeScript APIs with multiple options. Follow the coding standards in docs/DEVELOPMENT.md.
- Preserve docs/assignment/requirements.md, docs/assignment/requirements.pdf, supplied core assets and generated fixture bytes (including scenario metadata) and scripts/validate_run.py. Keep supplied assets outside formatters.
- Use the locked Docker workflows in justfile/Makefile. The ignored .venv is optional IDE support; do not add editor-specific interpreter settings.
- Run checks appropriate to the change and report their actual scope. Normal implementation acceptance uses scripts/verify.ps1 with no warnings/errors; fix causes rather than suppressing diagnostics.
- Current delivery-preparation instruction: do not run the full test suite or Docker gate until the user requests it. Use focused checks where useful; never infer a new pass from an older receipt.
- Keep current behavior and contracts consistent with the central docs. docs/AI-USAGE.md is a tracked, curated project narrative, not a continuously updated activity log.
- Sequential and concurrent simulation are both supported. Keep physical Docker traffic separate from deterministic logical traces. Observability is deferred; Grafana is excluded.
- Work locally on the current branch (main). Make small commits after reviewing intended changes and staged diffs. Never stage secrets, private logs, dependencies or incidental runtime exports. The curated data/test-artifacts delivery snapshot is explicitly tracked.
- Never push, publish, deploy or upload repository changes. Dependency/image downloads for local work are allowed.
- Scope Docker operations to this project's resources; never change Docker Desktop settings or prune unrelated resources.
