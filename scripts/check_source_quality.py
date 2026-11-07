"""Enforce readable production APIs and bounded Python implementation units."""

import ast
from pathlib import Path

SOURCE_ROOTS = (
    Path("apps/api/src"),
    Path("apps/worker/src"),
    Path("apps/container-controller/src"),
    Path("apps/mock-server/src"),
    Path("packages"),
)
# Count nonblank code lines, excluding docstrings and nested definitions. Nested
# units receive their own check; historical migration bodies remain immutable.
MAX_FUNCTION_CODE_LINES = 120
MAX_CLASS_CODE_LINES = 400
APPLICATION_IMPORTS = ("mslb_api", "mslb_worker", "mslb_controller", "mslb_mock_server")
type Definition = ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef


def code_lines(*, node: Definition, lines: list[str]) -> int:
    """Measure implementation size without penalizing explanatory documentation."""
    excluded: set[int] = set()
    for child in ast.walk(node=node):
        if (
            isinstance(child, ast.Expr)
            and isinstance(child.value, ast.Constant)
            and isinstance(child.value.value, str)
        ):
            excluded.update(range(child.lineno, (child.end_lineno or child.lineno) + 1))
    if not isinstance(node, ast.ClassDef):
        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                excluded.update(range(child.lineno, (child.end_lineno or child.lineno) + 1))
    return sum(
        bool(lines[index - 1].strip())
        and not lines[index - 1].lstrip().startswith("#")
        and index not in excluded
        for index in range(node.lineno, (node.end_lineno or node.lineno) + 1)
    )


def inspect_source(*, path: Path, source: str) -> list[str]:
    """Report undocumented public APIs, oversized units and reversed app dependencies."""
    tree = ast.parse(source=source, filename=str(object=path))
    lines = source.splitlines()
    errors: list[str] = []
    if ast.get_docstring(node=tree) is None and tree.body:
        errors.append(f"{path}:1: document the module's responsibility")
    for parent in ast.walk(node=tree):
        if isinstance(parent, (ast.Module, ast.ClassDef)):
            for node in parent.body:
                if (
                    isinstance(node, ast.ClassDef)
                    or (
                        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                        and not node.name.startswith("_")
                    )
                ) and ast.get_docstring(node=node) is None:
                    errors.append(f"{path}:{node.lineno}: document {node.name}")
        if isinstance(parent, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            size = code_lines(node=parent, lines=lines)
            maximum = (
                MAX_CLASS_CODE_LINES
                if isinstance(parent, ast.ClassDef)
                else MAX_FUNCTION_CODE_LINES
            )
            if size > maximum:
                errors.append(
                    f"{path}:{parent.lineno}: {parent.name} has {size} code lines; limit {maximum}"
                )
        if "packages" in path.parts:
            names = (
                [parent.module or ""]
                if isinstance(parent, ast.ImportFrom)
                else [item.name for item in parent.names]
                if isinstance(parent, ast.Import)
                else []
            )
            if isinstance(parent, (ast.Import, ast.ImportFrom)) and any(
                name.split(".")[0] in APPLICATION_IMPORTS for name in names
            ):
                errors.append(f"{path}:{parent.lineno}: shared packages cannot import app adapters")
    return errors


def main() -> None:
    """Check production sources only; never rewrite assignment assets or migrations."""
    paths = sorted(
        path
        for root in SOURCE_ROOTS
        for path in root.rglob(pattern="*.py")
        if "versions" not in path.parts
    )
    errors = [
        error
        for path in paths
        for error in inspect_source(path=path, source=path.read_text(encoding="utf-8"))
    ]
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"Source documentation, size and package boundaries passed for {len(paths)} modules")


if __name__ == "__main__":
    main()
