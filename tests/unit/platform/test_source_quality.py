"""Readability policy rejects regressions in public documentation and dependency direction."""

from pathlib import Path

from scripts.check_source_quality import inspect_source


def test_public_api_documentation_and_reverse_adapter_imports_are_rejected() -> None:
    errors = inspect_source(
        path=Path("packages/example/src/example.py"),
        source='"""Example module."""\nimport mslb_api.app\n\ndef operation() -> None:\n    pass\n',
    )
    assert any("document operation" in error for error in errors)
    assert any("cannot import app adapters" in error for error in errors)


def test_nested_units_and_documentation_are_measured_independently() -> None:
    source = (
        '"""Example module."""\n\ndef operation() -> None:\n'
        '    """Perform one documented operation."""\n'
        + "\n".join(f"    value_{index} = {index}" for index in range(121))
        + "\n"
    )
    errors = inspect_source(path=Path("example.py"), source=source)
    assert len(errors) == 1 and "limit 120" in errors[0]
