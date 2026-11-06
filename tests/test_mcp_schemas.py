import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from scripts.mcp_schemas import artifacts, check, generate


def test_generated_inventory_is_stable_valid_complete_and_detects_missing_files(
    tmp_path: Path,
) -> None:
    files = artifacts()
    assert files == artifacts()
    manifest = json.loads(files["manifest.json"])
    assert (
        len(manifest["tools"]) == 12
        and len(manifest["resources"]) == 4
        and len(manifest["prompts"]) == 3
    )
    for name, payload in files.items():
        if name != "manifest.json":
            Draft202012Validator.check_schema(json.loads(payload))
    generate(tmp_path)
    check(tmp_path)
    missing = tmp_path / "tools/list_servers.input.schema.json"
    missing.unlink()
    with pytest.raises(ValueError, match="missing"):
        check(tmp_path)
    generate(tmp_path)
    missing.write_bytes(b"{}")
    with pytest.raises(ValueError, match="changed"):
        check(tmp_path)
    generate(tmp_path)
    (tmp_path / "unexpected.schema.json").write_text("{}")
    with pytest.raises(ValueError, match="unexpected"):
        check(tmp_path)
