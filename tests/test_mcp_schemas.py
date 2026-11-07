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
    manifest = json.loads(s=files["manifest.json"])
    assert manifest["contract_version"] == "2.3.0"
    assert {item["uri"] for item in manifest["resources"]} == {
        "mslb://servers",
        "mslb://strategies",
        "mslb://runs/{run_id}/summary",
        "mslb://runs/{run_id}/events",
        "mslb://docker-runs/{run_id}/summary",
    }
    assert (
        len(manifest["tools"]) == 18
        and len(manifest["resources"]) == 5
        and len(manifest["prompts"]) == 3
    )
    for name, payload in files.items():
        if name != "manifest.json":
            Draft202012Validator.check_schema(schema=json.loads(s=payload))
    generate(root=tmp_path)
    check(root=tmp_path)
    missing = tmp_path / "tools/list_servers.input.schema.json"
    missing.unlink()
    with pytest.raises(expected_exception=ValueError, match="missing"):
        check(root=tmp_path)
    generate(root=tmp_path)
    missing.write_bytes(data=b"{}")
    with pytest.raises(expected_exception=ValueError, match="changed"):
        check(root=tmp_path)
    generate(root=tmp_path)
    (tmp_path / "unexpected.schema.json").write_text(data="{}")
    with pytest.raises(expected_exception=ValueError, match="unexpected"):
        check(root=tmp_path)
