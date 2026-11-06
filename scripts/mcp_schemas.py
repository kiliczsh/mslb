"""Generate/check the complete canonical MCP contract inventory, including missing files."""

import argparse
import hashlib
import tempfile
from pathlib import Path

from mslb_api.mcp_catalog import (
    COMMON,
    CONTRACT_VERSION,
    PROMPTS,
    RESOURCES,
    TOOLS,
    output_schema,
    schema,
)
from mslb_contracts.canonical import canonical_bytes


def artifacts() -> dict[str, bytes]:
    files: dict[str, bytes] = {}
    tools, resources, prompts = [], [], []
    for tool in TOOLS:
        source, result = (
            f"tools/{tool.name}.input.schema.json",
            f"tools/{tool.name}.output.schema.json",
        )
        files[source], files[result] = (
            canonical_bytes(schema(tool.input)),
            canonical_bytes(output_schema(tool)),
        )
        tools.append({"name": tool.name, "input_schema": source, "output_schema": result})
    for name, uri, model in RESOURCES:
        path = f"resources/{name}.schema.json"
        files[path] = canonical_bytes(schema(model))
        resources.append({"name": name, "uri": uri, "schema": path})
    for name, _, model in PROMPTS:
        path = f"prompts/{name}.arguments.schema.json"
        files[path] = canonical_bytes(schema(model))
        prompts.append({"name": name, "arguments_schema": path})
    common = {}
    for name, model in COMMON.items():
        path = f"common/{name}.schema.json"
        files[path] = canonical_bytes(schema(model))
        common[name] = path
    files["manifest.json"] = canonical_bytes(
        {
            "contract_version": CONTRACT_VERSION,
            "tools": tools,
            "resources": resources,
            "prompts": prompts,
            "common": common,
            "files": {
                path: hashlib.sha256(data).hexdigest() for path, data in sorted(files.items())
            },
        }
    )
    return files


def generate(root: Path) -> None:
    for name, data in artifacts().items():
        path = root / name
        if path.is_symlink():
            raise ValueError("Schema destination must not be a symlink")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)


def check(root: Path) -> None:
    with tempfile.TemporaryDirectory(prefix="mslb-mcp-schemas-") as temporary:
        generated = Path(temporary)
        generate(generated)
        expected = {
            path.relative_to(generated).as_posix(): path.read_bytes()
            for path in generated.rglob("*")
            if path.is_file()
        }
        actual = {}
        if root.is_symlink():
            raise ValueError("Schema inventory must not be a symlink")
        for index, path in enumerate(root.rglob("*")):
            if index > 256 or path.is_symlink():
                raise ValueError("Unsafe or oversized schema inventory")
            if path.is_file():
                if path.stat().st_size > 1024**2:
                    raise ValueError("Schema file exceeds its byte limit")
                actual[path.relative_to(root).as_posix()] = path.read_bytes()
        if actual != expected:
            raise ValueError("MCP schema drift: changed, missing, or unexpected files")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("generate", "check"))
    parser.add_argument("--directory", type=Path, default=Path("docs/contracts/mcp"))
    arguments = parser.parse_args()
    if arguments.command == "generate":
        generate(arguments.directory)
    else:
        check(arguments.directory)
    print("MCP contract schemas " + arguments.command + " passed")
