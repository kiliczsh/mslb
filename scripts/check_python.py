"""Run foundation checks and immutable fixture validation in a container."""

import subprocess


def main() -> None:
    """Stop on the first failed check."""
    commands = [
        ["ruff", "check", "."],
        ["ruff", "format", "--check", "."],
        ["ty", "check"],
        ["just", "check-mcp-schemas"],
        ["pytest"],
        [
            "python",
            "/fixtures/core/validate_run.py",
            "--servers",
            "/fixtures/core/servers.json",
            "--requests",
            "/fixtures/core/requests.csv",
            "--run",
            "/fixtures/core/run.jsonl",
        ],
        ["python", "/fixtures/expanded/verify.py"],
    ]
    for command in commands:
        print("Running:", " ".join(command), flush=True)
        subprocess.run(command, check=True)  # noqa: S603 -- fixed commands, no user input


if __name__ == "__main__":
    main()
