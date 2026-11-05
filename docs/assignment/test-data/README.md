# Assignment test data

This directory contains the original supplied fixtures and the synthetic scenario
suite. The [assignment](../requirements.md) defines the task;
[DESIGN.md](../../DESIGN.md) documents implemented behavior and
[DEVELOPMENT.md](../../DEVELOPMENT.md) describes Docker-based verification.

| Folder | Purpose |
| --- | --- |
| core/ | Original servers.json, requests.csv, example run.jsonl and validate_run.py, preserved byte-for-byte. |
| expanded/ | 24 synthetic scenarios, 32 invalid inputs, generator/verifier and hash manifest. |

A supplied example trace is input material, not evidence that a new application run
passed. Generated scenario files, including per-scenario README metadata, retain their
manifest hashes. Only these top-level navigation guides change with documentation layout.

Fixture settings map to the API as follows: execution_mode becomes mode,
scheduling_strategy becomes strategy, queue_timeout_ticks of zero becomes null,
and autoscale_enabled=false becomes autoscaling.enabled=false. The API ignores no
implicit server changes: explicitly configure the scenario cluster before submitting.
Logical execution has no wall-clock pacing setting.

The default dataset ceiling is 10,000 requests. The 12k/50k scenarios require the
separately bounded isolated stress profile; ordinary production uploads must reject
them. Fixture structure checks alone are not application acceptance.

Use the project Docker commands in DEVELOPMENT rather than installing host dependencies.
Container images copy this directory to /fixtures, so its container paths remain
/fixtures/core and /fixtures/expanded after the documentation move.
