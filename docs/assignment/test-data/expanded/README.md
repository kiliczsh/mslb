# Expanded load balancer test data

This package contains 24 scenarios with 67,209 request rows and 32 invalid input
examples, created from scratch using the existing data format and simulation
rules. All generated files are contained in this folder. The supplied
files are kept unchanged in [core](../core). The original assignment is
[requirements.md](../../requirements.md). See the
[test-data guide](../README.md) for the shared layout and verification commands.
All data is synthetic and contains no real personal or patient information.

## Application usage and evidence boundary

The logical application is implemented. Existing engine golden tests exercise 22 supported
workloads; fixture structure verification is separate. The isolated HTTP/worker campaign
now passes all 24 scenarios, the sequential variant and all 32 invalid examples through
their applicable parser/HTTP surfaces. Both 50k executions have identical bundle and trace
hashes. The default 10,000-request ceiling remains enforced; 12k/50k execution uses a
separate bounded stress profile. See [measured evidence](../../../DEVELOPMENT.md#verification-record),
[current coverage](../../../DEVELOPMENT.md#verification-record), [simulation](../../../DESIGN.md)
and [resource bounds](../../../DESIGN.md). These synthetic fixtures do not
add assignment requirements or independently prove application correctness.

1. Start with `05_equal_cpu_sharing` for a small scenario that can be checked manually.
2. Configure only the scenario's servers in a separate test environment.
3. Map its `config.json` to the actual run-create DTO using the table below.
4. Upload `requests.csv` through `POST /api/v1/datasets`, then use the returned
   dataset ID in `POST /api/v1/simulations`.
5. Compare the generated trace with `expected.json` and independently replay it.

| Fixture setting | Application setting |
| --- | --- |
| `scheduling_strategy` | `strategy` |
| `execution_mode: concurrent` | `mode: concurrent` |
| `execution_mode: sequential` | `mode: sequential` |
| `queue_timeout_ticks: 0` | `queue_timeout_ticks: null` |
| Positive `queue_timeout_ticks` | Preserve the positive value |
| `autoscale_enabled: false` | `autoscaling: {enabled: false}` |
| `real_time_ticks: false` | Omit; logical execution does not use wall-clock pacing |

`servers.json` uses `{tick_seconds: 1, servers: [...]}`. In the API, create
servers individually through `POST /api/v1/servers`; run configuration belongs in
the simulation request. See [API contracts](../../../DESIGN.md).
Uploading a dataset does not change managed servers or run configuration. These
fixture scripts do not contact an application or delete existing servers.

## File structure

```text
docs/assignment/test-data/expanded/
  README.md
  manifest.json                     # Catalog, statistics, seed, and generated file hashes
  verification-report.json           # Results of the checks performed on this package
  generate.py                       # Regenerates fixtures using the same seed
  verify.py                         # Input checks and optional offline engine checks
  scenarios/
    01_idle_gaps/
      requests.csv
      servers.json
      config.json
      expected.json
      README.md
    ...
    16_execution_modes/
      config.sequential.json        # A second execution mode for the same CSV
      expected.sequential.json
    ...
  invalid/
    csv/                            # 24 intentionally invalid CSV files
    servers/                        # 8 intentionally invalid server JSON files
    expected_rejections.json
```

## Scenarios

| No | Scenario | Requests | Behavior covered |
|---|---|---:|---|
| 01 | idle_gaps | 8 | Late initial arrivals and idle intervals |
| 02 | exact_memory_release | 4 | Full memory occupancy and reuse within the same tick |
| 03 | fragmented_memory | 4 | Aggregate free memory versus free memory on an individual server |
| 04 | rate_window_reset | 12 | Per-tick admission limit and reset on the next tick |
| 05 | equal_cpu_sharing | 3 | CPU share changes as the active request count decreases |
| 06 | midflight_arrival | 2 | New arrivals changing an active request's CPU share |
| 07 | lexical_ties | 12 | Lexicographic order, letter case, and server tie-breaking |
| 08 | shuffled_input | 12 | Identical output for the shuffled records from scenario 07 |
| 09 | heterogeneous_eligibility | 36 | Eligible assignments across different CPU and memory capacities |
| 10 | memory_boundary | 7 | One below, exactly at, and one above capacity |
| 11 | all_unschedulable | 8 | Termination when no request can start |
| 12 | timeout_boundary | 5 | Queue timeout coinciding with resource release |
| 13 | pending_bypass | 4 | A smaller request progressing behind a large pending request |
| 14 | tick_rounding | 6 | Rounding fractional execution times to whole ticks |
| 15 | finish_arrive_collision | 6 | Event ordering for simultaneous completions, arrivals, and starts |
| 16 | execution_modes | 4 | Exact timing differences between concurrent and sequential modes |
| 17 | ramp_up_down | 1,200 | Five phases of increasing and decreasing load |
| 18 | periodic_spikes | 660 | Six periodic spikes on top of continuous traffic |
| 19 | long_tail_mix | 1,500 | Short jobs mixed with long CPU jobs and high-memory jobs |
| 20 | sustained_12k | 12,000 | Steady traffic over 1,200 ticks |
| 21 | stress_50k | 50,000 | 50,000 requests across 16 servers |
| 22 | overload_recovery | 1,700 | Overload followed by idle recovery and normal traffic |
| 23 | sparse_checkpoint_ticks | 10 | Tick 50/100 boundaries and an arrival at t=10,000 |
| 24 | quoted_unicode_ids | 6 | Commas, double quotes, Unicode symbols, and leading zeros |

Scenarios 07 and 08 deliberately contain the same logical records to verify that
row and server ordering do not affect the output. The other scenarios use different
workloads. Each folder includes its purpose, actual data ranges, server capacities,
and a checklist. Traffic phases are also recorded in `manifest.json`.

Scenarios 20 and 21 exceed the plan's default
[10,000-request dataset ceiling](../../../DESIGN.md).
Their completion expectations require a workload profile validated for 12,000
and 50,000 requests respectively. Under the default profile, expect input-limit
rejection instead.

All documentation, descriptions, and messages are in English. Scenario 24 keeps
language-neutral Unicode symbols in identifiers to exercise UTF-8 handling.

## Data contract and expectations

- The header is exactly `t,request_id,work_units,mem_mb`; encoding is UTF-8 without a BOM.
- `t >= 0`, `work_units > 0`, and `mem_mb > 0`; this suite uses integer values.
  Identifiers must be nonempty and unique after whitespace is stripped.
- Server CPU, memory, and admission limits are positive integers. One tick is one second.
- The primary configuration uses concurrent mode. Active requests share CPU equally,
  and memory remains reserved until completion. Scenario 16 also has a sequential variant.
- Each tick processes prior completions, arrivals, timeouts, assignments, and CPU
  consumption in that order. A request cannot finish on its start tick.
- Pending requests are ordered by `(arrival_tick, request_id)`; the final server
  tie-breaker is lexicographic `server_id`. A request that does not fit does not
  prevent later smaller requests from being considered.
- Autoscaling is disabled in all primary scenarios. Queue timeout is disabled
  except in scenario 12, where it is 3 ticks. A request that cannot fit on any
  server is dropped at arrival with `no_server_can_fit`.
- In scenario 12, `waiting_time >= timeout` is evaluated before assignment.

`expected.json` contains expected completion/drop counts and manually derived
start, finish, and drop ticks for small examples. These are expectations, not
outputs captured from the application. Large scenarios have no invented
performance or p95/p99 thresholds; measure those metrics from actual run output.
Changing the strategy, servers, execution mode, or timeout may change exact expectations.

## Invalid inputs

Files under `invalid` exercise input validation. Separate files cover missing or
duplicate columns, malformed rows, empty files, negative or zero values,
NaN/Infinity, fractional ticks, empty or duplicate identifiers, and invalid server
capacities. Each expected rejection reason is defined in `expected_rejections.json`.

These rejections represent **this suite's strict input contract**. The fixture
checker is not a live API test. Application acceptance must separately exercise
upload validation, numeric bounds, identifiers, and structured error responses.

The assignment does not prescribe one acceptance policy for zero work, zero memory,
extra columns, or fractional work units. This suite rejects them for consistency
with the original integer-based validator. If an application explicitly supports a
different policy, treat the relevant example as a contract difference rather than
automatically classifying it as a bug. Do not start simulations with invalid inputs,
even if the upload endpoint accepts them.

## Verification and regeneration

From the application directory (`mslb/`), using Python 3.10 or later without additional packages:

```powershell
python -B docs/assignment/test-data/expanded/verify.py
```

This checks valid CSV/JSON files, traffic phases, record counts, unique identifiers,
the manifest's generated-file SHA-256 hashes, and the expected rejection reason
for each of the 32 negative cases. It does not write any files.

These checks validate fixture structure and integrity only. They do not run the
application, Docker, database, Redis, HTTP endpoints, checkpoints, or crash recovery.
A stored fixture-only report is not evidence that application acceptance has passed.
Application acceptance must generate actual artifacts and run the independent
replay specified by the [implemented design](../../../DESIGN.md).
Use the unchanged core validator only for its documented static-server,
autoscaling-disabled `sequential` mode.

To regenerate the fixtures:

```powershell
python -B docs/assignment/test-data/expanded/generate.py
```

The seed is fixed at `20261105`. This command rewrites only generated scenario
files, negative inputs, and the manifest in this package. It overwrites manual
edits to generated scenario files; the top-level README, scripts, and verification
reports are preserved. Use `--output` to select a different target directory.
Do not select `docs/assignment/test-data/core/` as a target. After regeneration, refresh the
fixture-only report:

```powershell
python -B docs/assignment/test-data/expanded/verify.py --report docs/assignment/test-data/expanded/verification-report.json
```

## Requirement authority

Schema and core behavior come from [requirements.md](../../requirements.md). Exact arithmetic,
queue policies, execution modes, and deterministic strategy choices are documented
in the [execution rules](../../../DESIGN.md) and
[scheduling rules](../../../DESIGN.md). Synthetic workload
values and expectations are test design choices.
