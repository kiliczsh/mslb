# High volume: 50,000 requests

Intensive, variable CPU and memory demand across 16 servers over 250 ticks.

50,000 requests; arrival ticks 0–249;
at most 200 arrivals in a single tick.
Workload: 1–32 units;
memory: 16–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| stress-00 | 128 | 8192 | 16 |
| stress-01 | 128 | 8192 | 16 |
| stress-02 | 128 | 8192 | 16 |
| stress-03 | 128 | 8192 | 16 |
| stress-04 | 128 | 8192 | 16 |
| stress-05 | 128 | 8192 | 16 |
| stress-06 | 128 | 8192 | 16 |
| stress-07 | 128 | 8192 | 16 |
| stress-08 | 128 | 8192 | 16 |
| stress-09 | 128 | 8192 | 16 |
| stress-10 | 128 | 8192 | 16 |
| stress-11 | 128 | 8192 | 16 |
| stress-12 | 128 | 8192 | 16 |
| stress-13 | 128 | 8192 | 16 |
| stress-14 | 128 | 8192 | 16 |
| stress-15 | 128 | 8192 | 16 |

## Expected behavior

- Expect 50,000 unique requests and 150,000 lifecycle events.
- Use real_time_ticks=false when measuring performance.
- Wall-clock performance depends on the machine; no fixed runtime is claimed.

Expected totals: 50000 arrivals,
50000 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Traffic phases

| Phase | Start | End (exclusive) | Requests/tick |
|---|---:|---:|---:|
| high volume | 0 | 250 | 200 |

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
