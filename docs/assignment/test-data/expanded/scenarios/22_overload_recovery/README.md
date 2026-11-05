# Overload, idle recovery, and a return to normal traffic

A short intense wave creates a queue; new traffic arrives after 120 idle ticks.

1,700 requests; arrival ticks 0–177;
at most 200 arrivals in a single tick.
Workload: 4–16 units;
memory: 64–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| fleet-00 | 64 | 4096 | 8 |
| fleet-01 | 64 | 4096 | 8 |
| fleet-02 | 64 | 4096 | 8 |
| fleet-03 | 64 | 4096 | 8 |

## Expected behavior

- There must be 1,700 requests in total.
- The queue must drain during the idle interval.
- New traffic at t=158 must not inherit active or pending requests from the previous wave.

Expected totals: 1700 arrivals,
1700 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Traffic phases

| Phase | Start | End (exclusive) | Requests/tick |
|---|---:|---:|---:|
| warm-up | 0 | 30 | 2 |
| overload | 30 | 38 | 200 |
| idle recovery | 38 | 158 | 0 |
| normal traffic | 158 | 178 | 2 |

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
