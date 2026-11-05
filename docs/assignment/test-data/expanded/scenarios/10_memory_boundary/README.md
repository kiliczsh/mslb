# One below, exactly at, and one above the memory limit

Requests of 1023 or 1024 MB fit; 1025 MB cannot fit on any available server.

7 requests; arrival ticks 0–18;
at most 1 arrivals in a single tick.
Workload: 16–16 units;
memory: 1–1025 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| bound-a | 16 | 1024 | 4 |

## Expected behavior

- Only bound-2 and bound-5 must be dropped with no_server_can_fit.
- A request matching the capacity exactly must not be rejected.

Expected totals: 7 arrivals,
5 completions, and 2 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
