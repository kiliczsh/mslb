# Reusing fully occupied memory within the same tick

Two 128 MB reservations fill the 256 MB capacity exactly; completions release memory before new assignments.

4 requests; arrival ticks 0–2;
at most 2 arrivals in a single tick.
Workload: 8–8 units;
memory: 1–256 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| memory-a | 8 | 256 | 4 |

## Expected behavior

- The first two requests finish at t=2.
- c-full can start at t=2; d-small waits until t=3.

Expected totals: 4 arrivals,
4 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
