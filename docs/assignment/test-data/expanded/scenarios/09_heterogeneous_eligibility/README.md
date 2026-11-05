# Servers with different CPU and memory capacities

A fast server with little memory works alongside a slower server with more memory.

36 requests; arrival ticks 0–5;
at most 6 arrivals in a single tick.
Workload: 4–36 units;
memory: 64–4096 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| compute | 24 | 256 | 6 |
| general | 8 | 512 | 3 |
| memory | 4 | 4096 | 2 |

## Expected behavior

- Requests larger than 512 MB can only be assigned to the memory server.
- A request of exactly 4096 MB must be accepted.
- Compare both completion time and queue time when evaluating strategies.

Expected totals: 36 arrivals,
36 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
