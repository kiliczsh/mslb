# Enough aggregate free memory, insufficient memory on each server

Each of two servers has 256 MB free. A single 384 MB request cannot combine these separate capacities.

4 requests; arrival ticks 0–1;
at most 2 arrivals in a single tick.
Workload: 4–32 units;
memory: 128–384 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| frag-a | 8 | 512 | 4 |
| frag-b | 8 | 512 | 4 |

## Expected behavior

- c-large cannot start at t=1.
- The smaller d-small request can start while c-large remains pending.
- A request's memory must not be split across servers.

Expected totals: 4 arrivals,
4 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
