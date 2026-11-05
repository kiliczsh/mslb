# Lexicographic tie-breaking

Numeric-looking identifiers and mixed letter cases distinguish natural numeric order from lexicographic order.

12 requests; arrival ticks 0–1;
at most 6 arrivals in a single tick.
Workload: 8–16 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| node-2 | 16 | 256 | 2 |
| node-10 | 16 | 256 | 2 |
| node-1 | 16 | 256 | 2 |

## Expected behavior

- Use the lexicographic order node-1, node-10, node-2.
- The event trace must match scenario 08.

Expected totals: 12 arrivals,
12 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
