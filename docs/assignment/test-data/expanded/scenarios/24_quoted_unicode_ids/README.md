# CSV quoting and Unicode identifiers

Commas, double quotes, Unicode symbols, and leading zeros are part of request identifiers.

6 requests; arrival ticks 0–10;
at most 1 arrivals in a single tick.
Workload: 16–16 units;
memory: 64–64 MB.

## Servers

| ID | CPU units/tick | Memory MB | Starts/tick |
|---|---:|---:|---:|
| encoding-a | 16 | 512 | 8 |

## Expected behavior

- Read CSV with a standard parser rather than splitting lines on commas.
- Preserve identifiers without numeric conversion or character loss.

Expected totals: 6 arrivals,
6 completions, and 0 drops.
These counts apply only to the servers and config.json in this folder.
Exact tick expectations in expected.json were derived manually.

## Usage

Apply the servers in servers.json and the settings in config.json before
uploading requests.csv. Uploading a CSV does not change servers or configuration.
Files in this folder are not automatically loaded into the application.
See the package README.md for detailed usage and verification commands.
