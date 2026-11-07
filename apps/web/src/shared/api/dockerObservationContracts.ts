import { z } from "zod";

const counter = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
const time = z.iso.datetime({ offset: true });
const health = z.strictObject({
  backend_id: z.string().regex(/^[a-z][a-z0-9-]{0,127}$/),
  ready: z.boolean(),
  logical_admission_mem_mb: counter.min(1).max(1_000_000_000),
  max_inflight: counter.min(1).max(4),
  inflight: counter.max(4),
  reserved_mem_mb: counter.max(1_000_000_000),
  operations_per_unit: z.literal(2048),
  max_operations: z.literal(2_000_000),
  work_timeout_ms: z.literal(2000),
  rss_bytes: counter.nullable(),
  cgroup_memory_bytes: counter.nullable(),
  cpu_usage_usec: counter.nullable(),
  throttled_usec: counter.nullable(),
});
const sample = z.strictObject({
  resource_id: z.uuid(),
  container_id: z.string().regex(/^[0-9a-f]{64}$/),
  observed_at: time,
  health,
});
export const backendMeasurementsSchema = z.strictObject({
  first: sample,
  latest: sample,
  samples: counter.min(1).max(1000),
  sampled_peak_rss_bytes: counter.nullable(),
  sampled_peak_cgroup_memory_bytes: counter.nullable(),
  cpu_counter_reset: z.boolean(),
  throttling_counter_reset: z.boolean(),
  cpu_delta_usec: counter.nullable(),
  throttled_delta_usec: counter.nullable(),
});
export const dockerProgressSchema = z.strictObject({
  schema_version: z.literal(1),
  mode: z.literal("docker"),
  run_id: z.uuid(),
  fence: counter.min(1),
  snapshot_digest: z.string().regex(/^[0-9a-f]{64}$/),
  observed_at: time,
  elapsed_seconds: z.number().finite().nonnegative(),
  pending: counter.max(256),
  inflight: counter.max(16),
  backends: z.array(sample).max(4),
  unavailable_backends: z.array(z.string().min(1).max(128)).max(4),
});
