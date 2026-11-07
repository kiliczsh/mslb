import { z } from "zod";
import { fractionSchema } from "../fractions.js";
import { readSettingsSchema, serverInputSchema } from "./contracts.js";

const tick = z.number().int().min(0).max(1_000_000);
const count = z.number().int().min(0).max(50_000);
const safe = z.number().int().min(-Number.MAX_SAFE_INTEGER).max(Number.MAX_SAFE_INTEGER);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
export const valueSchema = z.union([safe, fractionSchema]);
export type MetricValue = z.infer<typeof valueSchema>;
const distributionSchema = z.strictObject({
  count,
  mean: valueSchema.nullable(),
  min: tick.nullable(),
  max: tick.nullable(),
  p50: tick.nullable(),
  p95: tick.nullable(),
});
export const totalsSchema = z.strictObject({
  cpu_units_per_tick: safe,
  mem_mb: safe,
  started: count,
  finished: count,
  waiting: distributionSchema,
  response: distributionSchema,
  execution: distributionSchema,
  cpu_allocated: valueSchema,
  cpu_consumed: valueSchema,
  cpu_capacity_ticks: valueSchema,
  cpu_utilization: valueSchema,
  memory_reserved_mb_ticks: valueSchema,
  memory_capacity_mb_ticks: valueSchema,
  memory_utilization: valueSchema,
});
export const summarySchema = z.strictObject({
  arrived: count,
  started: count,
  finished: count,
  dropped: count,
  makespan: tick,
  terminal_tick: tick,
  completed_throughput: valueSchema,
  drop_fraction: valueSchema,
  queue_depth_peak: count,
  queue_depth_integral: safe,
  waiting: distributionSchema,
  response: distributionSchema,
  execution: distributionSchema,
  cluster: totalsSchema,
});
export const serverMetricSchema = z.strictObject({
  id: z.string().min(1).max(128),
  metrics: totalsSchema.extend({ active: z.boolean() }),
});
export const metricsSchema = z.strictObject({
  run_id: z.uuid(),
  bundle_digest: digest,
  summary: summarySchema,
  servers: z
    .record(z.string().max(128), totalsSchema.extend({ active: z.boolean() }))
    .refine((servers) => Object.keys(servers).length <= 32),
});
export const chartMetricSchema = z.enum([
  "cpu_utilization",
  "memory_utilization",
  "pending_depth",
  "active_requests",
]);
export const chartSchema = z.strictObject({
  run_id: z.uuid(),
  bundle_digest: digest,
  metric: chartMetricSchema,
  server_id: z.string().min(1).max(128).nullable(),
  start_tick: tick,
  end_tick: tick,
  points: z
    .array(
      z.strictObject({
        t: tick,
        end_tick: tick,
        covered_ticks: tick,
        min: valueSchema,
        max: valueSchema,
        mean: valueSchema,
      }),
    )
    .max(2000),
});
export type Chart = z.infer<typeof chartSchema>;
export const comparisonSchema = z.strictObject({
  baseline_run_id: z.uuid(),
  runs: z
    .array(
      z.strictObject({
        run_id: z.uuid(),
        bundle_digest: digest,
        configuration: readSettingsSchema,
        differing_settings: z.array(z.enum(["strategy", "autoscaling"])).max(2),
        metrics: z
          .record(
            z.string().max(128),
            z.strictObject({
              baseline: valueSchema.nullable(),
              candidate: valueSchema.nullable(),
              difference: valueSchema.nullable(),
              percentage_difference: valueSchema.nullable(),
            }),
          )
          .refine((values) => Object.keys(values).length <= 128),
      }),
    )
    .min(2)
    .max(8),
});
const identifier = z.string().min(1).max(128);
export const playbackSchema = z.strictObject({
  run_id: z.uuid(),
  bundle_digest: digest,
  tick,
  frame_tick: tick,
  duration_ticks: tick,
  terminal: z.boolean(),
  membership_count: z.number().int().min(1).max(64),
  server_offset: z.number().int().min(0).max(64),
  servers: z
    .array(
      serverInputSchema.extend({
        active_count: count,
        reserved_mem_mb: safe.nullable(),
        allocated_cpu: valueSchema.nullable(),
        consumed_cpu: valueSchema.nullable(),
        starts: count.nullable(),
      }),
    )
    .max(32),
  membership_added: z.array(identifier).max(64),
  membership_removed: z.array(identifier).max(64),
  queue_depth: count,
  active_count: count,
  request_kind: z.enum(["pending", "active"]),
  server_id: identifier.nullable(),
  server_present: z.boolean(),
  request_offset: count,
  request_count: count,
  request_ids: z.array(identifier).max(100),
  decisions: z
    .array(
      z.strictObject({
        t: tick,
        action: z.enum(["none", "scale_out", "scale_in"]),
        trigger: z.array(z.string().max(64)).max(8),
        applied_at: tick.nullable(),
        server_ids_added: z.array(identifier).max(64),
        server_ids_removed: z.array(identifier).max(64),
        reason: z.string().max(64).nullable(),
      }),
    )
    .max(2),
});
export type Playback = z.infer<typeof playbackSchema>;
export type PlaybackSelection = {
  tick: number;
  server_offset: number;
  request_offset: number;
  request_kind: "pending" | "active";
  server_id: string | null;
};
export const eventSchema = z.strictObject({
  sequence: z.number().int().min(1).max(150_000),
  t: tick,
  event: z.enum(["REQUEST_ARRIVED", "REQUEST_STARTED", "REQUEST_FINISHED", "REQUEST_DROPPED"]),
  request_id: identifier,
  server_id: identifier.nullable(),
  reason: z.enum(["UNSCHEDULABLE_MEMORY", "QUEUE_TIMEOUT", "QUEUE_CAPACITY"]).nullable(),
});
