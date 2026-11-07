import { z } from "zod";
import { fractionSchema } from "../fractions.js";
import { resourceDecisionSchema } from "./resourceContracts.js";

const integer = (max: number, min = 0) => z.number().int().min(min).max(max);
const capacity = integer(1_000_000_000, 1);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const id = z.uuid();
const time = z.iso.datetime({ offset: true });

export const serverInputSchema = z.strictObject({
  id: z
    .string()
    .trim()
    .min(1)
    .max(256)
    .transform((value) => value.normalize("NFC"))
    .refine(
      (value) =>
        value !== "." &&
        value !== ".." &&
        !value.includes("\u0000") &&
        new TextEncoder().encode(value).length <= 128 &&
        Array.from(value).every((character) => {
          const point = character.codePointAt(0) ?? 0;
          return point < 0xd800 || point > 0xdfff;
        }),
      "Server ID must contain 1–128 UTF-8 bytes, exclude NUL, and differ from . and ..",
    ),
  cpu_units_per_tick: capacity,
  mem_mb: capacity,
  rate_limit_per_sec: capacity,
});
export const serverSchema = serverInputSchema.extend({ version: integer(2 ** 31 - 1, 1) });
export type Server = z.infer<typeof serverSchema>;
export type ServerInput = z.infer<typeof serverInputSchema>;

export function pageSchema<T extends z.ZodType>(item: T) {
  return z.strictObject({
    items: z.array(item).max(500),
    next_cursor: z.string().max(2048).nullable(),
  });
}
export const datasetSchema = z.strictObject({
  id,
  digest,
  source_filename: z.string().min(1).max(255),
  request_count: integer(50_000, 1),
  earliest_arrival: integer(999_999),
  latest_arrival: integer(999_999),
  size_bytes: integer(10 * 1024 ** 2, 1),
  created_at: time,
  original_digest: digest.nullable(),
  source_size_bytes: integer(10 * 1024 ** 2, 1).nullable(),
});
export type Dataset = z.infer<typeof datasetSchema>;
export const strategySchema = z.enum([
  "first_fit",
  "round_robin",
  "least_connections",
  "most_available_memory",
]);
export const templateSchema = serverInputSchema.omit({ id: true });
export const scalingSchema = z.strictObject({
  enabled: z.boolean(),
  window_ticks: integer(10_000, 1),
  scale_out_cpu_threshold: fractionSchema,
  scale_in_cpu_threshold: fractionSchema,
  scale_out_drop_rate_threshold: fractionSchema,
  scale_out_queue_threshold: integer(10_000),
  cooldown_ticks: integer(1_000_000),
  min_servers: integer(64, 1).nullable(),
  max_servers: integer(64, 1).nullable(),
  scale_out_step: integer(64, 1),
  scale_in_step: integer(64, 1),
  server_template: templateSchema.nullable(),
});
export const settingsSchema = z.strictObject({
  mode: z.enum(["concurrent", "sequential"]),
  strategy: strategySchema,
  queue_timeout_ticks: integer(1_000_000, 1).nullable(),
  max_queue_size: integer(10_000).nullable(),
  max_simulation_ticks: integer(1_000_000, 1),
  max_samples: integer(1_000_000, 1),
  max_fraction_bits: integer(4096, 1),
  max_metric_series: integer(10_000, 1),
  autoscaling: scalingSchema,
});
export type Settings = z.infer<typeof settingsSchema>;
export const readSettingsSchema = z.union([
  settingsSchema,
  settingsSchema.extend({ capacity_profile: z.literal("stress"), max_requests: z.literal(50_000) }),
]);
export const defaultSettings: Settings = {
  mode: "sequential",
  strategy: "first_fit",
  queue_timeout_ticks: null,
  max_queue_size: null,
  max_simulation_ticks: 100_000,
  max_samples: 1_000_000,
  max_fraction_bits: 4096,
  max_metric_series: 10_000,
  autoscaling: {
    enabled: false,
    window_ticks: 5,
    scale_out_cpu_threshold: { numerator: "4", denominator: "5" },
    scale_in_cpu_threshold: { numerator: "1", denominator: "5" },
    scale_out_drop_rate_threshold: { numerator: "1", denominator: "10" },
    scale_out_queue_threshold: 1,
    cooldown_ticks: 3,
    min_servers: null,
    max_servers: null,
    scale_out_step: 1,
    scale_in_step: 1,
    server_template: null,
  },
};
export const statusSchema = z.enum([
  "queued",
  "running",
  "cancel_requested",
  "completed",
  "failed",
  "cancelled",
]);
export const runSchema = z.strictObject({
  id,
  dataset_id: id,
  snapshot_digest: digest,
  dataset_digest: digest,
  server_snapshot_digest: digest,
  engine_version: z.string().min(1).max(64),
  configuration: readSettingsSchema,
  status: statusSchema,
  attempts: integer(3),
  created_at: time,
  completed_at: time.nullable(),
  error_code: z.string().max(64).nullable(),
  progress: z
    .strictObject({
      revision: integer(Number.MAX_SAFE_INTEGER, 1),
      kind: z.enum(["checkpoint", "terminal"]),
      next_tick: integer(1_000_000),
      executed_intervals: integer(1_000_000),
      committed_events: integer(150_000),
    })
    .nullable(),
});
const manifestSchema = z.strictObject({
  schema_version: z.literal(1),
  engine_version: z.string().min(1).max(64),
  snapshot_digest: digest,
  bundle_digest: digest,
  terminal_tick: integer(1_000_000),
  event_count: integer(150_000, 1),
  decision_count: integer(1_000_000),
  independent_replay: z.literal("passed"),
  original_validator: z.enum(["passed", "not_applicable"]),
  original_validator_digest: digest,
  artifacts: z.record(
    z.string().max(128),
    z.strictObject({ size_bytes: integer(256 * 1024 ** 2), sha256: digest }),
  ),
});
export const detailSchema = runSchema.extend({
  mode: z.string().max(64),
  strategy: z.string().max(64),
  validation: manifestSchema.nullable(),
});
export type Run = z.infer<typeof runSchema>;
export const acceptedSchema = z.strictObject({
  run_id: id,
  status: statusSchema,
  snapshot_digest: digest,
});
export const errorSchema = z.strictObject({
  error: z.strictObject({
    code: z.string().max(64),
    message: z.string().max(1024),
    details: z
      .array(z.strictObject({ field: z.string().max(256), reason: z.string().max(128) }))
      .max(32),
  }),
  request_id: z.string().max(128),
});
export const readinessSchema = z.strictObject({
  ready: z.boolean(),
  api_ready: z.boolean(),
  complete_product: z.boolean(),
  resource_admission: resourceDecisionSchema.nullable().optional(),
  capabilities: z.record(z.string().max(64), z.string().max(64)),
});
export function terminal(status: Run["status"]) {
  return status === "completed" || status === "failed" || status === "cancelled";
}
