import { z } from "zod";
import { statusSchema } from "./contracts.js";
import { backendMeasurementsSchema, dockerProgressSchema } from "./dockerObservationContracts.js";
import { resourceDecisionSchema } from "./resourceContracts.js";

const integer = (max: number, min = 0) => z.number().int().min(min).max(max);
const digest = z.string().regex(/^[0-9a-f]{64}$/);
const time = z.iso.datetime({ offset: true });
export const dockerSettingsSchema = z.strictObject({
  mode: z.literal("docker"),
  strategy: z.enum(["first_fit", "round_robin", "least_connections", "most_available_memory"]),
  backend_template: z.literal("bounded-python-work"),
  initial_replicas: integer(4, 1),
  min_replicas: integer(4, 1),
  max_replicas: integer(4, 1),
  global_inflight: integer(16, 1),
  backend_inflight: integer(4, 1),
  logical_admission_mem_mb: integer(1_000_000_000, 1),
  starts_per_backend_second: integer(32, 1),
  max_pending: integer(256, 1),
  pending_timeout_seconds: integer(120, 1),
  duration_seconds: integer(120, 1),
  request_timeout_ms: integer(3000, 100),
  health_timeout_ms: integer(1000, 100),
  drain_timeout_seconds: integer(5, 1),
  backend_memory_mb: integer(128, 96),
  backend_nano_cpus: integer(500_000_000, 125_000_000),
  physical_scaling: z.strictObject({
    enabled: z.boolean(),
    evaluation_seconds: z.literal(1),
    pending_samples: integer(10, 2),
    idle_samples: integer(30, 5),
    cooldown_seconds: integer(30, 3),
  }),
});
export type DockerSettings = z.infer<typeof dockerSettingsSchema>;
export const defaultDockerSettings: DockerSettings = {
  mode: "docker",
  strategy: "first_fit",
  backend_template: "bounded-python-work",
  initial_replicas: 2,
  min_replicas: 1,
  max_replicas: 4,
  global_inflight: 16,
  backend_inflight: 4,
  logical_admission_mem_mb: 1024,
  starts_per_backend_second: 4,
  max_pending: 64,
  pending_timeout_seconds: 5,
  duration_seconds: 120,
  request_timeout_ms: 3000,
  health_timeout_ms: 1000,
  drain_timeout_seconds: 3,
  backend_memory_mb: 128,
  backend_nano_cpus: 500_000_000,
  physical_scaling: {
    enabled: false,
    evaluation_seconds: 1,
    pending_samples: 2,
    idle_samples: 5,
    cooldown_seconds: 3,
  },
};
export const dockerCreateSchema = dockerSettingsSchema
  .extend({ dataset_id: z.uuid() })
  .refine(
    (value) =>
      value.min_replicas <= value.initial_replicas && value.initial_replicas <= value.max_replicas,
    { message: "Replica bounds must satisfy minimum ≤ initial ≤ maximum." },
  );
export type DockerCreate = z.infer<typeof dockerCreateSchema>;
export const dockerRunSchema = z.strictObject({
  mode: z.literal("docker"),
  id: z.uuid(),
  dataset_id: z.uuid(),
  snapshot_digest: digest,
  dataset_digest: digest,
  configuration: dockerSettingsSchema,
  status: statusSchema,
  attempts: integer(3),
  created_at: time,
  completed_at: time.nullable(),
  error_code: z.string().max(64).nullable(),
});
const reason = z.enum([
  "CANCELLED",
  "FENCE_LOST",
  "RESOURCE_PRESSURE",
  "RUN_DEADLINE",
  "DRAIN_TIMEOUT",
  "PENDING_TIMEOUT",
  "PENDING_OVERFLOW",
  "UNSCHEDULABLE_MEMORY",
  "HTTP_TIMEOUT",
  "HTTP_STATUS",
  "CONNECTION_FAILED",
  "INVALID_RESPONSE",
  "HTTP_FAILURE",
]);
export const dockerSummarySchema = z
  .strictObject({
    schema_version: z.literal(1),
    mode: z.literal("docker"),
    status: z.enum(["completed", "cancelled", "failed"]),
    stop_reason: z
      .enum(["CANCELLED", "FENCE_LOST", "RESOURCE_PRESSURE", "RUN_DEADLINE"])
      .nullable(),
    total: integer(10_000, 1),
    completed: integer(10_000),
    failed: integer(10_000),
    dropped: integer(10_000),
    unexecuted: integer(10_000),
    elapsed_seconds: z.number().finite().nonnegative(),
    peak_pending: integer(256),
    peak_inflight: integer(16),
  })
  .refine((s) => s.completed + s.failed + s.dropped + s.unexecuted === s.total, {
    message: "HTTP accounting differs.",
  });
export const dockerArtifactNames = [
  "http_events.jsonl",
  "container_lifecycle.jsonl",
  "runtime_summary.json",
  "snapshot.json",
  "requests.csv",
] as const;
export const dockerArtifacts = [...dockerArtifactNames, "inputs.zip"] as const;
export const dockerDetailSchema = z.strictObject({
  run: dockerRunSchema,
  cleanup_pending: z.boolean(),
  resources: z
    .array(
      z.strictObject({
        id: z.uuid(),
        name: z.string().min(1).max(128),
        kind: z.enum(["static", "dynamic"]),
        state: z.enum([
          "prepared",
          "creating",
          "observed",
          "starting",
          "started",
          "draining",
          "removed",
          "released",
          "abandoned",
          "conflict",
          "unresolved",
        ]),
      }),
    )
    .max(64),
  summary: dockerSummarySchema.nullable(),
  progress: dockerProgressSchema.nullable().default(null),
  observations: z.array(backendMeasurementsSchema).max(64).default([]),
  artifacts: z
    .array(
      z.strictObject({
        name: z.enum(dockerArtifactNames),
        sha256: digest,
        size_bytes: integer(256 * 1024 ** 2),
      }),
    )
    .max(5),
  admission: resourceDecisionSchema.nullable(),
});
export type DockerDetail = z.infer<typeof dockerDetailSchema>;
export const dockerEventSchema = z.strictObject({
  schema_version: z.literal(1),
  mode: z.literal("docker"),
  sequence: integer(30_000, 1),
  kind: z.enum(["arrived", "admitted", "completed", "failed", "dropped", "unexecuted"]),
  elapsed_seconds: z.number().finite().nonnegative(),
  request_id: z.string().min(1).max(256),
  backend_id: z.string().min(1).max(128).nullable(),
  reason: reason.nullable(),
  result: z
    .strictObject({
      backend_id: z.string().max(128),
      request_id: z.string().max(128),
      outcome: z.literal("completed"),
      operations: integer(2_000_000, 1),
      checksum: z.string().regex(/^[0-9a-f]{8}$/),
      duration_ms: z.number().min(0).max(3000),
    })
    .nullable(),
  run_id: z.uuid(),
  fence: integer(Number.MAX_SAFE_INTEGER, 1),
  snapshot_digest: digest,
  observed_at: time,
});
