import { z } from "zod";
import { runSchema, statusSchema, strategySchema } from "../../shared/api/contracts.js";

export const historyEntrySchema = z.strictObject({
  id: z.uuid(),
  dataset_id: z.uuid(),
  source_filename: z.string().min(1).max(255),
  request_count: z.number().int().min(1).max(50_000),
  strategy: strategySchema,
  execution_mode: z.enum(["sequential", "concurrent", "docker"]),
  status: statusSchema,
  created_at: z.iso.datetime({ offset: true }),
  completed_at: z.iso.datetime({ offset: true }).nullable(),
  error_code: z.string().max(64).nullable(),
  progress: runSchema.shape.progress,
});
export type HistoryEntry = z.infer<typeof historyEntrySchema>;

export function failureMessage(code: string) {
  return (
    (
      {
        STORAGE_LIMIT_EXCEEDED: "Saved output reached its storage limit.",
        RESOURCE_PRESSURE: "The run stopped because safe resources were unavailable.",
        RESOURCE_LIMIT_EXCEEDED: "The workload exceeded a configured resource limit.",
        SIMULATION_LIMIT_EXCEEDED: "The simulation exceeded its configured execution limits.",
        VALIDATION_FAILED: "The saved output could not be validated.",
        INFRASTRUCTURE_FAILURE: "A service interruption prevented completion.",
      } as Record<string, string>
    )[code] ?? code.replaceAll("_", " ").toLowerCase()
  );
}
