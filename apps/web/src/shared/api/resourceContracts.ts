import { z } from "zod";
export const resourceDecisionSchema = z.strictObject({
  admit: z.boolean(),
  stop_running: z.boolean(),
  reason: z
    .enum([
      "MEASUREMENTS_UNAVAILABLE",
      "MEASUREMENTS_STALE",
      "ENGINE_CAPACITY",
      "MISSING_SERVICE",
      "MEMORY_HEADROOM",
      "RESOURCE_PRESSURE",
      "WARMING_UP",
    ])
    .nullable(),
  cap_sum_bytes: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
  required_headroom_bytes: z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER),
});
