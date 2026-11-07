import { expect, it } from "vitest";
import {
  defaultDockerSettings,
  dockerCreateSchema,
  dockerDetailSchema,
  dockerSummarySchema,
} from "./dockerContracts.js";

it("keeps physical requests strict, bounded and separate from logical configuration", () => {
  const input = { ...defaultDockerSettings, dataset_id: "00000000-0000-4000-8000-000000000001" };
  expect(dockerCreateSchema.safeParse(input).success).toBe(true);
  for (const change of [
    { image: "caller-image" },
    { command: ["anything"] },
    { max_replicas: 5 },
    { initial_replicas: 1, min_replicas: 2 },
    { mode: "concurrent" },
    { max_queue_size: 50 },
  ])
    expect(dockerCreateSchema.safeParse({ ...input, ...change }).success).toBe(false);
  expect(dockerDetailSchema.safeParse({ status: "completed", validation: {} }).success).toBe(false);
  expect(
    dockerSummarySchema.safeParse({
      schema_version: 1,
      mode: "docker",
      status: "completed",
      stop_reason: null,
      total: 2,
      completed: 1,
      failed: 0,
      dropped: 0,
      unexecuted: 0,
      elapsed_seconds: 1,
      peak_pending: 2,
      peak_inflight: 1,
    }).success,
  ).toBe(false);
});
