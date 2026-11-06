import { expect, it } from "vitest";
import { defaultSettings, serverInputSchema, settingsSchema } from "./contracts.js";

it("defaults to sequential and accepts only current execution mode names", () => {
  expect(defaultSettings.mode).toBe("sequential");
  expect(settingsSchema.parse(defaultSettings).mode).toBe("sequential");
  expect(settingsSchema.parse({ ...defaultSettings, mode: "concurrent" }).mode).toBe("concurrent");
  expect(settingsSchema.safeParse({ ...defaultSettings, mode: "sequential_compat" }).success).toBe(
    false,
  );
});

it.each([".", "..", " . ", " .. ", "a\u0000b"])("rejects an unaddressable identifier %j", (id) => {
  expect(
    serverInputSchema.safeParse({ id, cpu_units_per_tick: 1, mem_mb: 1, rate_limit_per_sec: 1 })
      .success,
  ).toBe(false);
});

it.each(["...", "a.b", "é/cluster", "a/../b", "%2e", "server?part#key"])(
  "preserves addressable identifier %j in an encoded URL component",
  (id) => {
    const parsed = serverInputSchema.parse({
      id,
      cpu_units_per_tick: 1,
      mem_mb: 1,
      rate_limit_per_sec: 1,
    });
    const path = `/api/v1/servers/${encodeURIComponent(parsed.id)}`;
    expect(new URL(path, "http://localhost").pathname).toBe(path);
  },
);
