import { expect, it } from "vitest";
import type { Server } from "../../shared/api/contracts.js";
import { demoInput, demoRecipes } from "./demoState.js";

it("keeps every recipe small, deterministic and within input bounds without changing servers", () => {
  const servers: Server[] = Array.from({ length: 64 }, (_, index) =>
    Object.freeze({
      id: `server-${index}`,
      version: 1,
      cpu_units_per_tick: 1_000_000_000,
      mem_mb: 1_000_000_000,
      rate_limit_per_sec: 1,
    }),
  );
  for (const recipe of demoRecipes) {
    const result = demoInput({ servers, kind: recipe.id });
    expect(result).toEqual(demoInput({ servers: [...servers].reverse(), kind: recipe.id }));
    const rows = result.csv
      .trim()
      .split("\n")
      .slice(1)
      .map((line) => line.split(","));
    expect(rows).toHaveLength(24);
    expect(new Set(rows.map((row) => row[1])).size).toBe(24);
    for (const row of rows) {
      expect(Number(row[0])).toBeGreaterThanOrEqual(0);
      for (const value of row.slice(2)) {
        expect(Number(value)).toBeGreaterThan(0);
        expect(Number(value)).toBeLessThanOrEqual(1_000_000_000);
      }
    }
    expect(result.settings.mode).toBe("concurrent");
    expect(result.settings.autoscaling.enabled).toBe(false);
  }
  expect(() => demoInput({ servers: [], kind: "balanced" })).toThrow("Configure");
  const heterogeneous: Server[] = [
    {
      id: "small-fast",
      version: 1,
      cpu_units_per_tick: 1_000_000_000,
      mem_mb: 1,
      rate_limit_per_sec: 1,
    },
    {
      id: "large-slow",
      version: 1,
      cpu_units_per_tick: 1,
      mem_mb: 1_000_000_000,
      rate_limit_per_sec: 1,
    },
  ];
  for (const recipe of demoRecipes) {
    expect(demoInput({ servers: heterogeneous, kind: recipe.id }).work).toBeLessThanOrEqual(2);
  }
  expect(() =>
    demoInput({
      servers: [
        ...servers,
        { id: "extra", version: 1, cpu_units_per_tick: 1, mem_mb: 1, rate_limit_per_sec: 1 },
      ],
      kind: "balanced",
    }),
  ).toThrow("Configure");
});
