import { describe, expect, it } from "vitest";
import { defaultSettings, type Run } from "../../shared/api/contracts.js";
import { chartPoints } from "./chartPoints.js";
import { compatibleRuns } from "./comparison.js";
import { chartNumber, displayValue, exactText } from "./metricValues.js";
import { createPlaybackStore } from "./playbackStore.js";
import { valueSchema } from "./resultContracts.js";

describe("exact result presentation", () => {
  it("closes the last chart interval and breaks sparse membership without bridging gaps", () => {
    const points = chartPoints([
      { t: 0, end_tick: 1, covered_ticks: 1, min: 1, max: 1, mean: 1 },
      { t: 4, end_tick: 5, covered_ticks: 1, min: 2, max: 2, mean: 2 },
    ]);
    expect(points.map((point) => [point.t, point.mean])).toEqual([
      [0, 1],
      [1, 1],
      [1, null],
      [4, 2],
      [5, 2],
    ]);
    const worst = chartPoints(
      Array.from({ length: 500 }, (_, index) => ({
        t: index * 2,
        end_tick: index * 2 + 1,
        covered_ticks: 1,
        min: 0,
        max: 1,
        mean: 1,
      })),
    );
    expect(worst.length).toBeLessThanOrEqual(1500);
  });
  it("preserves huge exact quantities and avoids Infinity/Infinity in chart conversion", () => {
    const value = {
      numerator: ((1n << 1000n) + 1n).toString(),
      denominator: ((1n << 1001n) + 1n).toString(),
    };
    expect(valueSchema.parse(value)).toEqual(value);
    expect(chartNumber(value)).toBe(0.5);
    expect(displayValue(value)).toBe("≈ 0.5");
    expect(exactText(value)).toBe(`${value.numerator}/${value.denominator}`);
    expect(exactText({ numerator: "9007199254740993", denominator: "1" })).toBe("9007199254740993");
    expect(displayValue({ numerator: "-1", denominator: "3" })).toBe("≈ -0.3333");
    expect(displayValue(null)).toBe("Not defined");
    expect(valueSchema.safeParse(9007199254740992).success).toBe(false);
  });
  it("keeps playback state scoped and changes only the presentation position/speed", () => {
    const first = createPlaybackStore(10),
      second = createPlaybackStore(10);
    first.getState().toggle();
    first.getState().setSpeed(4);
    first.getState().advance(5);
    expect(first.getState().tick).toBe(5);
    expect(second.getState().tick).toBe(0);
    first.getState().seek(7);
    expect(first.getState().playing).toBe(false);
    first.getState().seek(11);
    first.getState().setSpeed(100);
    expect(first.getState().tick).toBe(7);
    expect(first.getState().speed).toBe(4);
    expect(defaultSettings.strategy).toBe("first_fit");
  });
  it("permits comparison dimensions but rejects changed frozen semantic inputs", () => {
    const first: Run = {
      id: "00000000-0000-0000-0000-000000000001",
      dataset_id: "00000000-0000-0000-0000-000000000002",
      snapshot_digest: "a".repeat(64),
      dataset_digest: "b".repeat(64),
      server_snapshot_digest: "c".repeat(64),
      engine_version: "0.2.0",
      configuration: structuredClone(defaultSettings),
      status: "completed",
      attempts: 1,
      created_at: "2026-11-06T00:00:00Z",
      completed_at: "2026-11-06T00:00:01Z",
      error_code: null,
      progress: null,
    };
    const other = structuredClone(first);
    other.configuration.strategy = "round_robin";
    expect(compatibleRuns(first, other)).toBe(true);
    other.configuration.max_queue_size = 1;
    expect(compatibleRuns(first, other)).toBe(false);
    other.configuration.max_queue_size = null;
    other.dataset_digest = "d".repeat(64);
    expect(compatibleRuns(first, other)).toBe(false);
  });
});
