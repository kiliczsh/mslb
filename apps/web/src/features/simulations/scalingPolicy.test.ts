import { expect, it } from "vitest";
import { defaultSettings } from "../../shared/api/contracts.js";
import { scalingPolicyError } from "./scalingPolicy.js";

it("compares CPU thresholds with exact integer arithmetic beyond Number precision", () => {
  const policy = structuredClone(defaultSettings.autoscaling);
  policy.scale_out_cpu_threshold = {
    numerator: "9007199254740993",
    denominator: "9007199254740994",
  };
  policy.scale_in_cpu_threshold = {
    numerator: "9007199254740992",
    denominator: "9007199254740993",
  };
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toBeNull();
  policy.scale_in_cpu_threshold = policy.scale_out_cpu_threshold;
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toContain("below scale-out");
});

it("requires bounded ratios, an enabled template and cluster-compatible bounds", () => {
  const policy = structuredClone(defaultSettings.autoscaling);
  policy.scale_out_drop_rate_threshold = { numerator: "2", denominator: "1" };
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toContain("between 0 and 1");
  policy.scale_out_drop_rate_threshold = { numerator: "-1", denominator: "1" };
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toContain("between 0 and 1");
  policy.scale_out_drop_rate_threshold = { numerator: "1", denominator: "10" };
  policy.enabled = true;
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toContain(
    "explicit server template",
  );
  policy.server_template = { cpu_units_per_tick: 2, mem_mb: 100, rate_limit_per_sec: 3 };
  policy.min_servers = 3;
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toContain("initial server count");
  policy.min_servers = null;
  policy.max_servers = 1;
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toContain("initial server count");
  policy.max_servers = null;
  expect(scalingPolicyError({ policy, initialServerCount: 2 })).toBeNull();
});
