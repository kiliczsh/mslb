import assert from "node:assert/strict";
import { test } from "node:test";

test("JSON preserves large decimal strings for BigInt decoding", () => {
  const component = "732666138205131859729707206340762241";
  const decoded: unknown = JSON.parse(JSON.stringify(component));
  assert.equal(typeof decoded, "string");
  assert.equal(BigInt(component).toString(), decoded);
});
