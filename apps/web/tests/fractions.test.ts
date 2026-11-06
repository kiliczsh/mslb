import assert from "node:assert/strict";
import { test } from "node:test";
import { parseFraction, serializeFraction } from "../src/shared/fractions.js";

test("120-bit simulation metric survives JSON and BigInt round trips", () => {
  const wire = {
    numerator: "732666138205131859729707206340762241",
    denominator: "734184632222154704090370027645633600",
  };
  assert.deepEqual(serializeFraction(parseFraction(JSON.parse(JSON.stringify(wire)))), wire);
});

test("rejects ambiguous, oversized, non-string and unreduced values", () => {
  for (const numerator of [
    "-0",
    "+1",
    "01",
    "1\n",
    "１",
    "9".repeat(10000),
    (2n ** 4096n).toString(),
    1,
  ]) {
    assert.throws(() => parseFraction({ numerator, denominator: "1" }));
  }
  for (const denominator of ["0", "-1", "01", "1\n"]) {
    assert.throws(() => parseFraction({ numerator: "1", denominator }));
  }
  assert.throws(() => parseFraction({ numerator: "2", denominator: "4" }));
  assert.throws(() => parseFraction({ numerator: "0", denominator: "2" }));
  assert.throws(() => parseFraction({ numerator: "1", denominator: "2", extra: true }));
});

test("accepts zero, negative fractions, and the bit-limit boundary", () => {
  for (const wire of [
    { numerator: "0", denominator: "1" },
    { numerator: "-2", denominator: "3" },
    { numerator: (2n ** 4095n).toString(), denominator: "1" },
  ])
    assert.deepEqual(serializeFraction(parseFraction(wire)), wire);
});
