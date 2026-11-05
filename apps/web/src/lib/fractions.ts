import { z } from "zod";

const maxBits = 4096;

function gcd(a: bigint, b: bigint): bigint {
  let left = a < 0n ? -a : a;
  let right = b;
  while (right !== 0n) {
    [left, right] = [right, left % right];
  }
  return left;
}

export const fractionSchema = z
  .strictObject({
    numerator: z
      .string()
      .max(1235)
      .regex(/^(0|-?[1-9][0-9]*)(?![\s\S])/),
    denominator: z
      .string()
      .max(1234)
      .regex(/^[1-9][0-9]*(?![\s\S])/),
  })
  .superRefine((value, context) => {
    // superRefine can run after a recoverable string check failed. Never convert
    // unbounded or invalid text to BigInt even in that case.
    if (
      value.numerator.length > 1235 ||
      value.denominator.length > 1234 ||
      !/^(0|-?[1-9][0-9]*)(?![\s\S])/.test(value.numerator) ||
      !/^[1-9][0-9]*(?![\s\S])/.test(value.denominator) ||
      value.numerator.includes("\n") ||
      value.denominator.includes("\n")
    )
      return;
    const numerator = BigInt(value.numerator);
    const denominator = BigInt(value.denominator);
    const absolute = numerator < 0n ? -numerator : numerator;
    if (Math.max(absolute.toString(2).length, denominator.toString(2).length) > maxBits) {
      context.addIssue({ code: "custom", message: "Fraction exceeds 4096 bits" });
    }
    if (gcd(numerator, denominator) !== 1n) {
      context.addIssue({ code: "custom", message: "Fraction must be reduced; zero must be 0/1" });
    }
  });

export type FractionWire = z.infer<typeof fractionSchema>;
export type ExactFraction = Readonly<{ numerator: bigint; denominator: bigint }>;

export function parseFraction(value: unknown): ExactFraction {
  const parsed = fractionSchema.parse(value);
  return { numerator: BigInt(parsed.numerator), denominator: BigInt(parsed.denominator) };
}

export function serializeFraction(value: ExactFraction): FractionWire {
  return fractionSchema.parse({
    numerator: value.numerator.toString(),
    denominator: value.denominator.toString(),
  });
}
