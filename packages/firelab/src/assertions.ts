import * as Equal from "effect/Equal"
import * as Option from "effect/Option"
import * as assert from "node:assert/strict"

export const fail = (message: string): never => assert.fail(message)

export const deepStrictEqual = <A>(
  actual: A,
  expected: A,
  message?: string,
): void => {
  assert.deepStrictEqual(actual, expected, message)
}

export const strictEqual = <A>(
  actual: A,
  expected: A,
  message?: string,
): void => {
  assert.strictEqual(actual, expected, message)
}

export const assertEquals = <A>(
  actual: A,
  expected: A,
  message?: string,
): void => {
  if (!Equal.equals(actual, expected)) {
    deepStrictEqual(actual, expected, message)
    fail(message ?? "Expected values to satisfy Equal.equals")
  }
}

export const assertTrue = (
  actual: unknown,
  message?: string,
): void => {
  strictEqual(actual, true, message)
}

export const assertNone = <A>(
  option: Option.Option<A>,
): void => {
  deepStrictEqual(option, Option.none<A>())
}

export const assertSome = <A>(
  option: Option.Option<A>,
  expected: A,
): void => {
  deepStrictEqual(option, Option.some(expected))
}
