import { Given, Then, When } from "@cucumber/cucumber"
import type { Effect } from "effect"
import type { FiregridWorld } from "./world.ts"
import type { HarnessServices } from "../../packages/spec-harness/src/world.ts"
import { runSpecEffect } from "../../packages/spec-harness/src/world.ts"

type EffectStepBody<Args extends Array<unknown>> = (
  this: FiregridWorld,
  ...args: Args
) => Effect.Effect<unknown, unknown, HarnessServices>

type EffectStepCode = (this: FiregridWorld, ...args: Array<unknown>) => Promise<unknown>

const defineEffectStep =
  <Args extends Array<unknown>>(
    define: typeof Given,
    pattern: string,
    body: EffectStepBody<Args>,
  ): void =>
    define<FiregridWorld>(pattern, codeFor(body))

const runBody = (
  world: FiregridWorld,
  body: EffectStepBody<Array<unknown>>,
  args: Array<unknown>,
): Promise<unknown> => runSpecEffect(world, body.apply(world, args))

const codeFor = <Args extends Array<unknown>>(
  body: EffectStepBody<Args>,
): EffectStepCode => {
  const effectBody = body as EffectStepBody<Array<unknown>>
  switch (body.length) {
    case 0:
      return function(this: FiregridWorld) {
        return runBody(this, effectBody, [])
      }
    case 1:
      return function(this: FiregridWorld, a: unknown) {
        return runBody(this, effectBody, [a])
      }
    case 2:
      return function(this: FiregridWorld, a: unknown, b: unknown) {
        return runBody(this, effectBody, [a, b])
      }
    case 3:
      return function(this: FiregridWorld, a: unknown, b: unknown, c: unknown) {
        return runBody(this, effectBody, [a, b, c])
      }
    case 4:
      return function(this: FiregridWorld, a: unknown, b: unknown, c: unknown, d: unknown) {
        return runBody(this, effectBody, [a, b, c, d])
      }
    default:
      throw new Error(`Effect step bodies with ${body.length.toString()} parameters are not supported`)
  }
}

export const GivenEffect = <Args extends Array<unknown>>(
  pattern: string,
  body: EffectStepBody<Args>,
): void => defineEffectStep(Given, pattern, body)

export const WhenEffect = <Args extends Array<unknown>>(
  pattern: string,
  body: EffectStepBody<Args>,
): void => defineEffectStep(When, pattern, body)

export const ThenEffect = <Args extends Array<unknown>>(
  pattern: string,
  body: EffectStepBody<Args>,
): void => defineEffectStep(Then, pattern, body)
