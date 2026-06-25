import type { SleepOptions, StepOptions } from "@tanstack/workflow-core"
import * as Effect from "effect/Effect"

import { FluentDurableContext, type RunAction } from "./context.ts"
import { FluentFiregridError } from "./error.ts"

export interface RunOptions extends StepOptions {
  readonly name?: string
}

const actionName = <A>(action: RunAction<A>, options: RunOptions | undefined): string | undefined =>
  options?.name ?? action.name

export const run = <A>(
  action: RunAction<A>,
  options?: RunOptions
): Effect.Effect<A, FluentFiregridError, FluentDurableContext> => {
  const name = actionName(action, options)
  if (name === undefined || name === "") {
    return Effect.fail(
      new FluentFiregridError({ message: "run(action, options) requires options.name or a named action" })
    )
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) => ctx.step(name, action, options))
  )
}

export const sleep = (
  ms: number,
  options?: SleepOptions
): Effect.Effect<void, FluentFiregridError, FluentDurableContext> =>
  FluentDurableContext.pipe(
    Effect.flatMap((ctx) => ctx.sleep(ms, options))
  )

export const sleepUntil = (
  timestamp: number,
  options?: SleepOptions
): Effect.Effect<void, FluentFiregridError, FluentDurableContext> =>
  FluentDurableContext.pipe(
    Effect.flatMap((ctx) => ctx.sleepUntil(timestamp, options))
  )
