/* oxlint-disable effect/restricted-syntax -- This module bridges Effect handlers into TanStack's Promise-based ctx.step boundary. */
import type { SleepOptions, StepContext, StepOptions } from "@tanstack/workflow-core"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"

import { FluentFiregridError } from "./error.ts"

export interface RunActionContext {
  readonly id: string
  readonly attempt: number
  readonly signal: AbortSignal
}

export type RunAction<A> = (
  context: RunActionContext
) => A | PromiseLike<A> | Effect.Effect<A, unknown, never>

export interface FluentDurableContextService {
  readonly step: <A>(
    name: string,
    action: RunAction<A>,
    options?: StepOptions
  ) => Effect.Effect<A, FluentFiregridError>
  readonly sleep: (ms: number, options?: SleepOptions) => Effect.Effect<void, FluentFiregridError>
  readonly sleepUntil: (timestamp: number, options?: SleepOptions) => Effect.Effect<void, FluentFiregridError>
}

export class FluentDurableContext extends Context.Service<FluentDurableContext, FluentDurableContextService>()(
  "@firegrid/fluent-firegrid/FluentDurableContext"
) {}

export interface TanStackWorkflowContext {
  readonly step: <A>(
    id: string,
    fn: (stepContext: StepContext) => A | Promise<A>,
    options?: StepOptions
  ) => Promise<A>
  readonly sleep: (ms: number, options?: SleepOptions) => Promise<void>
  readonly sleepUntil: (timestamp: number, options?: SleepOptions) => Promise<void>
}

export const fluentContextFromTanStack = (ctx: TanStackWorkflowContext): FluentDurableContextService => ({
  sleep: (ms, options) =>
    Effect.tryPromise({
      try: () => ctx.sleep(ms, options),
      catch: (cause) => new FluentFiregridError({ cause, message: `sleep(${ms}) failed` })
    }),
  sleepUntil: (timestamp, options) =>
    Effect.tryPromise({
      try: () => ctx.sleepUntil(timestamp, options),
      catch: (cause) => new FluentFiregridError({ cause, message: `sleepUntil(${timestamp}) failed` })
    }),
  step: (name, action, options) =>
    Effect.tryPromise({
      try: () =>
        ctx.step(
          name,
          (stepContext) => {
            const value = action(stepContext)
            return Effect.isEffect(value) ? Effect.runPromise(value) : Promise.resolve(value)
          },
          options
        ),
      catch: (cause) => new FluentFiregridError({ cause, message: `step ${name} failed` })
    })
})
