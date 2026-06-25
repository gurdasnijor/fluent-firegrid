/* oxlint-disable effect/restricted-syntax -- This module bridges Effect handlers into TanStack's Promise-based ctx.step boundary. */
import type {
  DeterministicValueOptions,
  SleepOptions,
  StepContext,
  StepOptions,
  WaitForEventOptions
} from "@tanstack/workflow-core"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import type * as Option from "effect/Option"

import type { InvocationBinding } from "./clients.ts"
import { FluentFiregridError } from "./error.ts"
import type { StatePredicate } from "./statePredicate.ts"

export interface StateWaitBackendOptions {
  readonly name: string
  readonly signalName: string
  readonly timeoutAt?: number
  readonly timeoutMs?: number
  readonly waitId?: string
}

export interface SignalOperationIdentityInput {
  readonly kind: "awakeable" | "workflowEvent"
  readonly name: string
}

export interface ExternalSignalDeliveryRequest<Payload = unknown> {
  readonly runId: string
  readonly signalId: string
  readonly stepId?: string
  readonly name: string
  readonly payload: Payload
  readonly metadata?: Readonly<Record<string, unknown>>
}

export interface ExternalSignalDelivery {
  readonly kind: string
  readonly runId: string
  readonly workflowId?: string
}

export interface ExternalSignalBinding<Error = unknown, Requirements = never> {
  readonly deliverSignal: <Payload = unknown>(
    request: ExternalSignalDeliveryRequest<Payload>
  ) => Effect.Effect<ExternalSignalDelivery, Error, Requirements>
}

export interface ObjectStateBackend {
  readonly get: (
    table: string,
    key: string,
    options?: { readonly readId?: string }
  ) => Effect.Effect<Option.Option<unknown>, FluentFiregridError>
  readonly set: (
    table: string,
    key: string,
    value: unknown,
    options?: { readonly opId?: string }
  ) => Effect.Effect<void, FluentFiregridError>
  readonly delete: (
    table: string,
    key: string,
    options?: { readonly opId?: string }
  ) => Effect.Effect<void, FluentFiregridError>
  readonly waitFor?: (
    table: string,
    key: string,
    predicate: StatePredicate,
    options: StateWaitBackendOptions
  ) => Effect.Effect<Option.Option<unknown>, FluentFiregridError>
}

interface StateOperationIdentityInput {
  readonly kind: "get" | "set" | "delete" | "waitFor"
  readonly table: string
  readonly key: string
}

export interface RunActionContext {
  readonly id: string
  readonly attempt: number
  readonly signal: AbortSignal
}

export type RunAction<A> = (
  context: RunActionContext
) => A | PromiseLike<A> | Effect.Effect<A, unknown, never>

export interface FluentDurableContextService {
  readonly binding?: InvocationBinding<FluentFiregridError>
  readonly externalSignals?: ExternalSignalBinding<FluentFiregridError>
  readonly key?: string
  readonly runId?: string
  readonly state?: ObjectStateBackend
  readonly signalOperationId?: (input: SignalOperationIdentityInput) => string
  readonly stateOperationId?: (input: StateOperationIdentityInput) => string
  readonly step: <A>(
    name: string,
    action: RunAction<A>,
    options?: StepOptions
  ) => Effect.Effect<A, FluentFiregridError>
  readonly now?: (options?: DeterministicValueOptions) => Effect.Effect<number, FluentFiregridError>
  readonly sleep: (ms: number, options?: SleepOptions) => Effect.Effect<void, FluentFiregridError>
  readonly sleepUntil: (timestamp: number, options?: SleepOptions) => Effect.Effect<void, FluentFiregridError>
  readonly waitForSignal: <Payload>(
    name: string,
    options?: WaitForEventOptions<Payload>
  ) => Effect.Effect<Payload, FluentFiregridError>
}

export class FluentDurableContext extends Context.Service<FluentDurableContext, FluentDurableContextService>()(
  "@firegrid/fluent-firegrid/FluentDurableContext"
) {}

export interface TanStackWorkflowContext {
  readonly runId?: string
  readonly step: <A>(
    id: string,
    fn: (stepContext: StepContext) => A | Promise<A>,
    options?: StepOptions
  ) => Promise<A>
  readonly sleep: (ms: number, options?: SleepOptions) => Promise<void>
  readonly sleepUntil: (timestamp: number, options?: SleepOptions) => Promise<void>
  readonly now?: (options?: DeterministicValueOptions) => Promise<number>
  readonly waitForEvent: <Payload = unknown>(name: string, options?: WaitForEventOptions<Payload>) => Promise<Payload>
}

export const fluentContextFromTanStack = (
  ctx: TanStackWorkflowContext,
  options: {
    readonly binding?: InvocationBinding<FluentFiregridError>
    readonly externalSignals?: ExternalSignalBinding<FluentFiregridError>
    readonly key?: string
    readonly state?: ObjectStateBackend
  } = {}
): FluentDurableContextService => {
  let nextSignalOperation = 0
  let nextStateOperation = 0
  const runId = ctx.runId ?? "unknown-run"
  const now = ctx.now
  return {
    ...(options.binding === undefined ? {} : { binding: options.binding }),
    ...(options.externalSignals === undefined ? {} : { externalSignals: options.externalSignals }),
    ...(options.key === undefined ? {} : { key: options.key }),
    runId,
    ...(options.state === undefined ? {} : { state: options.state }),
    signalOperationId: (input) => `${runId}:signal:${nextSignalOperation++}:${input.kind}:${input.name}`,
    stateOperationId: (input) => `${runId}:state:${nextStateOperation++}:${input.kind}:${input.table}:${input.key}`,
    ...(now === undefined
      ? {}
      : {
        now: (options) =>
          Effect.tryPromise({
            try: () => now(options),
            catch: (cause) => new FluentFiregridError({ cause, message: "now failed" })
          })
      }),
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
    waitForSignal: (name, options) =>
      Effect.tryPromise({
        try: () => ctx.waitForEvent(name, options),
        catch: (cause) => new FluentFiregridError({ cause, message: `waitForSignal(${name}) failed` })
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
  }
}
