import * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"

import { FluentFiregridError } from "./error.ts"

export type DefinitionKind = "service" | "workflow" | "object"

declare const descriptorTypes: unique symbol

export interface HandlerDescriptor<Input = unknown, Output = unknown> {
  readonly _tag: "HandlerDescriptor"
  readonly input?: Schema.Schema<unknown>
  readonly output?: Schema.Schema<unknown>
  readonly [descriptorTypes]?: {
    readonly input: Input
    readonly output: Output
  }
}

export type AnyGeneratorHandler = (input: any) => Generator<any, any, any>

export type HandlerInput<Handler> = Handler extends (input: infer Input) => unknown ? Input : never

export type HandlerOutput<Handler> = Handler extends (input: any) => Generator<unknown, infer Output, unknown> ? Output
  : never

export type HandlerDescriptors<Handlers extends Record<string, AnyGeneratorHandler>> = {
  readonly [Key in keyof Handlers]: HandlerDescriptor<HandlerInput<Handlers[Key]>, HandlerOutput<Handlers[Key]>>
}

export interface CallRequest<Input = unknown> {
  readonly kind: DefinitionKind
  readonly name: string
  readonly handler: string
  readonly key?: string
  readonly input: Input
  readonly runId?: string
  readonly idempotencyKey?: string
  readonly delayMs?: number
  readonly metadata?: Readonly<Record<string, unknown>>
  readonly descriptor?: HandlerDescriptor
}

export type SendRequest<Input = unknown> = CallRequest<Input>

export type DurationLike = number | {
  readonly days?: number
  readonly hours?: number
  readonly milliseconds?: number
  readonly minutes?: number
  readonly seconds?: number
}

export interface InvocationOptions {
  readonly delay?: DurationLike
  readonly idempotencyKey?: string
  readonly metadata?: Readonly<Record<string, unknown>>
  readonly runId?: string
}

export type CallOptions = InvocationOptions
export type SendOptions = InvocationOptions

export interface SendReference<Output = unknown> {
  readonly handler?: string
  readonly invocationId: string
  readonly key?: string
  readonly kind?: DefinitionKind
  readonly name?: string
  readonly output?: Output
}

export interface InvocationHandle<
  Output = unknown,
  Error = unknown,
  Requirements = never
> extends SendReference<Output> {
  readonly attach: () => Effect.Effect<Output, Error, Requirements>
  readonly outputEffect: () => Effect.Effect<Output, Error, Requirements>
}

export interface InvocationBinding<Error = unknown, Requirements = never> {
  readonly call: <Output>(request: CallRequest) => Effect.Effect<Output, Error, Requirements>
  readonly send: <Output>(request: SendRequest) => Effect.Effect<SendReference<Output>, Error, Requirements>
}

export interface GenericInvocationRequest<Input = unknown> extends InvocationOptions {
  readonly handler: string
  readonly input: Input
  readonly key?: string
  readonly kind: DefinitionKind
  readonly name: string
}

export type AttachableReference<Output = unknown> =
  & SendReference<Output>
  & Pick<CallRequest, "handler" | "kind" | "name">
  & Partial<Pick<CallRequest, "input">>

export interface RuntimeInvocationRunResult {
  readonly kind: string
  readonly runId: string
  readonly workflowId?: string
  readonly run?: {
    readonly error?: unknown
    readonly output?: unknown
  }
}

export interface RuntimeInvocationHost {
  readonly runtime: {
    readonly deliverSignal?: (args: {
      readonly runId: string
      readonly signalId: string
      readonly stepId?: string
      readonly name: string
      readonly payload: unknown
      readonly leaseMs?: number
      readonly leaseOwner?: string
      readonly now?: number
    }) => Promise<RuntimeInvocationRunResult>
    readonly startRun: (args: {
      readonly workflowId: string
      readonly runId: string
      readonly input: unknown
      readonly leaseMs?: number
      readonly leaseOwner?: string
      readonly now?: number
    }) => Promise<RuntimeInvocationRunResult>
  }
}

export type FluentRuntimeHost = RuntimeInvocationHost

export interface FluentWorkflowInput {
  readonly input: unknown
  readonly key?: string
  readonly stateContext?: unknown
}

export const duration = (input: DurationLike): number => {
  if (typeof input === "number") return input
  return (input.milliseconds ?? 0)
    + (input.seconds ?? 0) * 1_000
    + (input.minutes ?? 0) * 60_000
    + (input.hours ?? 0) * 3_600_000
    + (input.days ?? 0) * 86_400_000
}

export const normalizeDuration = (input: DurationLike): number => {
  const ms = duration(input)
  if (!Number.isFinite(ms) || ms < 0) {
    throw new Error("fluent invocation delay must be a non-negative finite duration")
  }
  return ms
}

export const workflowIdForRequest = (request: Pick<CallRequest, "handler" | "kind" | "name">): string =>
  `${request.kind}:${request.name}:${request.handler}`

export const createTanStackRuntimeBinding = (
  host: RuntimeInvocationHost,
  options: { readonly now?: () => number } = {}
): InvocationBinding<FluentFiregridError> => {
  let nextRun = 0
  const now = options.now ?? Date.now
  const runIdFor = (request: CallRequest): string =>
    request.runId ?? `${request.kind}:${request.name}:${request.handler}:${nextRun++}`

  const start = (request: CallRequest): Effect.Effect<RuntimeInvocationRunResult, FluentFiregridError> =>
    request.delayMs !== undefined && request.delayMs > 0
      ? Effect.fail(
        new FluentFiregridError({
          message: "delayed fluent invocations require a binding with durable delayed-send support"
        })
      )
      : Effect.tryPromise({
        try: () =>
          host.runtime.startRun({
            input: {
              input: request.input,
              ...(request.key === undefined ? {} : { key: request.key })
            } satisfies FluentWorkflowInput,
            now: now(),
            runId: runIdFor(request),
            workflowId: workflowIdForRequest(request)
          }),
        catch: (cause) => new FluentFiregridError({ cause, message: "fluent TanStack binding failed to start run" })
      })

  return {
    call: <Output>(request: CallRequest) =>
      start(request).pipe(
        Effect.flatMap((result) =>
          result.kind === "completed"
            ? Effect.succeed(result.run?.output as Output)
            : Effect.fail(
              new FluentFiregridError({
                message: `fluent call ${request.name}.${request.handler} did not complete synchronously: ${result.kind}`
              })
            )
        )
      ),
    send: <Output>(request: CallRequest) => {
      const invocationId = runIdFor(request)
      return start({ ...request, runId: invocationId }).pipe(
        Effect.map((result) => ({
          invocationId,
          ...(result.run?.output === undefined ? {} : { output: result.run.output as Output })
        }))
      )
    }
  }
}
