/* oxlint-disable effect/restricted-syntax -- This module bridges fluent Effect handlers into TanStack's Promise-based workflow handler boundary. */
import { createWorkflow } from "@firegrid/runtime"
import type { WorkflowRegistrationMap, WorkflowRuntimeRunResult, WorkflowScheduleDefinition } from "@firegrid/runtime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

import type { CallRequest, InvocationBinding } from "./clients.ts"
import {
  type ExternalSignalBinding,
  fluentContextFromTanStack,
  FluentDurableContext,
  type ObjectStateBackend
} from "./context.ts"
import type {
  AnyGeneratorHandler,
  Definition,
  DefinitionKind,
  FluentScheduleDefinition,
  HandlerDescriptor
} from "./definitions.ts"
import { FluentFiregridError } from "./error.ts"

export interface FluentWorkflowInput {
  readonly input: unknown
  readonly key?: string
  readonly stateContext?: unknown
}

export interface FluentDefinitionBindingContext {
  readonly definition: Definition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>
  readonly handlerName: string
  readonly input: FluentWorkflowInput
}

export interface FluentDefinitionBindingOptions {
  readonly externalSignals?:
    | ExternalSignalBinding<FluentFiregridError>
    | (() => ExternalSignalBinding<FluentFiregridError> | undefined)
  readonly invocationBinding?:
    | InvocationBinding<FluentFiregridError>
    | (() => InvocationBinding<FluentFiregridError> | undefined)
  readonly stateBackendFor?: (context: FluentDefinitionBindingContext) => ObjectStateBackend | undefined
}

export const workflowIdForHandler = (
  definition: { readonly _kind: DefinitionKind; readonly name: string },
  handler: string
): string => `${definition._kind}:${definition.name}:${handler}`

const workflowIdForRequest = (request: Pick<CallRequest, "handler" | "kind" | "name">): string =>
  `${request.kind}:${request.name}:${request.handler}`

export const bindFluentDefinitions = (
  definitions: ReadonlyArray<Definition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>>,
  options: FluentDefinitionBindingOptions = {}
): WorkflowRegistrationMap =>
  Object.fromEntries(
    definitions.flatMap((definition) =>
      Object.entries(definition.handlers).map(([handlerName, handler]) => {
        const workflowId = workflowIdForHandler(definition, handlerName)
        const workflow = createWorkflow({ id: workflowId }).handler(async (ctx) => {
          const input = ctx.input as FluentWorkflowInput
          const state = options.stateBackendFor?.({ definition, handlerName, input })
          const descriptor = definition._handlers[handlerName]
          const externalSignals = externalSignalsFrom(options)
          const binding = invocationBindingFrom(options)
          const effect = Effect.gen(function*() {
            const handlerInput = yield* decodeHandlerInput(definition, handlerName, descriptor, input.input)
            const output = yield* Effect.gen(() => handler(handlerInput))
            return yield* decodeHandlerOutput(definition, handlerName, descriptor, output)
          }).pipe(
            Effect.provideService(
              FluentDurableContext,
              fluentContextFromTanStack(ctx, {
                ...(binding === undefined ? {} : { binding }),
                ...(externalSignals === undefined ? {} : { externalSignals }),
                ...(input.key === undefined ? {} : { key: input.key }),
                ...(state === undefined ? {} : { state })
              })
            )
          )
          return await Effect.runPromise(effect)
        })
        return [
          workflowId,
          {
            load: async () => workflow,
            ...schedulesForHandler(definition, handlerName, workflowId)
          }
        ] as const
      })
    )
  )

const schedulesForHandler = (
  definition: Definition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>,
  handlerName: string,
  workflowId: string
): { readonly schedules?: ReadonlyArray<WorkflowScheduleDefinition> } => {
  if (definition._kind !== "workflow" || definition.schedules === undefined) return {}
  const schedules = definition.schedules
    .filter((entry) => entry.handler === handlerName)
    .map((entry) => scheduleForWorkflow(entry, workflowId))
  return schedules.length === 0 ? {} : { schedules }
}

const scheduleForWorkflow = (
  entry: FluentScheduleDefinition,
  workflowId: string
): WorkflowScheduleDefinition => ({
  ...(entry.enabled === undefined ? {} : { enabled: entry.enabled }),
  ...(entry.id === undefined ? {} : { id: `${workflowId}:${entry.id}` }),
  input: scheduledInput(entry.input),
  ...(entry.overlapPolicy === undefined ? {} : { overlapPolicy: entry.overlapPolicy }),
  schedule: entry.schedule
})

const scheduledInput = (
  input: FluentScheduleDefinition["input"]
): WorkflowScheduleDefinition["input"] =>
  typeof input === "function"
    ? async () => ({ input: await input() } satisfies FluentWorkflowInput)
    : ({ input } satisfies FluentWorkflowInput)

const invocationBindingFrom = (
  options: FluentDefinitionBindingOptions
): InvocationBinding<FluentFiregridError> | undefined =>
  typeof options.invocationBinding === "function"
    ? options.invocationBinding()
    : options.invocationBinding

const externalSignalsFrom = (
  options: FluentDefinitionBindingOptions
): ExternalSignalBinding<FluentFiregridError> | undefined =>
  typeof options.externalSignals === "function"
    ? options.externalSignals()
    : options.externalSignals

const decodeHandlerInput = (
  definition: { readonly name: string },
  handlerName: string,
  descriptor: HandlerDescriptor | undefined,
  input: unknown
): Effect.Effect<unknown, FluentFiregridError> =>
  descriptor?.input === undefined
    ? Effect.succeed(input)
    : Schema.decodeUnknownEffect(
      descriptor.input as unknown as Schema.ConstraintCodec<unknown, unknown, never, never>
    )(input)
      .pipe(
        Effect.mapError((cause) =>
          new FluentFiregridError({
            cause,
            message: `invalid input for fluent handler ${definition.name}.${handlerName}`
          })
        )
      )

const decodeHandlerOutput = (
  definition: { readonly name: string },
  handlerName: string,
  descriptor: HandlerDescriptor | undefined,
  output: unknown
): Effect.Effect<unknown, FluentFiregridError> =>
  descriptor?.output === undefined
    ? Effect.succeed(output)
    : Schema.decodeUnknownEffect(
      descriptor.output as unknown as Schema.ConstraintCodec<unknown, unknown, never, never>
    )(output)
      .pipe(
        Effect.mapError((cause) =>
          new FluentFiregridError({
            cause,
            message: `invalid output for fluent handler ${definition.name}.${handlerName}`
          })
        )
      )

export interface FluentRuntimeHost {
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
    }) => Promise<WorkflowRuntimeRunResult>
    readonly startRun: (args: {
      readonly workflowId: string
      readonly runId: string
      readonly input: unknown
      readonly leaseMs?: number
      readonly leaseOwner?: string
      readonly now?: number
    }) => Promise<WorkflowRuntimeRunResult>
  }
}

export const createTanStackExternalSignalBinding = (
  host: FluentRuntimeHost,
  options: { readonly now?: () => number } = {}
): ExternalSignalBinding<FluentFiregridError> => {
  const now = options.now ?? Date.now
  return {
    deliverSignal: (request) =>
      host.runtime.deliverSignal === undefined
        ? Effect.fail(new FluentFiregridError({ message: "external signal delivery requires runtime.deliverSignal" }))
        : Effect.tryPromise({
          try: () =>
            host.runtime.deliverSignal!({
              name: request.name,
              ...(request.metadata === undefined ? {} : { meta: request.metadata }),
              now: now(),
              payload: request.payload,
              runId: request.runId,
              signalId: request.signalId,
              ...(request.stepId === undefined ? {} : { stepId: request.stepId })
            }),
          catch: (cause) => new FluentFiregridError({ cause, message: "external signal delivery failed" })
        }).pipe(
          Effect.map((result) => ({
            kind: result.kind,
            runId: result.runId,
            ...(result.workflowId === undefined ? {} : { workflowId: result.workflowId })
          }))
        )
  }
}

export const createTanStackRuntimeBinding = (
  host: FluentRuntimeHost,
  options: { readonly now?: () => number } = {}
): InvocationBinding<FluentFiregridError> => {
  let nextRun = 0
  const now = options.now ?? Date.now
  const runIdFor = (request: CallRequest): string =>
    request.runId ?? `${request.kind}:${request.name}:${request.handler}:${nextRun++}`

  const start = (request: CallRequest): Effect.Effect<WorkflowRuntimeRunResult, FluentFiregridError> =>
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
