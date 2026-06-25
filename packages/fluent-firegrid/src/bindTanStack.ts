/* oxlint-disable effect/restricted-syntax -- This module bridges fluent Effect handlers into TanStack's Promise-based workflow handler boundary. */
import { createWorkflow } from "@tanstack/workflow-core"
import type { WorkflowRegistrationMap, WorkflowRuntimeRunResult } from "@tanstack/workflow-runtime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"

import type { CallRequest, InvocationBinding, SendReference } from "./clients.ts"
import { fluentContextFromTanStack, FluentDurableContext, type ObjectStateBackend } from "./context.ts"
import type { AnyGeneratorHandler, Definition, DefinitionKind, HandlerDescriptor } from "./definitions.ts"
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
  readonly invocationBinding?: InvocationBinding<FluentFiregridError> | (() => InvocationBinding<FluentFiregridError> | undefined)
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
            load: async () => workflow
          }
        ] as const
      })
    )
  )

const invocationBindingFrom = (
  options: FluentDefinitionBindingOptions
): InvocationBinding<FluentFiregridError> | undefined =>
  typeof options.invocationBinding === "function"
    ? options.invocationBinding()
    : options.invocationBinding

const decodeHandlerInput = (
  definition: { readonly name: string },
  handlerName: string,
  descriptor: HandlerDescriptor | undefined,
  input: unknown
): Effect.Effect<unknown, FluentFiregridError> =>
  descriptor?.input === undefined
    ? Effect.succeed(input)
    : Schema.decodeUnknownEffect(descriptor.input)(input).pipe(
      Effect.mapError((cause) =>
        new FluentFiregridError({
          cause,
          message: `invalid input for fluent handler ${definition.name}.${handlerName}`
        })
      )
    ) as Effect.Effect<unknown, FluentFiregridError>

const decodeHandlerOutput = (
  definition: { readonly name: string },
  handlerName: string,
  descriptor: HandlerDescriptor | undefined,
  output: unknown
): Effect.Effect<unknown, FluentFiregridError> =>
  descriptor?.output === undefined
    ? Effect.succeed(output)
    : Schema.decodeUnknownEffect(descriptor.output)(output).pipe(
      Effect.mapError((cause) =>
        new FluentFiregridError({
          cause,
          message: `invalid output for fluent handler ${definition.name}.${handlerName}`
        })
      )
    ) as Effect.Effect<unknown, FluentFiregridError>

export interface FluentRuntimeHost {
  readonly runtime: {
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

export const createTanStackRuntimeBinding = (
  host: FluentRuntimeHost,
  options: { readonly now?: () => number } = {}
): InvocationBinding<FluentFiregridError> => {
  let nextRun = 0
  const now = options.now ?? Date.now
  const runIdFor = (request: CallRequest): string =>
    request.runId ?? `${request.kind}:${request.name}:${request.handler}:${nextRun++}`

  const start = (request: CallRequest): Effect.Effect<WorkflowRuntimeRunResult, FluentFiregridError> =>
    Effect.tryPromise({
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
      return Effect.tryPromise<SendReference<Output>, FluentFiregridError>({
        try: async () => {
          const result = await host.runtime.startRun({
            input: {
              input: request.input,
              ...(request.key === undefined ? {} : { key: request.key })
            } satisfies FluentWorkflowInput,
            now: now(),
            runId: invocationId,
            workflowId: workflowIdForRequest(request)
          })
          return {
            invocationId,
            ...(result.run?.output === undefined ? {} : { output: result.run.output as Output })
          }
        },
        catch: (cause) => new FluentFiregridError({ cause, message: "fluent TanStack binding failed to send run" })
      })
    }
  }
}
