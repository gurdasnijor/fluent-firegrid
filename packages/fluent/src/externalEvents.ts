import * as Data from "effect/Data"
import * as Effect from "effect/Effect"

import { type ExternalSignalBinding, type ExternalSignalDelivery, FluentDurableContext } from "./context.ts"
import { FluentFiregridError } from "./error.ts"

export interface Awakeable<T> {
  readonly await: Effect.Effect<T, AwakeableRejected | FluentFiregridError>
  readonly id: string
  readonly promise: Effect.Effect<T, AwakeableRejected | FluentFiregridError>
  readonly effect: Effect.Effect<T, AwakeableRejected | FluentFiregridError>
}

export interface AwakeableOptions {
  readonly id?: string
  readonly name?: string
}

export interface AwakeableResolveOptions {
  readonly metadata?: Readonly<Record<string, unknown>>
  readonly signalId?: string
}

export interface AwakeableRejectOptions extends AwakeableResolveOptions {}

export interface WorkflowEventReference {
  readonly name: string
  readonly runId: string
  readonly signalId?: string
  readonly stepId?: string
}

export interface WorkflowEvent<T> extends WorkflowEventReference {
  readonly await: Effect.Effect<T, FluentFiregridError>
  readonly promise: Effect.Effect<T, FluentFiregridError>
  readonly effect: Effect.Effect<T, FluentFiregridError>
}

export interface WorkflowEventOptions {
  readonly id?: string
}

export interface ResolveWorkflowEventOptions {
  readonly metadata?: Readonly<Record<string, unknown>>
  readonly signalId?: string
}

export class AwakeableRejected extends Data.TaggedError("AwakeableRejected")<{
  readonly id: string
  readonly reason: unknown
}> {}

interface AwakeableToken {
  readonly _tag: "FiregridAwakeable"
  readonly name: string
  readonly runId: string
  readonly signalId: string
  readonly stepId: string
}

type AwakeablePayload<T> =
  | {
    readonly _tag: "AwakeableResolved"
    readonly value: T
  }
  | {
    readonly _tag: "AwakeableRejected"
    readonly reason: unknown
  }

const tokenPrefix = "ffg_awakeable:"

const awakeableSignalName = (stepId: string): string => `__firegrid_awakeable:${stepId}`

const workflowEventSignalName = (name: string): string => `__firegrid_workflow_event:${name}`

const encodeToken = (token: AwakeableToken): string => `${tokenPrefix}${encodeURIComponent(JSON.stringify(token))}`

export const decodeAwakeableToken = (id: string): AwakeableToken => {
  if (!id.startsWith(tokenPrefix)) {
    throw new Error("invalid Firegrid awakeable token")
  }
  const parsed = JSON.parse(decodeURIComponent(id.slice(tokenPrefix.length))) as Partial<AwakeableToken>
  if (
    parsed._tag !== "FiregridAwakeable"
    || typeof parsed.name !== "string"
    || typeof parsed.runId !== "string"
    || typeof parsed.signalId !== "string"
    || typeof parsed.stepId !== "string"
  ) {
    throw new Error("invalid Firegrid awakeable token payload")
  }
  return {
    _tag: "FiregridAwakeable",
    name: parsed.name,
    runId: parsed.runId,
    signalId: parsed.signalId,
    stepId: parsed.stepId
  }
}

export const awakeable = <T = unknown>(
  options: AwakeableOptions = {}
): Effect.Effect<Awakeable<T>, FluentFiregridError, FluentDurableContext> =>
  FluentDurableContext.pipe(
    Effect.flatMap((ctx) => {
      if (ctx.runId === undefined) {
        return Effect.fail(new FluentFiregridError({ message: "awakeable requires a durable run id" }))
      }
      const name = options.name ?? "awakeable"
      const stepId = options.id ?? ctx.signalOperationId?.({ kind: "awakeable", name }) ??
        `${ctx.runId}:awakeable:${name}`
      const signalName = awakeableSignalName(stepId)
      const token = encodeToken({
        _tag: "FiregridAwakeable",
        name: signalName,
        runId: ctx.runId,
        signalId: `awakeable:${stepId}`,
        stepId
      })
      const effect = ctx.waitForSignal<AwakeablePayload<T>>(signalName, { id: stepId }).pipe(
        Effect.flatMap((payload) =>
          payload._tag === "AwakeableRejected"
            ? Effect.fail(new AwakeableRejected({ id: token, reason: payload.reason }))
            : Effect.succeed(payload.value)
        )
      )
      return Effect.succeed({ await: effect, effect, id: token, promise: effect })
    })
  )

export const workflowEvent = <T = unknown>(
  name: string,
  options: WorkflowEventOptions = {}
): Effect.Effect<WorkflowEvent<T>, FluentFiregridError, FluentDurableContext> =>
  FluentDurableContext.pipe(
    Effect.flatMap((ctx) => {
      if (ctx.runId === undefined) {
        return Effect.fail(new FluentFiregridError({ message: "workflowEvent requires a durable run id" }))
      }
      const stepId = options.id ?? ctx.signalOperationId?.({ kind: "workflowEvent", name }) ??
        `${ctx.runId}:workflowEvent:${name}`
      const signalName = workflowEventSignalName(name)
      const effect = ctx.waitForSignal<T>(signalName, { id: stepId })
      return Effect.succeed({
        await: effect,
        effect,
        name,
        promise: effect,
        runId: ctx.runId,
        signalId: `workflow-event:${ctx.runId}:${name}`,
        stepId
      })
    })
  )

export function resolveAwakeable<T>(
  binding: ExternalSignalBinding<FluentFiregridError>,
  id: string,
  value: T,
  options?: AwakeableResolveOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError>
export function resolveAwakeable<T>(
  id: string,
  value: T,
  options?: AwakeableResolveOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError, FluentDurableContext>
export function resolveAwakeable<T>(
  first: ExternalSignalBinding<FluentFiregridError> | string,
  second: string | T,
  third?: T | AwakeableResolveOptions,
  fourth?: AwakeableResolveOptions
) {
  if (typeof first !== "string") {
    return resolveAwakeableWithBinding(first, second as string, third as T, fourth)
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.externalSignals === undefined
        ? Effect.fail(new FluentFiregridError({ message: "resolveAwakeable requires an external signal binding" }))
        : deliverAwakeable(
          ctx.externalSignals,
          first,
          { _tag: "AwakeableResolved", value: second as T },
          third as AwakeableResolveOptions | undefined
        )
    )
  )
}

const resolveAwakeableWithBinding = <T>(
  binding: ExternalSignalBinding<FluentFiregridError>,
  id: string,
  value: T,
  options?: AwakeableResolveOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError> =>
  deliverAwakeable(binding, id, { _tag: "AwakeableResolved", value }, options)

export function rejectAwakeable(
  binding: ExternalSignalBinding<FluentFiregridError>,
  id: string,
  reason: unknown,
  options?: AwakeableRejectOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError>
export function rejectAwakeable(
  id: string,
  reason: unknown,
  options?: AwakeableRejectOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError, FluentDurableContext>
export function rejectAwakeable(
  first: ExternalSignalBinding<FluentFiregridError> | string,
  second: string | unknown,
  third?: unknown | AwakeableRejectOptions,
  fourth?: AwakeableRejectOptions
) {
  if (typeof first !== "string") {
    return rejectAwakeableWithBinding(first, second as string, third, fourth)
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.externalSignals === undefined
        ? Effect.fail(new FluentFiregridError({ message: "rejectAwakeable requires an external signal binding" }))
        : deliverAwakeable(
          ctx.externalSignals,
          first,
          { _tag: "AwakeableRejected", reason: second },
          third as AwakeableRejectOptions | undefined
        )
    )
  )
}

const rejectAwakeableWithBinding = (
  binding: ExternalSignalBinding<FluentFiregridError>,
  id: string,
  reason: unknown,
  options?: AwakeableRejectOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError> =>
  deliverAwakeable(binding, id, { _tag: "AwakeableRejected", reason }, options)

export function resolveWorkflowEvent<T>(
  binding: ExternalSignalBinding<FluentFiregridError>,
  reference: WorkflowEventReference,
  value: T,
  options?: ResolveWorkflowEventOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError>
export function resolveWorkflowEvent<T>(
  reference: WorkflowEventReference,
  value: T,
  options?: ResolveWorkflowEventOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError, FluentDurableContext>
export function resolveWorkflowEvent<T>(
  first: ExternalSignalBinding<FluentFiregridError> | WorkflowEventReference,
  second: WorkflowEventReference | T,
  third?: T | ResolveWorkflowEventOptions,
  fourth?: ResolveWorkflowEventOptions
) {
  if ("deliverSignal" in first) {
    return resolveWorkflowEventWithBinding(first, second as WorkflowEventReference, third as T, fourth)
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.externalSignals === undefined
        ? Effect.fail(new FluentFiregridError({ message: "resolveWorkflowEvent requires an external signal binding" }))
        : deliverWorkflowEvent(
          ctx.externalSignals,
          first,
          second as T,
          third as ResolveWorkflowEventOptions | undefined
        )
    )
  )
}

const resolveWorkflowEventWithBinding = <Payload>(
  binding: ExternalSignalBinding<FluentFiregridError>,
  reference: WorkflowEventReference,
  value: Payload,
  options?: ResolveWorkflowEventOptions
): Effect.Effect<ExternalSignalDelivery, FluentFiregridError> =>
  deliverWorkflowEvent(binding, reference, value, options)

const deliverWorkflowEvent = <Payload, Error, Requirements>(
  binding: ExternalSignalBinding<Error, Requirements>,
  reference: WorkflowEventReference,
  value: Payload,
  options: ResolveWorkflowEventOptions | undefined
): Effect.Effect<ExternalSignalDelivery, Error | FluentFiregridError, Requirements> =>
  binding.deliverSignal({
    name: workflowEventSignalName(reference.name),
    payload: value,
    runId: reference.runId,
    signalId: options?.signalId ?? reference.signalId ?? `workflow-event:${reference.runId}:${reference.name}`,
    ...(reference.stepId === undefined ? {} : { stepId: reference.stepId }),
    ...(options?.metadata === undefined ? {} : { metadata: options.metadata })
  })

const deliverAwakeable = <Payload, Error, Requirements>(
  binding: ExternalSignalBinding<Error, Requirements>,
  id: string,
  payload: AwakeablePayload<Payload>,
  options: AwakeableResolveOptions | undefined
): Effect.Effect<ExternalSignalDelivery, Error | FluentFiregridError, Requirements> =>
  Effect.try({
    try: () => decodeAwakeableToken(id),
    catch: (cause) => new FluentFiregridError({ cause, message: "invalid awakeable token" })
  }).pipe(
    Effect.flatMap((token) =>
      binding.deliverSignal({
        name: token.name,
        payload,
        runId: token.runId,
        signalId: options?.signalId ?? token.signalId,
        stepId: token.stepId,
        ...(options?.metadata === undefined ? {} : { metadata: options.metadata })
      })
    )
  )
