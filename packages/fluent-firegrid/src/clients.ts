import * as Effect from "effect/Effect"

import { FluentDurableContext } from "./context.ts"
import type {
  AnyGeneratorHandler,
  Definition,
  DefinitionKind,
  HandlerDescriptor,
  HandlerDescriptors,
  HandlerInput,
  HandlerOutput
} from "./definitions.ts"
import { FluentFiregridError } from "./error.ts"

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

type ClientResult<
  Mode extends ClientMode,
  Handler extends AnyGeneratorHandler,
  Error,
  Requirements,
  HandleRequirements
> = Mode extends "call" ? Effect.Effect<HandlerOutput<Handler>, Error, Requirements>
  : Effect.Effect<InvocationHandle<HandlerOutput<Handler>, Error, HandleRequirements>, Error, Requirements>

type ClientShape<
  Mode extends ClientMode,
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never,
  HandleRequirements = Requirements
> = {
  readonly [Key in keyof Handlers]: (
    input: HandlerInput<Handlers[Key]>,
    options?: Mode extends "send" ? SendOptions : CallOptions
  ) => ClientResult<Mode, Handlers[Key], Error, Requirements, HandleRequirements>
}

export type Client<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = ClientShape<"call", Handlers, Error, Requirements>

export type SendClient<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = ClientShape<"send", Handlers, Error, Requirements>

export type ObjectClient<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = (key: string) => Client<Handlers, Error, Requirements>

export type SendObjectClient<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = (key: string) => SendClient<Handlers, Error, Requirements>

type ClientMode = "call" | "send"

type ClientFor<
  Mode extends ClientMode,
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error,
  Requirements,
  HandleRequirements = Requirements
> = Mode extends "call" ? Client<Handlers, Error, Requirements>
  : ClientShape<"send", Handlers, Error, Requirements, HandleRequirements>

type UntypedDefinition = BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>
type UntypedObjectDefinition = BindableDefinition<string, "object", Record<string, AnyGeneratorHandler>>

type ObjectClientFor<
  Mode extends ClientMode,
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error,
  Requirements,
  HandleRequirements = Requirements
> = Mode extends "call" ? ObjectClient<Handlers, Error, Requirements>
  : (key: string) => ClientShape<"send", Handlers, Error, Requirements, HandleRequirements>

type BindableDefinition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, AnyGeneratorHandler>
> = Definition<Name, Kind, Handlers> | {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: HandlerDescriptors<Handlers>
}

interface BindingClientFactory<Mode extends ClientMode> {
  <
    const Name extends string,
    const Kind extends DefinitionKind,
    const Handlers extends Record<string, AnyGeneratorHandler>,
    Error = unknown,
    Requirements = never
  >(
    binding: InvocationBinding<Error, Requirements>,
    definition: BindableDefinition<Name, Kind, Handlers>,
    key?: string
  ): ClientFor<Mode, Handlers, Error, Requirements>
}

interface ContextualClientFactory<Mode extends ClientMode> extends BindingClientFactory<Mode> {
  <
    const Name extends string,
    const Kind extends DefinitionKind,
    const Handlers extends Record<string, AnyGeneratorHandler>
  >(
    definition: BindableDefinition<Name, Kind, Handlers>
  ): ClientFor<Mode, Handlers, FluentFiregridError, FluentDurableContext, never>
}

interface ObjectContextualClientFactory<Mode extends ClientMode> {
  <
    const Name extends string,
    const Handlers extends Record<string, AnyGeneratorHandler>,
    Error = unknown,
    Requirements = never
  >(
    binding: InvocationBinding<Error, Requirements>,
    definition: BindableDefinition<Name, "object", Handlers>
  ): ObjectClientFor<Mode, Handlers, Error, Requirements>

  <
    const Name extends string,
    const Handlers extends Record<string, AnyGeneratorHandler>,
    Error = unknown,
    Requirements = never
  >(
    binding: InvocationBinding<Error, Requirements>,
    definition: BindableDefinition<Name, "object", Handlers>,
    key: string
  ): ClientFor<Mode, Handlers, Error, Requirements>

  <
    const Name extends string,
    const Handlers extends Record<string, AnyGeneratorHandler>
  >(
    definition: BindableDefinition<Name, "object", Handlers>
  ): ObjectClientFor<Mode, Handlers, FluentFiregridError, FluentDurableContext, never>

  <
    const Name extends string,
    const Handlers extends Record<string, AnyGeneratorHandler>
  >(
    definition: BindableDefinition<Name, "object", Handlers>,
    key: string
  ): ClientFor<Mode, Handlers, FluentFiregridError, FluentDurableContext, never>
}

const methodNames = (descriptors: Record<string, HandlerDescriptor>): ReadonlyArray<string> => Object.keys(descriptors)

const requestFor = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, Kind, Handlers>,
  key: string | undefined,
  handler: string,
  input: unknown,
  options: InvocationOptions | undefined
): CallRequest => {
  const runId = options?.runId ?? options?.idempotencyKey
  return {
    handler,
    input,
    kind: definition._kind,
    name: definition.name,
    ...(options?.delay === undefined ? {} : { delayMs: normalizeDuration(options.delay) }),
    ...(options?.idempotencyKey === undefined ? {} : { idempotencyKey: options.idempotencyKey }),
    ...(options?.metadata === undefined ? {} : { metadata: options.metadata }),
    ...(definition._handlers[handler] === undefined ? {} : { descriptor: definition._handlers[handler] }),
    ...(key === undefined ? {} : { key }),
    ...(runId === undefined ? {} : { runId })
  }
}

export const duration = (input: DurationLike): number => {
  if (typeof input === "number") return input
  return (input.milliseconds ?? 0)
    + (input.seconds ?? 0) * 1_000
    + (input.minutes ?? 0) * 60_000
    + (input.hours ?? 0) * 3_600_000
    + (input.days ?? 0) * 86_400_000
}

const normalizeDuration = (input: DurationLike): number => {
  const ms = duration(input)
  if (!Number.isFinite(ms) || ms < 0) {
    throw new Error("fluent invocation delay must be a non-negative finite duration")
  }
  return ms
}

export const rpc = {
  callOpts: (options: CallOptions): CallOptions => options,
  duration,
  opts: (options: InvocationOptions): InvocationOptions => options,
  sendOpts: (options: SendOptions): SendOptions => options
} as const

const invocationHandle = <Output, Error, Requirements>(
  binding: InvocationBinding<Error, Requirements>,
  request: CallRequest,
  reference: SendReference<Output>
): InvocationHandle<Output, Error, Requirements> => {
  const attach = () =>
    binding.call<Output>({
      ...request,
      runId: reference.invocationId
    })
  const handle = { ...reference } as unknown as InvocationHandle<Output, Error, Requirements>
  Object.defineProperties(handle, {
    attach: {
      enumerable: false,
      value: attach
    },
    outputEffect: {
      enumerable: false,
      value: attach
    }
  })
  return handle
}

const invoke = <Output, Error, Requirements>(
  binding: InvocationBinding<Error, Requirements>,
  mode: ClientMode,
  request: CallRequest
): Effect.Effect<Output | InvocationHandle<Output, Error, Requirements>, Error, Requirements> =>
  mode === "call"
    ? binding.call<Output>(request)
    : binding.send<Output>(request).pipe(
      Effect.map((reference) => invocationHandle(binding, request, reference))
    )

export interface GenericInvocationRequest<Input = unknown> extends InvocationOptions {
  readonly handler: string
  readonly input: Input
  readonly key?: string
  readonly kind: DefinitionKind
  readonly name: string
}

const genericRequest = (request: GenericInvocationRequest): CallRequest => ({
  handler: request.handler,
  input: request.input,
  kind: request.kind,
  name: request.name,
  ...(request.delay === undefined ? {} : { delayMs: normalizeDuration(request.delay) }),
  ...(request.idempotencyKey === undefined ? {} : { idempotencyKey: request.idempotencyKey }),
  ...(request.key === undefined ? {} : { key: request.key }),
  ...(request.metadata === undefined ? {} : { metadata: request.metadata }),
  ...((request.runId ?? request.idempotencyKey) === undefined ? {} : { runId: request.runId ?? request.idempotencyKey })
})

type AttachableReference<Output> =
  & SendReference<Output>
  & Pick<CallRequest, "handler" | "kind" | "name">
  & Partial<Pick<CallRequest, "input">>

export const invocation = <Output, Error, Requirements>(
  binding: InvocationBinding<Error, Requirements>,
  reference: AttachableReference<Output>
): InvocationHandle<Output, Error, Requirements> =>
  invocationHandle(binding, {
    handler: reference.handler,
    input: reference.input,
    kind: reference.kind,
    name: reference.name,
    ...(reference.key === undefined ? {} : { key: reference.key })
  }, reference)

export function genericCall<Output>(
  binding: InvocationBinding<FluentFiregridError>,
  request: GenericInvocationRequest
): Effect.Effect<Output, FluentFiregridError>
export function genericCall<Output>(
  request: GenericInvocationRequest
): Effect.Effect<Output, FluentFiregridError, FluentDurableContext>
export function genericCall<Output>(
  first: InvocationBinding<FluentFiregridError> | GenericInvocationRequest,
  second?: GenericInvocationRequest
) {
  if (second !== undefined) {
    const binding = first as InvocationBinding<FluentFiregridError>
    return genericCallWithBinding<Output>(binding, second)
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.binding === undefined
        ? Effect.fail(new FluentFiregridError({ message: "genericCall requires an invocation binding" }))
        : ctx.binding.call<Output>(genericRequest(first as GenericInvocationRequest))
    )
  )
}

const genericCallWithBinding = <Output>(
  binding: InvocationBinding<FluentFiregridError>,
  request: GenericInvocationRequest
): Effect.Effect<Output, FluentFiregridError> => binding.call<Output>(genericRequest(request))

export function genericSend<Output>(
  binding: InvocationBinding<FluentFiregridError>,
  request: GenericInvocationRequest
): Effect.Effect<InvocationHandle<Output, FluentFiregridError, never>, FluentFiregridError>
export function genericSend<Output>(
  request: GenericInvocationRequest
): Effect.Effect<InvocationHandle<Output, FluentFiregridError, never>, FluentFiregridError, FluentDurableContext>
export function genericSend<Output>(
  first: InvocationBinding<FluentFiregridError> | GenericInvocationRequest,
  second?: GenericInvocationRequest
) {
  if (second !== undefined) {
    const binding = first as InvocationBinding<FluentFiregridError>
    return genericSendWithBinding<Output>(binding, second)
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) => {
      const binding = ctx.binding
      if (binding === undefined) {
        return Effect.fail(new FluentFiregridError({ message: "genericSend requires an invocation binding" }))
      }
      const request = genericRequest(first as GenericInvocationRequest)
      return binding.send<Output>(request).pipe(
        Effect.map((reference) => invocationHandle(binding, request, reference))
      )
    })
  )
}

const genericSendWithBinding = <Output>(
  binding: InvocationBinding<FluentFiregridError>,
  request: GenericInvocationRequest
): Effect.Effect<InvocationHandle<Output, FluentFiregridError, never>, FluentFiregridError> => {
  const callRequest = genericRequest(request)
  return binding.send<Output>(callRequest).pipe(
    Effect.map((reference) => invocationHandle(binding, callRequest, reference))
  )
}

export function attach<Output>(
  binding: InvocationBinding<FluentFiregridError>,
  reference: AttachableReference<Output>
): Effect.Effect<Output, FluentFiregridError>
export function attach<Output>(
  reference: AttachableReference<Output>
): Effect.Effect<Output, FluentFiregridError, FluentDurableContext>
export function attach<Output>(
  first: InvocationBinding<FluentFiregridError> | AttachableReference<Output>,
  second?: AttachableReference<Output>
) {
  if (second !== undefined) {
    const binding = first as InvocationBinding<FluentFiregridError>
    return attachWithBinding<Output>(binding, second)
  }
  return FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.binding === undefined
        ? Effect.fail(new FluentFiregridError({ message: "attach requires an invocation binding" }))
        : invocation(ctx.binding, first as AttachableReference<Output>).attach()
    )
  )
}

const attachWithBinding = <Output>(
  binding: InvocationBinding<FluentFiregridError>,
  reference: AttachableReference<Output>
): Effect.Effect<Output, FluentFiregridError> => invocation(binding, reference).attach()

const bindInvocationBinding = (mode: ClientMode) =>
<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): ClientFor<typeof mode, Handlers, Error, Requirements> =>
  Object.fromEntries(
    methodNames(definition._handlers).map((handler) => [
      handler,
      (input: unknown, options?: InvocationOptions) =>
        invoke(binding, mode, requestFor(definition, key, handler, input, options))
    ])
  ) as ClientFor<typeof mode, Handlers, Error, Requirements>

const bindAmbientContext = (mode: ClientMode) =>
<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): ClientFor<typeof mode, Handlers, FluentFiregridError, FluentDurableContext, never> =>
  Object.fromEntries(
    methodNames(definition._handlers).map((handler) => [
      handler,
      (input: unknown, options?: InvocationOptions) =>
        FluentDurableContext.pipe(
          Effect.flatMap((ctx) => {
            const binding = ctx.binding
            return binding === undefined
              ? Effect.fail(
                new FluentFiregridError({
                  message: `fluent ambient client ${definition.name}.${handler} requires an invocation binding`
                })
              )
              : invoke(binding, mode, requestFor(definition, key, handler, input, options))
          })
        )
    ])
  ) as ClientFor<typeof mode, Handlers, FluentFiregridError, FluentDurableContext, never>

const contextualClient = (
  mode: ClientMode,
  first: unknown,
  second: unknown | undefined,
  key: string | undefined
) =>
  second === undefined
    ? bindAmbientContext(mode)(first as UntypedDefinition, key)
    : bindInvocationBinding(mode)(first as InvocationBinding, second as UntypedDefinition, key)

const keyedContextualClient = (
  mode: ClientMode,
  first: unknown,
  second: unknown | undefined,
  key: string | undefined
) =>
  key !== undefined
    ? bindInvocationBinding(mode)(first as InvocationBinding, second as UntypedObjectDefinition, key)
    : typeof second === "string"
    ? bindAmbientContext(mode)(first as UntypedObjectDefinition, second)
    : second === undefined
    ? (key: string) => bindAmbientContext(mode)(first as UntypedObjectDefinition, key)
    : (key: string) => bindInvocationBinding(mode)(first as InvocationBinding, second as UntypedObjectDefinition, key)

export const client = bindInvocationBinding("call") as BindingClientFactory<"call">

export const sendClient = bindInvocationBinding("send") as BindingClientFactory<"send">

export const serviceClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("call", first, second, key)) as ContextualClientFactory<"call">

export const workflowClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("call", first, second, key)) as ContextualClientFactory<"call">

export const sendServiceClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("send", first, second, key)) as ContextualClientFactory<"send">

export const serviceSendClient = sendServiceClient

export const sendWorkflowClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("send", first, second, key)) as ContextualClientFactory<"send">

export const workflowSendClient = sendWorkflowClient

export const objectClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => keyedContextualClient("call", first, second, key)) as ObjectContextualClientFactory<"call">

export const sendObjectClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => keyedContextualClient("send", first, second, key)) as ObjectContextualClientFactory<"send">

export const objectSendClient = sendObjectClient
