import {
  type AnyGeneratorHandler,
  type AttachableReference,
  type CallOptions,
  type CallRequest,
  type DefinitionKind,
  duration,
  type DurationLike,
  type GenericInvocationRequest,
  type HandlerDescriptor,
  type HandlerDescriptors,
  type HandlerInput,
  type HandlerOutput,
  type InvocationBinding,
  type InvocationHandle,
  type InvocationOptions,
  normalizeDuration,
  type SendOptions,
  type SendReference,
  type SendRequest
} from "@firegrid/core"
import * as Effect from "effect/Effect"

export type {
  AnyGeneratorHandler,
  AttachableReference,
  CallOptions,
  CallRequest,
  DefinitionKind,
  DurationLike,
  GenericInvocationRequest,
  InvocationBinding,
  InvocationHandle,
  InvocationOptions,
  SendOptions,
  SendReference,
  SendRequest
}

export { duration }

export type ClientMode = "call" | "send"

export type ClientResult<
  Mode extends ClientMode,
  Handler extends AnyGeneratorHandler,
  Error,
  Requirements,
  HandleRequirements
> = Mode extends "call" ? Effect.Effect<HandlerOutput<Handler>, Error, Requirements>
  : Effect.Effect<InvocationHandle<HandlerOutput<Handler>, Error, HandleRequirements>, Error, Requirements>

export type ClientShape<
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
  Requirements = never,
  HandleRequirements = Requirements
> = ClientShape<"send", Handlers, Error, Requirements, HandleRequirements>

export type ObjectClient<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = (key: string) => Client<Handlers, Error, Requirements>

export type SendObjectClient<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never,
  HandleRequirements = Requirements
> = (key: string) => SendClient<Handlers, Error, Requirements, HandleRequirements>

export type ClientFor<
  Mode extends ClientMode,
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error,
  Requirements,
  HandleRequirements = Requirements
> = Mode extends "call" ? Client<Handlers, Error, Requirements>
  : ClientShape<"send", Handlers, Error, Requirements, HandleRequirements>

export type ObjectClientFor<
  Mode extends ClientMode,
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error,
  Requirements,
  HandleRequirements = Requirements
> = Mode extends "call" ? ObjectClient<Handlers, Error, Requirements>
  : (key: string) => ClientShape<"send", Handlers, Error, Requirements, HandleRequirements>

export type BindableDefinition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, AnyGeneratorHandler>
> = {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: HandlerDescriptors<Handlers>
}

export interface BindingClientFactory<Mode extends ClientMode> {
  <
    const Name extends string,
    const Kind extends DefinitionKind,
    const Handlers extends Record<string, AnyGeneratorHandler>,
    Error = unknown,
    Requirements = never,
    HandleRequirements = Requirements
  >(
    binding: InvocationBinding<Error, Requirements>,
    definition: BindableDefinition<Name, Kind, Handlers>,
    key?: string
  ): ClientFor<Mode, Handlers, Error, Requirements, HandleRequirements>
}

export interface ObjectBindingClientFactory<Mode extends ClientMode> {
  <
    const Name extends string,
    const Handlers extends Record<string, AnyGeneratorHandler>,
    Error = unknown,
    Requirements = never,
    HandleRequirements = Requirements
  >(
    binding: InvocationBinding<Error, Requirements>,
    definition: BindableDefinition<Name, "object", Handlers>
  ): ObjectClientFor<Mode, Handlers, Error, Requirements, HandleRequirements>

  <
    const Name extends string,
    const Handlers extends Record<string, AnyGeneratorHandler>,
    Error = unknown,
    Requirements = never,
    HandleRequirements = Requirements
  >(
    binding: InvocationBinding<Error, Requirements>,
    definition: BindableDefinition<Name, "object", Handlers>,
    key: string
  ): ClientFor<Mode, Handlers, Error, Requirements, HandleRequirements>
}

const methodNames = (descriptors: Record<string, HandlerDescriptor>): ReadonlyArray<string> => Object.keys(descriptors)

export const requestFor = <
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

export const genericCall = <Output, Error, Requirements>(
  binding: InvocationBinding<Error, Requirements>,
  request: GenericInvocationRequest
): Effect.Effect<Output, Error, Requirements> => binding.call<Output>(genericRequest(request))

export const genericSend = <Output, Error, Requirements>(
  binding: InvocationBinding<Error, Requirements>,
  request: GenericInvocationRequest
): Effect.Effect<InvocationHandle<Output, Error, Requirements>, Error, Requirements> => {
  const callRequest = genericRequest(request)
  return binding.send<Output>(callRequest).pipe(
    Effect.map((reference) => invocationHandle(binding, callRequest, reference))
  )
}

export const attach = <Output, Error, Requirements>(
  binding: InvocationBinding<Error, Requirements>,
  reference: AttachableReference<Output>
): Effect.Effect<Output, Error, Requirements> => invocation(binding, reference).attach()

const bindInvocationBinding = (mode: ClientMode) =>
<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never,
  HandleRequirements = Requirements
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): ClientFor<typeof mode, Handlers, Error, Requirements, HandleRequirements> =>
  Object.fromEntries(
    methodNames(definition._handlers).map((handler) => [
      handler,
      (input: unknown, options?: InvocationOptions) =>
        invoke(binding, mode, requestFor(definition, key, handler, input, options))
    ])
  ) as ClientFor<typeof mode, Handlers, Error, Requirements, HandleRequirements>

const keyedClient = (
  mode: ClientMode,
  first: unknown,
  second: unknown,
  key: string | undefined
) =>
  key !== undefined
    ? bindInvocationBinding(mode)(first as InvocationBinding, second as BindableDefinition<string, "object", any>, key)
    : (key: string) =>
      bindInvocationBinding(mode)(first as InvocationBinding, second as BindableDefinition<string, "object", any>, key)

export const client = bindInvocationBinding("call") as BindingClientFactory<"call">

export const sendClient = bindInvocationBinding("send") as BindingClientFactory<"send">

export const serviceClient = client

export const workflowClient = client

export const sendServiceClient = sendClient

export const serviceSendClient = sendServiceClient

export const sendWorkflowClient = sendClient

export const workflowSendClient = sendWorkflowClient

export const objectClient = ((
  first: unknown,
  second: unknown,
  key?: string
) => keyedClient("call", first, second, key)) as ObjectBindingClientFactory<"call">

export const sendObjectClient = ((
  first: unknown,
  second: unknown,
  key?: string
) => keyedClient("send", first, second, key)) as ObjectBindingClientFactory<"send">

export const objectSendClient = sendObjectClient

export const rpc = {
  callOpts: (options: CallOptions): CallOptions => options,
  duration,
  opts: (options: InvocationOptions): InvocationOptions => options,
  sendOpts: (options: SendOptions): SendOptions => options
} as const
