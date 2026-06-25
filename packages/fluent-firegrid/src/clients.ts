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
  readonly descriptor?: HandlerDescriptor
}

export type SendRequest<Input = unknown> = CallRequest<Input>

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
    options?: { readonly runId?: string }
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
    const Handlers extends Record<string, AnyGeneratorHandler>
  >(
    definition: BindableDefinition<Name, "object", Handlers>
  ): ObjectClientFor<Mode, Handlers, FluentFiregridError, FluentDurableContext, never>
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
  options: { readonly runId?: string } | undefined
): CallRequest => ({
  handler,
  input,
  kind: definition._kind,
  name: definition.name,
  ...(definition._handlers[handler] === undefined ? {} : { descriptor: definition._handlers[handler] }),
  ...(key === undefined ? {} : { key }),
  ...(options?.runId === undefined ? {} : { runId: options.runId })
})

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
      (input: unknown, options?: { readonly runId?: string }) =>
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
      (input: unknown, options?: { readonly runId?: string }) =>
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
  second: unknown | undefined
) =>
  second === undefined
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

export const sendWorkflowClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("send", first, second, key)) as ContextualClientFactory<"send">

export const objectClient = ((
  first: unknown,
  second?: unknown
) => keyedContextualClient("call", first, second)) as ObjectContextualClientFactory<"call">

export const sendObjectClient = ((
  first: unknown,
  second?: unknown
) => keyedContextualClient("send", first, second)) as ObjectContextualClientFactory<"send">
