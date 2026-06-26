import * as Clients from "@firegrid/clients"
import type {
  AnyGeneratorHandler,
  AttachableReference,
  BindableDefinition,
  BindingClientFactory,
  ClientFor,
  ClientMode,
  DefinitionKind,
  GenericInvocationRequest,
  InvocationBinding,
  InvocationHandle,
  InvocationOptions,
  ObjectBindingClientFactory,
  ObjectClientFor
} from "@firegrid/clients"
import * as Effect from "effect/Effect"

import { FluentDurableContext } from "./context.ts"
import { FluentFiregridError } from "./error.ts"

export type {
  CallOptions,
  CallRequest,
  Client,
  DurationLike,
  GenericInvocationRequest,
  InvocationBinding,
  InvocationHandle,
  InvocationOptions,
  ObjectClient,
  SendClient,
  SendObjectClient,
  SendOptions,
  SendReference,
  SendRequest
} from "@firegrid/clients"

export const duration = Clients.duration
export const invocation = Clients.invocation
export const rpc = Clients.rpc

interface ContextualClientFactory<Mode extends ClientMode> extends BindingClientFactory<Mode> {
  <
    const Name extends string,
    const Kind extends DefinitionKind,
    const Handlers extends Record<string, AnyGeneratorHandler>
  >(
    definition: BindableDefinition<Name, Kind, Handlers>
  ): ClientFor<Mode, Handlers, FluentFiregridError, FluentDurableContext, never>
}

interface ObjectContextualClientFactory<Mode extends ClientMode> extends ObjectBindingClientFactory<Mode> {
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

const missingBinding = (message: string) => new FluentFiregridError({ message })

const ambientBinding = <A>(
  name: string,
  run: (binding: InvocationBinding<FluentFiregridError>) => Effect.Effect<A, FluentFiregridError>
): Effect.Effect<A, FluentFiregridError, FluentDurableContext> =>
  FluentDurableContext.pipe(
    Effect.flatMap((ctx) =>
      ctx.binding === undefined
        ? Effect.fail(missingBinding(`${name} requires an invocation binding`))
        : run(ctx.binding)
    )
  )

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
    return Clients.genericCall<Output, FluentFiregridError, never>(
      first as InvocationBinding<FluentFiregridError>,
      second
    )
  }
  const request = first as GenericInvocationRequest
  return ambientBinding(
    "genericCall",
    (binding) => Clients.genericCall<Output, FluentFiregridError, never>(binding, request)
  )
}

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
    return Clients.genericSend<Output, FluentFiregridError, never>(
      first as InvocationBinding<FluentFiregridError>,
      second
    )
  }
  const request = first as GenericInvocationRequest
  return ambientBinding(
    "genericSend",
    (binding) => Clients.genericSend<Output, FluentFiregridError, never>(binding, request)
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
    return Clients.attach<Output, FluentFiregridError, never>(
      first as InvocationBinding<FluentFiregridError>,
      second
    )
  }
  const reference = first as AttachableReference<Output>
  return ambientBinding("attach", (binding) => Clients.attach<Output, FluentFiregridError, never>(binding, reference))
}

const ambientClient = (
  mode: ClientMode,
  definition: BindableDefinition<string, any, Record<string, AnyGeneratorHandler>>,
  key?: string
) =>
  Object.fromEntries(
    Object.keys(definition._handlers).map((handler) => [
      handler,
      (input: unknown, options?: InvocationOptions) =>
        ambientBinding(`fluent ambient client ${definition.name}.${handler}`, (binding) =>
          mode === "call"
            ? Clients.client(binding, definition, key)[handler]!(input, options)
            : Clients.sendClient(binding, definition, key)[handler]!(input, options))
    ])
  )

const contextualClient = (
  mode: ClientMode,
  first: unknown,
  second: unknown | undefined,
  key: string | undefined
) =>
  second === undefined
    ? ambientClient(mode, first as BindableDefinition<string, any, any>, key)
    : mode === "call"
    ? Clients.client(first as InvocationBinding, second as BindableDefinition<string, any, any>, key)
    : Clients.sendClient(first as InvocationBinding, second as BindableDefinition<string, any, any>, key)

const keyedContextualClient = (
  mode: ClientMode,
  first: unknown,
  second: unknown | undefined,
  key: string | undefined
) =>
  key !== undefined
    ? mode === "call"
      ? Clients.objectClient(first as InvocationBinding, second as BindableDefinition<string, "object", any>, key)
      : Clients.sendObjectClient(first as InvocationBinding, second as BindableDefinition<string, "object", any>, key)
    : typeof second === "string"
    ? ambientClient(mode, first as BindableDefinition<string, "object", any>, second)
    : second === undefined
    ? (key: string) => ambientClient(mode, first as BindableDefinition<string, "object", any>, key)
    : mode === "call"
    ? Clients.objectClient(first as InvocationBinding, second as BindableDefinition<string, "object", any>)
    : Clients.sendObjectClient(first as InvocationBinding, second as BindableDefinition<string, "object", any>)

export const client = Clients.client

export const sendClient = Clients.sendClient

export const serviceClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("call", first, second, key)) as ContextualClientFactory<"call">

export const workflowClient = serviceClient

export const sendServiceClient = ((
  first: unknown,
  second?: unknown,
  key?: string
) => contextualClient("send", first, second, key)) as ContextualClientFactory<"send">

export const serviceSendClient = sendServiceClient

export const sendWorkflowClient = sendServiceClient

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
