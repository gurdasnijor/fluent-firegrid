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

export interface InvocationBinding<Error = unknown, Requirements = never> {
  readonly call: <Output>(request: CallRequest) => Effect.Effect<Output, Error, Requirements>
  readonly send: <Output>(request: SendRequest) => Effect.Effect<SendReference<Output>, Error, Requirements>
}

export type Client<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = {
  readonly [Key in keyof Handlers]: (
    input: HandlerInput<Handlers[Key]>,
    options?: { readonly runId?: string }
  ) => Effect.Effect<HandlerOutput<Handlers[Key]>, Error, Requirements>
}

export type SendClient<
  Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
> = {
  readonly [Key in keyof Handlers]: (
    input: HandlerInput<Handlers[Key]>,
    options?: { readonly runId?: string }
  ) => Effect.Effect<SendReference<HandlerOutput<Handlers[Key]>>, Error, Requirements>
}

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
  Requirements
> = Mode extends "call" ? Client<Handlers, Error, Requirements> : SendClient<Handlers, Error, Requirements>

type BindableDefinition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, AnyGeneratorHandler>
> = Definition<Name, Kind, Handlers> | {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: HandlerDescriptors<Handlers>
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
        binding[mode](requestFor(definition, key, handler, input, options))
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
): ClientFor<typeof mode, Handlers, FluentFiregridError, FluentDurableContext> =>
  Object.fromEntries(
    methodNames(definition._handlers).map((handler) => [
      handler,
      (input: unknown, options?: { readonly runId?: string }) =>
        FluentDurableContext.pipe(
          Effect.flatMap((ctx) =>
            ctx.binding === undefined
              ? Effect.fail(
                new FluentFiregridError({
                  message: `fluent ambient client ${definition.name}.${handler} requires an invocation binding`
                })
              )
              : ctx.binding[mode](requestFor(definition, key, handler, input, options))
          )
        )
    ])
  ) as ClientFor<typeof mode, Handlers, FluentFiregridError, FluentDurableContext>

export const client = bindInvocationBinding("call") as <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
) => Client<Handlers, Error, Requirements>

export const sendClient = bindInvocationBinding("send") as <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
) => SendClient<Handlers, Error, Requirements>

export function serviceClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): Client<Handlers, Error, Requirements>
export function serviceClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, Kind, Handlers>
): Client<Handlers, FluentFiregridError, FluentDurableContext>
export function serviceClient(
  first: unknown,
  second?: unknown,
  key?: string
) {
  return second === undefined
    ? bindAmbientContext("call")(
      first as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>
    )
    : client(
      first as InvocationBinding,
      second as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>,
      key
    )
}

export function workflowClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): Client<Handlers, Error, Requirements>
export function workflowClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, Kind, Handlers>
): Client<Handlers, FluentFiregridError, FluentDurableContext>
export function workflowClient(
  first: unknown,
  second?: unknown,
  key?: string
) {
  return second === undefined
    ? serviceClient(first as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>)
    : serviceClient(
      first as InvocationBinding,
      second as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>,
      key
    )
}

export function sendServiceClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): SendClient<Handlers, Error, Requirements>
export function sendServiceClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, Kind, Handlers>
): SendClient<Handlers, FluentFiregridError, FluentDurableContext>
export function sendServiceClient(
  first: unknown,
  second?: unknown,
  key?: string
) {
  return second === undefined
    ? bindAmbientContext("send")(
      first as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>
    )
    : sendClient(
      first as InvocationBinding,
      second as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>,
      key
    )
}

export function sendWorkflowClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string
): SendClient<Handlers, Error, Requirements>
export function sendWorkflowClient<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, Kind, Handlers>
): SendClient<Handlers, FluentFiregridError, FluentDurableContext>
export function sendWorkflowClient(
  first: unknown,
  second?: unknown,
  key?: string
) {
  return second === undefined
    ? sendServiceClient(first as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>)
    : sendServiceClient(
      first as InvocationBinding,
      second as BindableDefinition<string, DefinitionKind, Record<string, AnyGeneratorHandler>>,
      key
    )
}

export function objectClient<
  const Name extends string,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, "object", Handlers>
): ObjectClient<Handlers, Error, Requirements>
export function objectClient<
  const Name extends string,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, "object", Handlers>
): ObjectClient<Handlers, FluentFiregridError, FluentDurableContext>
export function objectClient(
  first: unknown,
  second?: unknown
) {
  return second === undefined
    ? (key: string) =>
      bindAmbientContext("call")(
        first as BindableDefinition<string, "object", Record<string, AnyGeneratorHandler>>,
        key
      )
    : (key: string) =>
      client(
        first as InvocationBinding,
        second as BindableDefinition<string, "object", Record<string, AnyGeneratorHandler>>,
        key
      )
}

export function sendObjectClient<
  const Name extends string,
  const Handlers extends Record<string, AnyGeneratorHandler>,
  Error = unknown,
  Requirements = never
>(
  binding: InvocationBinding<Error, Requirements>,
  definition: BindableDefinition<Name, "object", Handlers>
): SendObjectClient<Handlers, Error, Requirements>
export function sendObjectClient<
  const Name extends string,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  definition: BindableDefinition<Name, "object", Handlers>
): SendObjectClient<Handlers, FluentFiregridError, FluentDurableContext>
export function sendObjectClient(
  first: unknown,
  second?: unknown
) {
  return second === undefined
    ? (key: string) =>
      bindAmbientContext("send")(
        first as BindableDefinition<string, "object", Record<string, AnyGeneratorHandler>>,
        key
      )
    : (key: string) =>
      sendClient(
        first as InvocationBinding,
        second as BindableDefinition<string, "object", Record<string, AnyGeneratorHandler>>,
        key
      )
}
