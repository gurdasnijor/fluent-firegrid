import type { Effect } from "effect"

import type {
  Definition,
  DefinitionKind,
  GeneratorHandler,
  HandlerDescriptor,
  HandlerDescriptors,
  HandlerInput,
  HandlerOutput
} from "./definitions.ts"

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
  readonly invocationId: string
  readonly output?: Output
}

export interface InvocationBinding<Error = unknown, Requirements = never> {
  readonly call: <Output>(request: CallRequest) => Effect.Effect<Output, Error, Requirements>
  readonly send: <Output>(request: SendRequest) => Effect.Effect<SendReference<Output>, Error, Requirements>
}

export type Client<
  Handlers extends Record<string, GeneratorHandler>,
  Error = unknown,
  Requirements = never
> = {
  readonly [Key in keyof Handlers]: (
    input: HandlerInput<Handlers[Key]>,
    options?: { readonly runId?: string }
  ) => Effect.Effect<HandlerOutput<Handlers[Key]>, Error, Requirements>
}

export type SendClient<
  Handlers extends Record<string, GeneratorHandler>,
  Error = unknown,
  Requirements = never
> = {
  readonly [Key in keyof Handlers]: (
    input: HandlerInput<Handlers[Key]>,
    options?: { readonly runId?: string }
  ) => Effect.Effect<SendReference<HandlerOutput<Handlers[Key]>>, Error, Requirements>
}

type ClientMode = "call" | "send"

type ClientFor<
  Mode extends ClientMode,
  Handlers extends Record<string, GeneratorHandler>,
  Error,
  Requirements
> = Mode extends "call" ? Client<Handlers, Error, Requirements> : SendClient<Handlers, Error, Requirements>

type BindableDefinition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, GeneratorHandler>
> = Definition<Name, Kind, Handlers> | {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: HandlerDescriptors<Handlers>
}

const methodNames = (descriptors: Record<string, HandlerDescriptor>): ReadonlyArray<string> => Object.keys(descriptors)

const requestFor = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, GeneratorHandler>
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
  const Handlers extends Record<string, GeneratorHandler>,
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

export const client = bindInvocationBinding("call")

export const sendClient = bindInvocationBinding("send")
