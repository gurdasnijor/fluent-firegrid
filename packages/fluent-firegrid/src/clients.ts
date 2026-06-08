import type { Effect } from "effect"
import type {
  DefinitionKind,
  HandlerDescriptor,
} from "./definitions.ts"
import type { DefinitionDescriptor } from "./interface.ts"

export interface CallRequest<Input = unknown, Output = unknown> {
  readonly kind: DefinitionKind
  readonly name: string
  readonly handler: string
  readonly key?: string
  readonly input: Input
  readonly descriptor?: HandlerDescriptor<Input, Output>
}

export type SendRequest<Input = unknown, Output = unknown> = CallRequest<Input, Output>

export interface SendReference<Output = unknown> {
  readonly invocationId: string
  readonly output?: Output
}

export interface FluentIngress<Error = unknown, Requirements = never> {
  readonly call: <Input, Output>(
    request: CallRequest<Input, Output>,
  ) => Effect.Effect<Output, Error, Requirements>
  readonly send: <Input, Output>(
    request: SendRequest<Input, Output>,
  ) => Effect.Effect<SendReference<Output>, Error, Requirements>
}

type InferInput<Descriptor> = Descriptor extends HandlerDescriptor<infer Input, unknown> ? Input : never
type InferOutput<Descriptor> = Descriptor extends HandlerDescriptor<unknown, infer Output> ? Output : never

export type Client<Handlers extends Record<string, HandlerDescriptor>, Error = unknown, Requirements = never> = {
  readonly [Key in keyof Handlers]: [InferInput<Handlers[Key]>] extends [void] ? (
      input?: InferInput<Handlers[Key]>,
    ) => Effect.Effect<InferOutput<Handlers[Key]>, Error, Requirements>
    : (
      input: InferInput<Handlers[Key]>,
    ) => Effect.Effect<InferOutput<Handlers[Key]>, Error, Requirements>
}

export type SendClient<Handlers extends Record<string, HandlerDescriptor>, Error = unknown, Requirements = never> = {
  readonly [Key in keyof Handlers]: [InferInput<Handlers[Key]>] extends [void] ? (
      input?: InferInput<Handlers[Key]>,
    ) => Effect.Effect<SendReference<InferOutput<Handlers[Key]>>, Error, Requirements>
    : (
      input: InferInput<Handlers[Key]>,
    ) => Effect.Effect<SendReference<InferOutput<Handlers[Key]>>, Error, Requirements>
}

type BindableDefinition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, HandlerDescriptor>,
> =
  | DefinitionDescriptor<Name, Kind, Handlers>
  | {
    readonly name: Name
    readonly _kind: Kind
    readonly _handlers: Handlers
  }

type ClientMode = "call" | "send"

type ClientFor<
  Mode extends ClientMode,
  Handlers extends Record<string, HandlerDescriptor>,
  Error,
  Requirements,
> = Mode extends "call" ? Client<Handlers, Error, Requirements>
  : SendClient<Handlers, Error, Requirements>

type ClientBinder<Mode extends ClientMode> = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, HandlerDescriptor>,
  Error = unknown,
  Requirements = never,
>(
  ingress: FluentIngress<Error, Requirements>,
  definition: BindableDefinition<Name, Kind, Handlers>,
  key?: string,
) => ClientFor<Mode, Handlers, Error, Requirements>

const methodNames = (
  descriptors: Record<string, HandlerDescriptor>,
): ReadonlyArray<string> =>
  Object.keys(descriptors)

const requestFor = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, HandlerDescriptor>,
>(
  definition: BindableDefinition<Name, Kind, Handlers>,
  key: string | undefined,
  handler: string,
  input: unknown,
): CallRequest => ({
  kind: definition._kind,
  name: definition.name,
  handler,
  ...(key === undefined ? {} : { key }),
  input,
  ...(definition._handlers[handler] === undefined ? {} : { descriptor: definition._handlers[handler] }),
})

const bindClient = <
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, HandlerDescriptor>,
  Result,
>(
  definition: BindableDefinition<Name, Kind, Handlers>,
  key: string | undefined,
  dispatch: (request: CallRequest) => Result,
): Record<string, (input: unknown) => Result> =>
  Object.fromEntries(
    methodNames(definition._handlers).map((handler) => [
      handler,
      (input: unknown) => dispatch(requestFor(definition, key, handler, input)),
    ]),
  )

const bindIngress = (
  mode: ClientMode,
) => (
  ingress: FluentIngress,
  definition: BindableDefinition<string, DefinitionKind, Record<string, HandlerDescriptor>>,
  key?: string,
): Record<string, (input: unknown) => unknown> =>
  bindClient(
    definition,
    key,
    (request) => ingress[mode](request),
  )

export const client = bindIngress("call") as ClientBinder<"call">
export const sendClient = bindIngress("send") as ClientBinder<"send">
