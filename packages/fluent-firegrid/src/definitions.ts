import type * as Effect from "effect/Effect"

import type { FluentDurableContext } from "./context.ts"

export type Operation<A, E = unknown, R = never> = Effect.Effect<A, E, R>

export type FluentGenerator<A> = Generator<Effect.Effect<unknown, unknown, FluentDurableContext>, A, unknown>

export type GeneratorHandler<Input = unknown, Output = unknown> = (input: Input) => FluentGenerator<Output>

export type AnyGeneratorHandler = GeneratorHandler<any, any>

export type DefinitionKind = "service" | "workflow" | "object"

declare const descriptorTypes: unique symbol

export interface HandlerDescriptor<Input = unknown, Output = unknown> {
  readonly _tag: "HandlerDescriptor"
  readonly [descriptorTypes]?: {
    readonly input: Input
    readonly output: Output
  }
}

export type HandlerInput<Handler> = Handler extends (input: infer Input) => unknown ? Input : never

export type HandlerOutput<Handler> = Handler extends (input: unknown) => Generator<unknown, infer Output, unknown>
  ? Output
  : never

export type HandlerDescriptors<Handlers extends Record<string, AnyGeneratorHandler>> = {
  readonly [Key in keyof Handlers]: HandlerDescriptor<HandlerInput<Handlers[Key]>, HandlerOutput<Handlers[Key]>>
}

export interface Definition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, AnyGeneratorHandler>
> {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: HandlerDescriptors<Handlers>
  readonly handlers: Handlers
}

export type ServiceDefinition<Name extends string, Handlers extends Record<string, AnyGeneratorHandler>> = Definition<
  Name,
  "service",
  Handlers
>

export type WorkflowDefinition<Name extends string, Handlers extends Record<string, AnyGeneratorHandler>> = Definition<
  Name,
  "workflow",
  Handlers
>

export type ObjectDefinition<Name extends string, Handlers extends Record<string, AnyGeneratorHandler>> = Definition<
  Name,
  "object",
  Handlers
>

interface DefinitionConfig<Name extends string, Handlers extends Record<string, AnyGeneratorHandler>> {
  readonly name: Name
  readonly handlers: Handlers
}

const descriptor = <Input = void, Output = void>(): HandlerDescriptor<Input, Output> => ({ _tag: "HandlerDescriptor" })

const makeDescriptors = <Handlers extends Record<string, AnyGeneratorHandler>>(
  handlers: Handlers
): HandlerDescriptors<Handlers> =>
  Object.fromEntries(
    Object.keys(handlers).map((key) => [key, descriptor()])
  ) as HandlerDescriptors<Handlers>

const makeDefinition = <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  kind: Kind,
  definition: DefinitionConfig<Name, Handlers>
): Definition<Name, Kind, Handlers> => ({
  _handlers: makeDescriptors(definition.handlers),
  _kind: kind,
  handlers: definition.handlers,
  name: definition.name
})

export const service = <const Name extends string, const Handlers extends Record<string, AnyGeneratorHandler>>(
  definition: DefinitionConfig<Name, Handlers>
): ServiceDefinition<Name, Handlers> => makeDefinition("service", definition)

export const workflow = <const Name extends string, const Handlers extends Record<string, AnyGeneratorHandler>>(
  definition: DefinitionConfig<Name, Handlers>
): WorkflowDefinition<Name, Handlers> => makeDefinition("workflow", definition)

export const object = <const Name extends string, const Handlers extends Record<string, AnyGeneratorHandler>>(
  definition: DefinitionConfig<Name, Handlers>
): ObjectDefinition<Name, Handlers> => makeDefinition("object", definition)
