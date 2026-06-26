import type * as Effect from "effect/Effect"
import type * as Schema from "effect/Schema"
import type { DefinitionKind, HandlerDescriptor, HandlerDescriptors, HandlerInput } from "@firegrid/core"
import { cron, every } from "./runtime/define-runtime.ts"
import type { WorkflowOverlapPolicy, WorkflowScheduleDefinition, WorkflowScheduleSpec } from "./runtime/types.ts"

import type { FluentDurableContext } from "./context.ts"

export { cron, every }

export type Operation<A, E = unknown, R = never> = Effect.Effect<A, E, R>

export type FluentGenerator<A> = Generator<Effect.Effect<unknown, unknown, FluentDurableContext>, A, unknown>

export type GeneratorHandler<Input = unknown, Output = unknown> = (input: Input) => FluentGenerator<Output>

export type AnyGeneratorHandler = GeneratorHandler<any, any>
export type { DefinitionKind, HandlerDescriptor, HandlerDescriptors, HandlerInput, HandlerOutput } from "@firegrid/core"

export interface FluentScheduleDefinition<
  Handlers extends Record<string, AnyGeneratorHandler> = Record<string, AnyGeneratorHandler>,
  HandlerName extends keyof Handlers & string = keyof Handlers & string
> extends Omit<WorkflowScheduleDefinition, "input"> {
  readonly handler: HandlerName
  readonly input?:
    | HandlerInput<Handlers[HandlerName]>
    | (() => HandlerInput<Handlers[HandlerName]> | Promise<HandlerInput<Handlers[HandlerName]>>)
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
  readonly schedules?: ReadonlyArray<FluentScheduleDefinition<Handlers>>
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
  readonly descriptors?: Partial<Record<keyof Handlers, HandlerDescriptor>>
  readonly schedules?: ReadonlyArray<FluentScheduleDefinition<Handlers>>
}

export type HandlerDescriptorOptions<Input, Output> = {
  readonly input?: Schema.Schema<Input>
  readonly output?: Schema.Schema<Output>
}

const descriptor = <Input = void, Output = void>(
  options?: HandlerDescriptorOptions<Input, Output>
): HandlerDescriptor<Input, Output> => ({
  _tag: "HandlerDescriptor",
  ...(options?.input === undefined ? {} : { input: options.input }),
  ...(options?.output === undefined ? {} : { output: options.output })
})

export const json = <Input = void, Output = void>(): HandlerDescriptor<Input, Output> => descriptor<Input, Output>()

export const schemas = <Input = void, Output = void>(
  options: HandlerDescriptorOptions<Input, Output>
): HandlerDescriptor<Input, Output> => descriptor(options)

export const serdes: typeof schemas = (options) => descriptor(options)

export const schedule = <
  const Handlers extends Record<string, AnyGeneratorHandler>,
  const HandlerName extends keyof Handlers & string
>(
  definition: FluentScheduleDefinition<Handlers, HandlerName>
): FluentScheduleDefinition<Handlers, HandlerName> => definition

const makeDescriptors = <Handlers extends Record<string, AnyGeneratorHandler>>(
  handlers: Handlers,
  descriptors: Partial<Record<keyof Handlers, HandlerDescriptor>> | undefined
): HandlerDescriptors<Handlers> =>
  Object.fromEntries(
    Object.keys(handlers).map((key) => [
      key,
      descriptors?.[key as keyof Handlers] ?? descriptor()
    ])
  ) as HandlerDescriptors<Handlers>

const makeDefinition = <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, AnyGeneratorHandler>
>(
  kind: Kind,
  definition: DefinitionConfig<Name, Handlers>
): Definition<Name, Kind, Handlers> => ({
  _handlers: makeDescriptors(definition.handlers, definition.descriptors),
  _kind: kind,
  handlers: definition.handlers,
  name: definition.name,
  ...(definition.schedules === undefined ? {} : { schedules: definition.schedules })
})

export type { WorkflowOverlapPolicy, WorkflowScheduleDefinition, WorkflowScheduleSpec }

export const service = <const Name extends string, const Handlers extends Record<string, AnyGeneratorHandler>>(
  definition: DefinitionConfig<Name, Handlers>
): ServiceDefinition<Name, Handlers> => makeDefinition("service", definition)

export const workflow = <const Name extends string, const Handlers extends Record<string, AnyGeneratorHandler>>(
  definition: DefinitionConfig<Name, Handlers>
): WorkflowDefinition<Name, Handlers> => makeDefinition("workflow", definition)

export const object = <const Name extends string, const Handlers extends Record<string, AnyGeneratorHandler>>(
  definition: DefinitionConfig<Name, Handlers>
): ObjectDefinition<Name, Handlers> => makeDefinition("object", definition)
