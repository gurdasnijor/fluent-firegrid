import type { Effect, Schema } from "effect"
import type { Journal } from "./journal.ts"
import type {
  DurableExecutionRequirements,
  ExecutionContext,
} from "./schema.ts"

export type Handler<Input, Output> = (
  ctx: ExecutionContext,
  input: Input,
) => Effect.Effect<Output, unknown, DurableExecutionRequirements>

export type Operation<
  Output,
  Error = unknown,
  Requirements = Journal | DurableExecutionRequirements,
> = Effect.fn.Return<Output, Error, Requirements>

export type GeneratorHandler<Input, Output> = (
  input: Input,
) => Operation<Output>

export type DefinitionHandler = (...args: Array<never>) => unknown

export type DefinitionKind = "service" | "object" | "workflow"

declare const descriptorTypes: unique symbol

export interface HandlerDescriptor<Input = unknown, Output = unknown> {
  readonly _tag: "HandlerDescriptor"
  readonly input?: Schema.Schema<unknown, unknown, never>
  readonly output?: Schema.Schema<unknown, unknown, never>
  readonly [descriptorTypes]?: {
    readonly input: Input
    readonly output: Output
  }
}

export type DescriptorInput<Descriptor> =
  Descriptor extends HandlerDescriptor<infer Input, unknown> ? Input : never

export type DescriptorOutput<Descriptor> =
  Descriptor extends HandlerDescriptor<unknown, infer Output> ? Output : never

export type HandlerInput<H> = H extends (
  ctx: ExecutionContext,
  input: infer Input,
) => unknown
  ? Input
  : H extends (input: infer Input) => unknown
    ? Input
    : never

export type HandlerOutput<H> = H extends (
  ...args: Array<never>
) => Effect.Effect<infer Output, unknown, unknown>
  ? Output
  : H extends (
        ...args: Array<never>
      ) => Operation<infer Output, unknown, unknown>
    ? Output
    : never

export type HandlerDescriptors<
  Handlers extends Record<string, DefinitionHandler>,
> = {
  readonly [Key in keyof Handlers]: HandlerDescriptor<
    HandlerInput<Handlers[Key]>,
    HandlerOutput<Handlers[Key]>
  >
}

export interface Definition<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, DefinitionHandler>,
> {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: HandlerDescriptors<Handlers>
  readonly handlers: Handlers
}

export type ServiceDefinition<
  Name extends string,
  Handlers extends Record<string, DefinitionHandler>,
> = Definition<Name, "service", Handlers>

export type ObjectDefinition<
  Name extends string,
  Handlers extends Record<string, DefinitionHandler>,
> = Definition<Name, "object", Handlers>

export type WorkflowDefinition<
  Name extends string,
  Handlers extends Record<string, DefinitionHandler>,
> = Definition<Name, "workflow", Handlers>

const descriptor = <
  Input = void,
  Output = void,
  EncodedInput = unknown,
  EncodedOutput = unknown,
>(options?: {
  readonly input?: Schema.Schema<Input, EncodedInput, never>
  readonly output?: Schema.Schema<Output, EncodedOutput, never>
}): HandlerDescriptor<Input, Output> => ({
  _tag: "HandlerDescriptor",
  ...(options?.input === undefined
    ? {}
    : { input: options.input as Schema.Schema<unknown, unknown, never> }),
  ...(options?.output === undefined
    ? {}
    : { output: options.output as Schema.Schema<unknown, unknown, never> }),
})

type SchemaDescriptorOptions<Input, Output, EncodedInput, EncodedOutput> = {
  readonly input?: Schema.Schema<Input, EncodedInput, never>
  readonly output?: Schema.Schema<Output, EncodedOutput, never>
}

export const json = <Input = void, Output = void>(): HandlerDescriptor<
  Input,
  Output
> => descriptor<Input, Output>()

export const schemas = <
  Input = void,
  Output = void,
  EncodedInput = unknown,
  EncodedOutput = unknown,
>(
  options: SchemaDescriptorOptions<Input, Output, EncodedInput, EncodedOutput>,
): HandlerDescriptor<Input, Output> => descriptor(options)

export const serdes: typeof schemas = (options) => descriptor(options)

const makeDescriptors = <Handlers extends Record<string, DefinitionHandler>>(
  handlers: Handlers,
  descriptors: Partial<Record<keyof Handlers, HandlerDescriptor>> | undefined,
): HandlerDescriptors<Handlers> =>
  Object.fromEntries(
    Object.keys(handlers).map((key) => [
      key,
      descriptors?.[key as keyof Handlers] ?? json(),
    ]),
  ) as HandlerDescriptors<Handlers>

interface DefinitionConfig<
  Name extends string,
  Handlers extends Record<string, DefinitionHandler>,
> {
  readonly name: Name
  readonly handlers: Handlers
  readonly descriptors?: Partial<Record<keyof Handlers, HandlerDescriptor>>
}

const makeDefinition = <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, DefinitionHandler>,
>(
  kind: Kind,
  definition: DefinitionConfig<Name, Handlers>,
): Definition<Name, Kind, Handlers> => ({
  name: definition.name,
  _kind: kind,
  _handlers: makeDescriptors(definition.handlers, definition.descriptors),
  handlers: definition.handlers,
})

const makeDefinitionFor =
  <const Kind extends DefinitionKind>(kind: Kind) =>
  <
    const Name extends string,
    const Handlers extends Record<string, DefinitionHandler>,
  >(
    definition: DefinitionConfig<Name, Handlers>,
  ): Definition<Name, Kind, Handlers> =>
    makeDefinition(kind, definition)

export const service = makeDefinitionFor("service")
export const object = makeDefinitionFor("object")
export const workflow = makeDefinitionFor("workflow")
