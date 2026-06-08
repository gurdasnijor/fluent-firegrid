import {
  type Definition,
  type DefinitionKind,
  type HandlerDescriptor,
  type Operation,
  json,
  object as defineObject,
  schemas,
  serdes,
  service as defineService,
  workflow as defineWorkflow,
} from "./definitions.ts"

export interface DefinitionDescriptor<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, HandlerDescriptor>,
> {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: Handlers
}

export type ServiceDescriptor<
  Name extends string,
  Handlers extends Record<string, HandlerDescriptor>,
> = DefinitionDescriptor<Name, "service", Handlers>

export type ObjectDescriptor<
  Name extends string,
  Handlers extends Record<string, HandlerDescriptor>,
> = DefinitionDescriptor<Name, "object", Handlers>

export type WorkflowDescriptor<
  Name extends string,
  Handlers extends Record<string, HandlerDescriptor>,
> = DefinitionDescriptor<Name, "workflow", Handlers>

type InferInput<Descriptor> = Descriptor extends HandlerDescriptor<infer Input, unknown> ? Input : never
type InferOutput<Descriptor> = Descriptor extends HandlerDescriptor<unknown, infer Output> ? Output : never

export type ImplementHandlers<Handlers extends Record<string, HandlerDescriptor>> = {
  readonly [Key in keyof Handlers]: (
    input: InferInput<Handlers[Key]>,
  ) => Operation<InferOutput<Handlers[Key]>>
}

const definitionDescriptor = <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, HandlerDescriptor>,
>(
  kind: Kind,
  name: Name,
  handlers: Handlers,
): DefinitionDescriptor<Name, Kind, Handlers> => ({
  name,
  _kind: kind,
  _handlers: handlers,
})

const makeDescriptorFor =
  <const Kind extends DefinitionKind>(kind: Kind) =>
  <
    const Name extends string,
    const Handlers extends Record<string, HandlerDescriptor>,
  >(name: Name, handlers: Handlers): DefinitionDescriptor<Name, Kind, Handlers> =>
    definitionDescriptor(kind, name, handlers)

export const service = makeDescriptorFor("service")
export const object = makeDescriptorFor("object")
export const workflow = makeDescriptorFor("workflow")

export function implement<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>,
>(
  definition: ServiceDescriptor<Name, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> },
): Definition<Name, "service", ImplementHandlers<Handlers>>

export function implement<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>,
>(
  definition: ObjectDescriptor<Name, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> },
): Definition<Name, "object", ImplementHandlers<Handlers>>

export function implement<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>,
>(
  definition: WorkflowDescriptor<Name, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> },
): Definition<Name, "workflow", ImplementHandlers<Handlers>>

export function implement<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, HandlerDescriptor>,
>(
  definition: DefinitionDescriptor<Name, Kind, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> },
): Definition<Name, Kind, ImplementHandlers<Handlers>> {
  const implementation = {
    name: definition.name,
    handlers: config.handlers,
    descriptors: definition._handlers,
  }
  switch (definition._kind) {
    case "service":
      return defineService(implementation) as Definition<Name, Kind, ImplementHandlers<Handlers>>
    case "object":
      return defineObject(implementation) as Definition<Name, Kind, ImplementHandlers<Handlers>>
    case "workflow":
      return defineWorkflow(implementation) as Definition<Name, Kind, ImplementHandlers<Handlers>>
  }
}

export { json, schemas, serdes }
