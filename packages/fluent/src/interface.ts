import {
  type AnyGeneratorHandler,
  type Definition,
  type DefinitionKind,
  type FluentGenerator,
  type HandlerDescriptor,
  json,
  object as defineObject,
  schemas,
  serdes,
  service as defineService,
  workflow as defineWorkflow
} from "./definitions.ts"

export interface DefinitionDescriptor<
  Name extends string,
  Kind extends DefinitionKind,
  Handlers extends Record<string, HandlerDescriptor>
> {
  readonly name: Name
  readonly _kind: Kind
  readonly _handlers: Handlers
}

export type ServiceDescriptor<
  Name extends string,
  Handlers extends Record<string, HandlerDescriptor>
> = DefinitionDescriptor<Name, "service", Handlers>

export type ObjectDescriptor<
  Name extends string,
  Handlers extends Record<string, HandlerDescriptor>
> = DefinitionDescriptor<Name, "object", Handlers>

export type WorkflowDescriptor<
  Name extends string,
  Handlers extends Record<string, HandlerDescriptor>
> = DefinitionDescriptor<Name, "workflow", Handlers>

type InferInput<Descriptor> = Descriptor extends HandlerDescriptor<infer Input, unknown> ? Input : never
type InferOutput<Descriptor> = Descriptor extends HandlerDescriptor<unknown, infer Output> ? Output : never

export type ImplementHandlers<Handlers extends Record<string, HandlerDescriptor>> = {
  readonly [Key in keyof Handlers]: (input: InferInput<Handlers[Key]>) => FluentGenerator<InferOutput<Handlers[Key]>>
}

const definitionDescriptor = <
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, HandlerDescriptor>
>(
  kind: Kind,
  name: Name,
  handlers: Handlers
): DefinitionDescriptor<Name, Kind, Handlers> => ({
  _handlers: handlers,
  _kind: kind,
  name
})

const makeDescriptorFor = <const Kind extends DefinitionKind>(kind: Kind) =>
<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>
>(
  name: Name,
  handlers: Handlers
): DefinitionDescriptor<Name, Kind, Handlers> => definitionDescriptor(kind, name, handlers)

export const service = makeDescriptorFor("service")
export const object = makeDescriptorFor("object")
export const workflow = makeDescriptorFor("workflow")

export function implement<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>
>(
  definition: ServiceDescriptor<Name, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> }
): Definition<Name, "service", ImplementHandlers<Handlers>>

export function implement<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>
>(
  definition: ObjectDescriptor<Name, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> }
): Definition<Name, "object", ImplementHandlers<Handlers>>

export function implement<
  const Name extends string,
  const Handlers extends Record<string, HandlerDescriptor>
>(
  definition: WorkflowDescriptor<Name, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> }
): Definition<Name, "workflow", ImplementHandlers<Handlers>>

export function implement<
  const Name extends string,
  const Kind extends DefinitionKind,
  const Handlers extends Record<string, HandlerDescriptor>
>(
  definition: DefinitionDescriptor<Name, Kind, Handlers>,
  config: { readonly handlers: ImplementHandlers<Handlers> }
): Definition<Name, Kind, ImplementHandlers<Handlers>> {
  const implementation = {
    descriptors: definition._handlers,
    handlers: config.handlers,
    name: definition.name
  }
  switch (definition._kind) {
    case "service": {
      return defineService(implementation) as Definition<Name, Kind, ImplementHandlers<Handlers>>
    }
    case "object": {
      return defineObject(implementation) as Definition<Name, Kind, ImplementHandlers<Handlers>>
    }
    case "workflow": {
      return defineWorkflow(implementation) as Definition<Name, Kind, ImplementHandlers<Handlers>>
    }
  }
}

export { json, schemas, serdes }
export type { AnyGeneratorHandler, HandlerDescriptor }
