namespace Firegrid.Fluent

open Fable.Core
open Fable.Core.JsInterop

// ============================================================
// interface.ts — DefinitionDescriptor + service/object/workflow descriptor
// factories + `implement`.
// ============================================================

/// `DefinitionDescriptor<Name, Kind, Handlers>` — a definition's interface,
/// `_handlers` being a `Record<string, HandlerDescriptor>` (held as `obj`).
type DefinitionDescriptor =
    { Name: string
      Kind: DefinitionKind
      Handlers: obj }

[<RequireQualifiedAccess>]
module Interface =

    /// `definitionDescriptor(kind, name, handlers)`.
    let private definitionDescriptor (kind: DefinitionKind) (name: string) (handlers: obj) : DefinitionDescriptor =
        { Name = name
          Kind = kind
          Handlers = handlers }

    /// `service(name, handlers)`.
    let service (name: string) (handlers: obj) : DefinitionDescriptor = definitionDescriptor "service" name handlers

    /// `object(name, handlers)`.
    let object (name: string) (handlers: obj) : DefinitionDescriptor = definitionDescriptor "object" name handlers

    /// `workflow(name, handlers)`.
    let workflow (name: string) (handlers: obj) : DefinitionDescriptor = definitionDescriptor "workflow" name handlers

    /// `implement(definition, config)` — dispatch on kind to the matching
    /// `Definitions.{service|object|workflow}` builder. `config.handlers` is the
    /// JS object of generator handler functions; `definition._handlers` becomes
    /// the descriptors.
    let implement (definition: DefinitionDescriptor) (handlers: obj) : Definition =
        let config: DefinitionConfig =
            { Name = definition.Name
              Handlers = handlers
              Descriptors = Some definition.Handlers
              Schedules = None }

        match definition.Kind with
        | "service" -> Definitions.service config
        | "object" -> Definitions.object config
        | "workflow" -> Definitions.workflow config
        | other -> failwithf "implement: unknown definition kind %s" other

    // Re-exports (interface.ts re-exports json/schemas/serdes).
    let json = Definitions.json
    let schemas = Definitions.schemas
    let serdes = Definitions.serdes
