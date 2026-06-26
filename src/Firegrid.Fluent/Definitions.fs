namespace Firegrid.Fluent

open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Runtime

// ============================================================
// definitions.ts — Definition / HandlerDescriptor builders
// ============================================================

/// `DefinitionKind` = "service" | "workflow" | "object". Held as a string.
type DefinitionKind = string

/// `Operation<A, E, R>` = `Effect.Effect<A, E, R>`. In F#/EffSharp consumers
/// use `Effect<'A,'E,'R>` directly; kept here only as documentation.

/// A generator handler `(input) => FluentGenerator<Output>`. The fluent runtime
/// drives Effect generators; modeled as an opaque JS function `obj` so the
/// generator machinery is preserved at runtime. REDUCTION: the TS generator
/// type machinery is collapsed to a runtime `obj` function (see summary).
type AnyGeneratorHandler = obj

/// `FluentGenerator<A>` — opaque generator value (JS). Kept as `obj`.
type FluentGenerator = obj

/// `HandlerDescriptor` — schema fields are opaque `obj option` (effect/Schema
/// typed decoding is reduced to pass-through; the schema is carried so the HTTP
/// layer / bindTanStack can pass it through unchanged).
type HandlerDescriptor =
    { Tag: string // "HandlerDescriptor"
      Input: obj option
      Output: obj option }

/// `FluentScheduleDefinition` — `Omit<WorkflowScheduleDefinition, "input">` plus
/// `handler` and a fluent `input` (value or thunk). Held with the input as
/// `obj option` (value | `() => value | Promise<value>`).
type FluentScheduleDefinition =
    { Id: ScheduleId option
      Schedule: WorkflowScheduleSpec
      OverlapPolicy: WorkflowOverlapPolicy option
      Enabled: bool option
      Handler: string
      Input: obj option }

/// `Definition<Name, Kind, Handlers>`.
type Definition =
    { Name: string
      Kind: DefinitionKind // _kind
      /// `_handlers` — descriptor per handler name (JS object).
      Handlers: obj // HandlerDescriptors (Record<string, HandlerDescriptor>)
      /// `handlers` — the generator handler functions (JS object).
      HandlerFns: obj
      Schedules: FluentScheduleDefinition[] option }

/// `DefinitionConfig`.
type DefinitionConfig =
    { Name: string
      Handlers: obj // Record<string, AnyGeneratorHandler>
      Descriptors: obj option // Partial<Record<keyof Handlers, HandlerDescriptor>>
      Schedules: FluentScheduleDefinition[] option }

    static member Create(name: string, handlers: obj) =
        { Name = name
          Handlers = handlers
          Descriptors = None
          Schedules = None }

/// `HandlerDescriptorOptions`.
type HandlerDescriptorOptions =
    { Input: obj option
      Output: obj option }

    static member Empty = { Input = None; Output = None }

[<RequireQualifiedAccess>]
module Definitions =

    // Re-export cron / every from Runtime (definitions.ts re-exports them).
    let cron = DefineRuntime.cron

    module every =
        let milliseconds = DefineRuntime.every.milliseconds
        let seconds = DefineRuntime.every.seconds
        let minutes = DefineRuntime.every.minutes
        let hours = DefineRuntime.every.hours

    /// `descriptor(options?)` — base builder.
    let descriptor (options: HandlerDescriptorOptions option) : HandlerDescriptor =
        let input =
            match options with
            | Some o -> o.Input
            | None -> None

        let output =
            match options with
            | Some o -> o.Output
            | None -> None

        { Tag = "HandlerDescriptor"
          Input = input
          Output = output }

    /// JS-object form of a descriptor (so it can live inside `_handlers`).
    let descriptorToObj (d: HandlerDescriptor) : obj =
        let o = createObj [ "_tag" ==> d.Tag ]

        match d.Input with
        | Some i -> FluentSdk.setProp o "input" i
        | None -> ()

        match d.Output with
        | Some out -> FluentSdk.setProp o "output" out
        | None -> ()

        o

    /// Reconstruct a `HandlerDescriptor` from its JS-object form.
    let descriptorOfObj (o: obj) : HandlerDescriptor =
        { Tag = FluentSdk.prop<string> o "_tag"
          Input =
            let v = FluentSdk.prop<obj> o "input"
            if FluentSdk.isUndefined v then None else Some v
          Output =
            let v = FluentSdk.prop<obj> o "output"
            if FluentSdk.isUndefined v then None else Some v }

    /// `json()` — descriptor with no schemas.
    let json () : HandlerDescriptor = descriptor None

    /// `schemas(options)`.
    let schemas (options: HandlerDescriptorOptions) : HandlerDescriptor = descriptor (Some options)

    /// `serdes` — alias of `schemas`.
    let serdes (options: HandlerDescriptorOptions) : HandlerDescriptor = schemas options

    /// `schedule(definition)` — identity.
    let schedule (definition: FluentScheduleDefinition) : FluentScheduleDefinition = definition

    /// `makeDescriptors(handlers, descriptors)` — one descriptor per handler key.
    /// `descriptors?.[key] ?? descriptor()`. Returns a JS object.
    let makeDescriptors (handlers: obj) (descriptors: obj option) : obj =
        let entries =
            FluentSdk.objectKeys handlers
            |> Array.map (fun key ->
                let provided =
                    match descriptors with
                    | Some d ->
                        let v = FluentSdk.prop<obj> d key
                        if FluentSdk.isUndefined v then None else Some v
                    | None -> None

                let value =
                    match provided with
                    | Some v -> v
                    | None -> descriptorToObj (descriptor None)

                (key, value))

        FluentSdk.objectFromEntries entries

    /// `makeDefinition(kind, definition)`.
    let makeDefinition (kind: DefinitionKind) (definition: DefinitionConfig) : Definition =
        { Name = definition.Name
          Kind = kind
          Handlers = makeDescriptors definition.Handlers definition.Descriptors
          HandlerFns = definition.Handlers
          Schedules = definition.Schedules }

    /// `service(definition)`.
    let service (definition: DefinitionConfig) : Definition = makeDefinition "service" definition

    /// `workflow(definition)`.
    let workflow (definition: DefinitionConfig) : Definition = makeDefinition "workflow" definition

    /// `object(definition)`.
    let object (definition: DefinitionConfig) : Definition = makeDefinition "object" definition
