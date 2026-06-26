# SDD: F# / EffSharp Package Migration

### Package-by-package API and implementation plan for PR 72

|   |   |
| --- | --- |
| Status | Draft migration guidance |
| Date | 2026-06-26 |
| Branch focus | `pr-72-migrate-fable`, PR 72 |
| Package focus | `@firegrid/core`, `@firegrid/runtime`, `@firegrid/clients`, `@firegrid/fluent`, `@firegrid/trace`, plus `@firegrid/log` and `@firegrid/store` integration |

---

## Decision

PR 72 should not be treated as a completed port. It removed the TypeScript
implementations for `core`, `runtime`, `fluent`, and `trace`, then replaced
them with compileable F# stubs. The next step should be an explicit F# API
design pass and a behavior-preserving migration, package by package.

The migration target is:

- F# domain APIs that feel natural in F#.
- F#-native async/error/resource patterns by default.
- EffSharp only where typed requirements, layers, resource scopes, or streams
  materially improve the implementation.
- Fable-generated JavaScript packages that keep a practical compatibility
  layer for existing TypeScript callers.
- Existing behavior captured by translated tests before deleting the old
  TypeScript contracts permanently.

## Toolchain Constraints

Current branch facts:

- `.config/dotnet-tools.json` pins `fable` to `5.4.0`.
- F# projects target `netstandard2.0`.
- F# projects reference `Fable.Core` `5.1.0`, `Fable.Promise` `3.2.0`, and
  floating local `EffSharp` packages.
- No project currently pins `<LangVersion>`, so the active compiler comes from
  the installed .NET SDK, but Fable support is the real portability boundary.
- Fable 5 adds .NET 10 / F# 10 support but the explicitly documented supported
  language additions are still modest: nullable reference types, discriminated
  union `.Is*` properties, bool partial active patterns, empty-bodied
  computation expressions, and FSharp.Core updates.

Practical rule for this migration:

- Use records, discriminated unions, modules, object expressions, active
  patterns, options, lists, maps, and computation expressions.
- Use ordinary F# functions, `Result`, `Async`, `task`, or Fable `Promise`
  where those are enough.
- Use EffSharp `Effect<'A, 'E, 'R>` only when the function benefits from an
  explicit error channel and/or explicit requirements.
- Avoid relying on .NET reflection, type providers, C#-only libraries, or
  advanced runtime metadata. Fable output needs plain JavaScript interop.
- Keep F#-first domain types separate from JS-friendly `Exports` modules.

References:

- F# async and task overview:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/tutorials/async>
- F# computation expressions:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/computation-expressions>
- F# discriminated unions:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/discriminated-unions>
- F# Result:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/results>
- Fable 5 release candidate notes:
  <https://fable.io/blog/2026/2026-02-27-Fable_5_release_candidate.html>
- Fable compatibility notes:
  <https://fable.io/docs/javascript/compatibility.html>

## EffSharp Scope Decision

The first migration sketch assumed EffSharp should be the default surface. That
is probably too much. F# and Fable already provide much of what the TypeScript
Effect layer was compensating for:

- algebraic data types through discriminated unions;
- `Option` and `Result` for expected absence and failures;
- pattern matching for exhaustive domain handling;
- computation expressions for domain-specific syntax;
- `Async`, `task`, and Fable `Promise` interop for asynchronous work;
- records and modules for lightweight dependency passing;
- `use`, `use!`, `try/finally`, and scoped object lifetimes for ordinary
  resources.

EffSharp should therefore be treated as an implementation tool, not the default
SDK authoring model. The full evaluation lives in
[`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md);
this package migration plan follows that decision.

### What EffSharp Still Buys

EffSharp is worth keeping where these benefits are active:

- typed error and requirement channels on one value;
- service/layer composition for infrastructure-heavy packages;
- scoped resource acquisition for stream/session lifecycles;
- effect streams around JS async iterables;
- shared semantics with the existing TypeScript Effect proof harness;
- consistent tracing/retry/cancellation combinators if EffSharp exposes them
  well enough for Fable.

These are most relevant in `Firegrid.Log`, `Firegrid.Store`,
`Firegrid.Runtime`, and possibly `Firegrid.Trace`.

### Where F# Native Should Win

Public authoring APIs should prefer F# native shapes:

- handler bodies can be `Async<'T>`, `Promise<'T>`, or a small Firegrid-specific
  workflow CE rather than raw `Effect<'T, 'E, 'R>`;
- domain validation should return `Result<'T, 'Error>`;
- optional state should use `Option<'T>`;
- expected domain outcomes should use discriminated unions;
- dependencies for simple APIs should be explicit records or parameters;
- F# clients should call typed handler references, not manipulate Effect
  environments.

This keeps the SDK understandable to F# users who do not already know Effect.

### Recommended Boundary

Use a layered design:

1. F# domain and authoring surface: no EffSharp in signatures unless needed.
2. Runtime adapter layer: converts F# handler computations into the internal
   durable runtime representation.
3. Infrastructure packages: may use EffSharp internally and expose either
   EffSharp or F#-native wrappers depending on audience.
4. JS exports: preserve Promise-returning compatibility for TypeScript callers.

In short: use EffSharp for substrate implementation power, not for every SDK
shape.

## Cross-Package Shape

Use this split everywhere:

- `Types.fs`: records, discriminated unions, nominal aliases, error types.
- Domain modules: pure functions and EffSharp programs.
- `Exports.fs`: Fable/JS compatibility functions and dynamic object facades.
- `Interop.fs`: imports from npm packages or browser/runtime APIs.

Internal F# APIs should be typed, explicit, and expression-oriented. JS exports
can preserve familiar names like `workflow`, `service`, `run`, `client`,
`genericCall`, `defineWorkflowRuntime`, and `materializeWorkflowSchedules`.

Do not force TypeScript's dynamic object method generation into the core F# API.
Where TypeScript had "object with one method per handler", F# should use
first-class handler references, records, discriminated unions, and computation
expressions. The JS `Exports` layer can still build dynamic method objects with
Fable interop for compatibility.

For ergonomic F# authoring, prefer:

- computation expressions for assembling service/workflow/object definitions;
- typed handler references for reusable contracts and client calls;
- functions and modules over static classes;
- records for configuration;
- discriminated unions for modes and results;
- `nameof` only as an escape hatch, not as the main authoring model;
- explicit codecs only when runtime validation or JS wire validation is needed.

## `@firegrid/core` -> `Firegrid.Core`

### Responsibility

`core` owns the package-neutral durable workflow model:

- workflow identifiers, versions, run identifiers;
- workflow definitions and handlers;
- durable context primitives;
- workflow event schema;
- state diff operations;
- registry/version selection;
- request parsing and webhook envelope helpers;
- domain errors shared by runtime, fluent, and store.

The old TypeScript package also contained `runWorkflow`. In the F# shape, the
pure workflow driver can remain in `Core`, while orchestration across leases,
stores, schedules, and sweeps belongs in `Runtime`.

### Proposed API

Important shape only:

```fsharp
namespace Firegrid.Core

type WorkflowId = string
type RunId = string
type StepId = string

type HandlerDescriptor = { InputSchema: obj option; OutputSchema: obj option }

type WorkflowEvent =
    | RunStarted of runId: RunId
    | RunFinished of runId: RunId * output: obj
    | StepFinished of stepId: StepId * result: obj
    | SignalResolved of stepId: StepId * payload: obj
    | RunErrored of runId: RunId * error: obj

type Workflow<'A>

type WorkflowDefinition<'Input, 'Output, 'State> =
    { Id: WorkflowId
      Initialize: 'Input -> 'State
      Handler: 'Input -> Workflow<'Output> }
```

The important point is not the exact payload list above; it is the shape:
events are a discriminated union internally, then converted at the boundary.

```fsharp
[<RequireQualifiedAccess>]
module WorkflowEvent =
    val toWire: WorkflowEvent -> obj
    val fromWire: obj -> Result<WorkflowEvent, FluentFiregridError>
    val isCheckpoint: WorkflowEvent -> bool
```

### Migration Plan

1. Port `types.ts` first, but replace string unions with discriminated unions.
2. Port `state-diff.ts` as a pure module with exhaustive pattern matching.
3. Port `select-version.ts` with explicit version-routing result cases.
4. Port `define-workflow.ts` as a small builder module, not a TypeScript-style
   chain type clone.
5. Port `run-workflow.ts` after the event and run-state model is stable.
6. Add JS wire conversion tests for every `WorkflowEvent` case before changing
   `Store.WorkflowLog`.

## `@firegrid/runtime` -> `Firegrid.Runtime`

### Responsibility

`runtime` owns process-level orchestration:

- `WorkflowExecutionStore` contracts;
- in-memory test store;
- run store adapter to the core workflow engine;
- start, signal, approval, timer, and sweep driver;
- recurring schedule materialization;
- runtime definition builder.

It should not duplicate core event or run-state types. The current branch
duplicates several types in `Firegrid.Store`; that should be removed as part of
this migration.

### Proposed API

The important shape is a runtime record backed by a store capability. The full
store contract should be implemented in code, not duplicated in this document.

```fsharp
namespace Firegrid.Runtime

open Firegrid.Core

type WorkflowExecution =
    { RunId: RunId; WorkflowId: WorkflowId; Status: RunStatus; Input: obj }

type RuntimeError<'StoreError> =
    | StoreError of 'StoreError
    | WorkflowError of FluentFiregridError

type WorkflowExecutionStore<'StoreError> =
    { Runs: obj; Events: obj; Signals: obj; Timers: obj; Schedules: obj }

type WorkflowRuntimeDefinition<'StoreError> =
    { Workflows: Map<WorkflowId, WorkflowRegistration>
      Store: WorkflowExecutionStore<'StoreError>
      StartRun: obj -> JS.Promise<Result<RunResult, RuntimeError<'StoreError>>>
      Sweep: obj -> JS.Promise<Result<SweepResult, RuntimeError<'StoreError>>> }
```

The `obj` placeholders above are intentional in this document. The concrete
argument/result records should be defined in code once the runtime store API is
ported, but the design decision is that the runtime is grouped by capabilities:
runs, events, signals, timers, and schedules.

Schedule definitions should be DUs:

```fsharp
type WorkflowOverlapPolicy =
    | Skip
    | Allow
    | BufferOne
    | CancelPrevious
    | TerminatePrevious

type WorkflowScheduleSpec =
    | Cron of expression: string * timezone: string option
    | Interval of everyMs: int64 * timezone: string option
```

### Migration Plan

1. Replace the current stub `Types.fs` with the full store and runtime contract.
2. Port `schedule-materializer.ts` as a mostly pure module.
3. Port `run-store-adapter.ts` after `Core.runWorkflow` exists.
4. Port `runtime-driver.ts` into `RuntimeDriver.fs` as the interpreter for
   `Workflow<'A>`.
5. Port `in-memory-store.ts` with scoped mutable state hidden behind a runtime
   record; add an EffSharp layer adapter only if host composition needs it.
6. Make `defineWorkflowRuntime` return `WorkflowRuntimeDefinition`, not a loose
   record with only `Workflows`.

## `@firegrid/log` -> `Firegrid.Log`

### Responsibility

`log` is already a useful F# package. It wraps the S2 SDK with EffSharp:

- `S2Config`;
- typed S2 request/response records;
- `S2Error`;
- `S2Client` service tag and layer;
- stream append/read/session helpers.

### Proposed API Adjustments

Keep the current module shape. The key design point is that S2 remains an
EffSharp service/layer, and downstream packages consume typed stream helpers
through that service.

```fsharp
[<RequireQualifiedAccess>]
module S2 =
    val tag: Tag<S2Client>
    val layer: S2Config -> Layer<S2Error, 'RIn>
    val service<'E> : Effect<S2Client, 'E, Context>

    module Stream =
        val append: S2AppendRequest -> Effect<S2AppendAck, S2Error, Context>
        val read: S2ReadRequest -> Effect<S2ReadBatch, S2Error, Context>
```

### Migration Plan

1. Do not rewrite the S2 wrapper during this migration.
2. Add missing tests around JS interop conversion and error classification.
3. Add helpers required by `Store` only after the `Runtime` store contract is
   finalized.

## `@firegrid/store` -> `Firegrid.Store`

### Responsibility

`store` is already F# and should become the S2-backed implementation of the
runtime contracts:

- object state streams;
- workflow event log streams;
- workflow execution metadata;
- timers and signals;
- schedule buckets;
- stale run leasing.

The current package only covers object state and append/read workflow events.
It also duplicates `RunId`, `WorkflowEvent`, `AppendEventsArgs`, and
`StoredWorkflowEvent`. Those should move to imports from `Core` and `Runtime`.

### Proposed API

```fsharp
namespace Firegrid.Store

open Effect
open Firegrid.Log
open Firegrid.Runtime

type S2Runtime =
    { Basin: string
      Namespace: string
      Layer: Layer<S2Error, unit> }

[<RequireQualifiedAccess>]
module S2WorkflowExecutionStore =
    val make: S2Runtime -> WorkflowExecutionStore<S2Error, unit>

[<RequireQualifiedAccess>]
module S2ObjectState =
    val append: S2Runtime -> S2StateAppend -> Effect<S2AppendAck, S2Error, unit>
    val read: S2Runtime -> S2StateRead -> Effect<string list, S2Error, unit>
```

### Migration Plan

1. Change `Types.fs` to import `Core` and `Runtime` types instead of redefining
   them.
2. Keep object state working while adding run execution metadata streams.
3. Implement the complete `WorkflowExecutionStore<S2Error, unit>` record.
4. Preserve current stream naming functions, because proofs and guides depend
   on stable S2 names.
5. Re-run store proof translations after each store contract method lands.

## `@firegrid/clients` -> `Firegrid.Clients`

### Responsibility

`clients` should become a real package split from the old `fluent/clients.ts`:

- call and send request model;
- invocation options and duration normalization;
- invocation binding contract;
- send references and attachable handles;
- generic call/send helpers;
- JS-compatible dynamic client builders.

Typed handler method objects are excellent in TypeScript but are not the right
core abstraction in F#. F# should keep explicit `DefinitionRef` and handler
names. The JS export layer can recreate the old dynamic method ergonomics.

### Proposed API

Important shape only:

```fsharp
namespace Firegrid.Clients

open Firegrid.Core

type DefinitionKind = Service | Workflow | Object

type DefinitionRef =
    { Kind: DefinitionKind
      Name: string
      Handlers: Map<string, HandlerDescriptor> }

type CallRequest<'Input> =
    { Kind: DefinitionKind
      Name: string
      Handler: string
      Key: string option
      Input: 'Input }

type InvocationBinding<'Error> =
    { Call: CallRequest<obj> -> JS.Promise<Result<obj, 'Error>>
      Send: CallRequest<obj> -> JS.Promise<Result<InvocationHandle<obj>, 'Error>> }
```

Typed helper shape:

```fsharp
[<RequireQualifiedAccess>]
module Client =
    val call:
        HandlerRef<'Input, 'Output> ->
        'Input ->
            JS.Promise<Result<'Output, FluentFiregridError>>

    val send:
        HandlerRef<'Input, 'Output> ->
        'Input ->
            JS.Promise<Result<InvocationHandle<'Output>, FluentFiregridError>>
```

Details such as idempotency keys, delayed sends, metadata, attach/output
aliases, and dynamic JS client objects should live behind these primitives, not
dominate the F# authoring API.

### Migration Plan

1. Move request and handle types out of `Firegrid.Fluent`.
2. Implement duration validation before client generation.
3. Add ambient-context integration from `Firegrid.Fluent`, not in this package.
4. Add `Exports.fs` dynamic method builders for TypeScript compatibility.
5. Translate old `public-surface.test.ts` client cases into tests against the
   generated JS package.

## `@firegrid/fluent` -> `Firegrid.Fluent`

### Responsibility

`fluent` is the authoring package:

- direct `service`, `workflow`, and `object` definitions;
- descriptor-first `iface` / `implement`;
- handler descriptors and schema codecs;
- durable context service;
- `run`, `sleep`, `sleepUntil`, `waitForSignal`;
- ambient service/workflow/object clients;
- external events and awakeables;
- table-shaped object state and CEL predicates;
- binding fluent definitions into the runtime;
- HTTP transport binding.

### Proposed API

The fluent package should be designed around the things F# is good at:
typed values, computation expressions, records, discriminated unions, and
pipeline-friendly functions. The author should not have to nest
`Definition.service` and `Handler.define` lists for ordinary code.

The target authoring surface should have two ergonomic paths:

- direct definitions for app-local services;
- contract-first definitions for shared clients and implementations.

### Direct Definition Ergonomics

For app-local services, workflows, and objects, a computation expression should
be the primary surface:

```fsharp
open Firegrid.Fluent

type TriageInput = { IncidentId: string }
type TriageOutput = { Accepted: bool }

let triage =
    handler<TriageInput, TriageOutput> "triage"

let reserveIncident =
    Step.defineAsync "reserve" reserveIncidentAsync

let incident =
    service "incident" {
        handle triage (fun input ->
            workflow {
                do! Step.call reserveIncident input.IncidentId
                return { Accepted = true }
            })
    }
```

The important ergonomic choices are:

- the handler reference is a typed value;
- `service "incident" { ... }` reads as a definition block;
- `handle triage ...` ties implementation to the typed handler reference;
- `workflow { ... }` is the durable Firegrid computation, not a raw EffSharp
  program;
- named durable work is represented by typed values such as
  `Step<'Input, 'Output>`.

Runtime schemas/codecs should be opt-in:

```fsharp
let triage =
    handler<TriageInput, TriageOutput> "triage"
    |> Handler.withJson

let submit =
    handler<SubmitInput, SubmitOutput> "submit"
    |> Handler.withSchemas SubmitInput.schema SubmitOutput.schema
```

This lets pure F# callers use strong static types without pretending Fable can
derive runtime schemas from arbitrary F# records. When JS callers or transport
validation need codecs, the handler reference carries them explicitly.

### Contract-First Ergonomics

Shared APIs should be values, not generated dynamic members. A module can expose
handler references and a contract:

```fsharp
module IncidentApi =
    type TriageInput = { IncidentId: string }
    type TriageOutput = { Accepted: bool }

    let triage =
        handler<TriageInput, TriageOutput> "triage"
        |> Handler.withJson

    let contract =
        serviceContract "incident" {
            endpoint triage
        }
```

An implementation then stays concise and type-safe:

```fsharp
let incident =
    implement IncidentApi.contract {
        handle IncidentApi.triage (fun input ->
            workflow {
                do! Step.call reserveIncident input.IncidentId
                return { Accepted = true }
            })
    }
```

Typed clients should consume the same handler reference:

```fsharp
let routeIncident incidentId =
    promise {
        let! result =
            IncidentApi.contract
            |> Client.call IncidentApi.triage { IncidentId = incidentId }

        return result.Accepted
    }
```

This is a better F# equivalent to the old TypeScript dynamic
`client(binding, incident).triage(...)` shape. TypeScript callers can still get
dynamic objects from `Exports.fs`, but F# callers should not need generated
property names to get type safety.

### Workflow And Object Ergonomics

Workflows should use the same handler references plus schedule custom
operations:

```fsharp
module JobsApi =
    type DailyInput = { Id: string }
    type DailyOutput = { Id: string }

    let daily = handler<DailyInput, DailyOutput> "daily"
    let manual = handler<DailyInput, DailyOutput> "manual"

let jobs =
    workflow "jobs" {
        handle JobsApi.daily (fun input ->
            workflow {
                do! Step.call reconcileJob input.Id
                return { Id = input.Id }
            })

        handle JobsApi.manual (fun input ->
            workflow {
                return { Id = input.Id }
            })

        schedule JobsApi.daily (
            Cron "0 0 * * *"
            |> Schedule.id "daily-reconcile"
            |> Schedule.input { Id = "scheduled" }
            |> Schedule.overlap Skip
        )

        schedule JobsApi.daily (
            Every (Duration.hours 1)
            |> Schedule.disabled
        )
    }
```

Objects should avoid the reserved F# keyword `object` in the F#-first API. Use
`durableObject` in F#, and keep `object` only in the JS export layer:

```fsharp
module CounterApi =
    type AddInput = { By: int }
    type AddOutput = { Value: int }

    let add = handler<AddInput, AddOutput> "add"

let counter =
    durableObject "counter" {
        handle CounterApi.add (fun input ->
            workflow {
                let! current = State.get "value" |> Workflow.map (Option.defaultValue 0)
                let next = current + input.By
                do! State.set "value" next
                return { Value = next }
            })
    }
```

### Builder Sketch

The implementation should be small: typed handler refs are collected into
definition records by computation-expression builders. Avoid exposing these
internals as the main user-facing API.

```fsharp
type Workflow<'A>

type HandlerRef<'Input, 'Output> =
    { Name: string
      Descriptor: Firegrid.Core.HandlerDescriptor }

type HandlerImplementation =
    { Handler: HandlerRef<obj, obj>
      Invoke: obj -> Workflow<obj> }

type DefinitionBuilder =
    [<CustomOperation("handle")>]
    member Handle:
        spec: DefinitionSpec *
        handler: HandlerRef<'Input, 'Output> *
        run: ('Input -> Workflow<'Output>) ->
            DefinitionSpec

let handler<'Input, 'Output> : string -> HandlerRef<'Input, 'Output>
let service : string -> DefinitionBuilder
let workflow : string -> WorkflowBuilder
let durableObject : string -> DefinitionBuilder
```

Workflow operations should be typed durable values:

```fsharp
type Step<'Input, 'Output>

[<RequireQualifiedAccess>]
module Step =
    val define: string -> ('Input -> 'Output) -> Step<'Input, 'Output>
    val defineAsync: string -> ('Input -> Async<'Output>) -> Step<'Input, 'Output>
    val definePromise: string -> ('Input -> JS.Promise<'Output>) -> Step<'Input, 'Output>
    val call: Step<'Input, 'Output> -> 'Input -> Workflow<'Output>
    val callWithRetry: RetryPolicy -> Step<'Input, 'Output> -> 'Input -> Workflow<'Output>

[<RequireQualifiedAccess>]
module Workflow =
    val all: Workflow<'A> seq -> Workflow<'A list>
    val seq: Workflow<'A> list -> Workflow<'A list>
    val map: ('A -> 'B) -> Workflow<'A> -> Workflow<'B>
```

State tables should use F# records and explicit primary keys:

```fsharp
type IncidentRow =
    { Id: string
      Status: string
      Priority: int }

let IncidentTable =
    State.table<IncidentRow>
        { Name = "incident"
          PrimaryKey = fun row -> row.Id
          Schema = None }

let waitForHighPriority id =
    workflow {
        let! row =
            State.waitForKey
                IncidentTable
                id
                (Cel.expr "row.priority >= 3 && change.operation != 'delete'")
            |> Wait.timeout (Duration.minutes 1)

        return row
    }
```

External events should preserve the old vocabulary while using F# records and
DUs internally:

```fsharp
type Awakeable<'A> =
    { Id: string
      Await: Workflow<Result<'A, AwakeableError>> }

type AwakeablePayload<'A> =
    | AwakeableResolved of 'A
    | AwakeableRejected of reason: obj

type IExternalSignalBinding<'Error, 'R> =
    abstract DeliverSignal<'Payload>:
        ExternalSignalDeliveryRequest<'Payload> ->
            JS.Promise<Result<ExternalSignalDelivery, 'Error>>

[<RequireQualifiedAccess>]
module ExternalEvents =
    val awakeable<'A>: string -> Workflow<Awakeable<'A>>
    val resolveAwakeable<'A>: string -> 'A -> JS.Promise<Result<ExternalSignalDelivery, FluentFiregridError>>
```

### Migration Plan

1. Port `HandlerRef`, erased handler descriptors, and definition records first.
2. Implement `service`, `workflow`, `durableObject`, `serviceContract`, and
   `implement` computation expression builders.
3. Implement `Workflow<'A>` and the `workflow {}` builder before exposing
   handler implementations.
4. Port typed client helpers around `HandlerRef` after `Firegrid.Clients` is
   ready.
5. Port `run.ts` primitives as typed workflow operations (`Step.call`,
   `Timer.sleep`, `Timer.sleepUntil`, `Wait.event`) against the runtime
   interpreter.
6. Port external events and awakeables.
7. Port state table/CEL helpers after the context service is stable.
8. Port `bindTanStack.ts` into a runtime binding module.
9. Port HTTP handler last, once definitions and runtime binding compile.

## `@firegrid/trace` -> `Firegrid.Trace`

### Responsibility

The old `trace` package was not just a config holder. It contained:

- a chDB-backed Effect SQL client;
- ClickHouse literal and JSONEachRow encoding helpers;
- scoped session acquisition and cleanup;
- chDB native query wrappers;
- OpenTelemetry span row conversion;
- chDB span exporter and remote span exporter.

The F# package should keep this split: `ChdbClient` for SQL/query behavior and
`ChdbExporter` for OpenTelemetry export.

### Proposed API

```fsharp
namespace Firegrid.Trace

open Effect

type ChdbClientConfig =
    { Path: string option
      Database: string option
      Settings: Map<string, obj> }

type ChdbError =
    | ConnectionError of string
    | QueryError of string

type Ch<'A> =
    { Type: string
      Lit: 'A -> string }

[<RequireQualifiedAccess>]
module Ch =
    val String: Ch<string>
    val Int64: Ch<int64>
    val Array: Ch<'A> -> Ch<'A list>

type IChdbClientApi =
    abstract Param<'A>: Ch<'A> -> 'A -> obj
    abstract Query: string -> Effect<obj list, ChdbError, Context>
    abstract InsertRows: table: string -> rows: obj list -> Effect<unit, ChdbError, Context>

[<RequireQualifiedAccess>]
module ChdbClient =
    val tag: Tag<IChdbClientApi>
    val layer: ChdbClientConfig -> Layer<ChdbError, unit>
```

Span exporter sketch:

```fsharp
type ChdbSpanRow = Map<string, obj>

[<RequireQualifiedAccess>]
module ChdbSpanExporter =
    val spanToRow: ReadableSpan -> ChdbSpanRow
    val make: table: string -> Effect<SpanExporter, ChdbError, Context>
```

### Migration Plan

1. Add `Interop.fs` bindings for `chdb` and OpenTelemetry types.
2. Port the `Ch` literal DSL as pure F#.
3. Port error classification before query methods.
4. Port scoped session acquisition with EffSharp resource management.
5. Port query/insert helpers.
6. Port `spanToChdbRow`, table DDL, and exporter classes.
7. Translate `ChdbClient.test.ts` into Fable-run tests.

## JS Compatibility Layer

Each package should expose F# APIs first and JS-compatible helpers second.

Examples:

- `Firegrid.Core.Exports.statePredicate`.
- `Firegrid.Runtime.Exports.defineWorkflowRuntime`.
- `Firegrid.Clients.Exports.client`, `genericCall`, `genericSend`.
- `Firegrid.Fluent.Exports.workflow`, `service`, `object`, `run`, `iface`.
- `Firegrid.Trace.Exports.ChdbClient`, `ChdbSpanExporter`.

The compatibility layer can use Fable dynamic object creation, but the domain
modules should not.

## Validation Plan

Use the old TypeScript tests as migration fixtures:

- `packages/fluent/test/public-surface.test.ts` becomes the compatibility
  contract for `Firegrid.Fluent` and `Firegrid.Clients`.
- `packages/fluent/test/state.test.ts` becomes the state table/CEL contract.
- `packages/fluent/test/externalEvents.test.ts` becomes the awakeable/workflow
  event contract.
- `packages/fluent/test/http-handler.test.ts` becomes the HTTP binding
  contract.
- `packages/trace/test/ChdbClient.test.ts` becomes the chDB contract.

Add package-local F# tests where the F# domain model matters, and JS tests where
the compatibility export shape matters.

Minimum migration gates:

1. `dotnet build Firegrid.Fable.slnx`.
2. Fable build for every `src/Firegrid.*` package.
3. Compatibility tests for the package being migrated.
4. Store/runtime proofs after `Runtime` or `Store` behavior changes.

First fluent migration gate:

1. Add a compile-only F# sample that defines:
   - one direct `service "..." { handle ... }`;
   - one contract-first `serviceContract` plus `implement`;
   - one `workflow "..." { handle ...; schedule ... }`;
   - one `durableObject "..." { handle ... }`;
   - one typed `Client.call` through a handler reference.
2. Do not port the old dynamic TypeScript client surface until that sample
   compiles and reads cleanly.

## Recommended Migration Order

1. Stabilize `Firegrid.Core` types and event wire conversion.
2. Restore `Firegrid.Runtime` contracts and schedule materialization.
3. Align `Firegrid.Store` with `Runtime.WorkflowExecutionStore`.
4. Implement `Firegrid.Clients`.
5. Implement `Firegrid.Fluent` definitions, context, clients, external events,
   state, runtime binding, and HTTP.
6. Implement `Firegrid.Trace`.
7. Remove any temporary stub exports only after tests cover the replacement.

This order keeps the substrate compileable while avoiding a second round of API
churn in the higher-level fluent package.
