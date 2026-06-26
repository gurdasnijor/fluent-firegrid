# SDD: F# / Fable vs EffSharp Evaluation

### Whether the migrated SDK should be EffSharp-first or F#-native

|   |   |
| --- | --- |
| Status | Draft recommendation |
| Date | 2026-06-26 |
| Branch focus | `pr-72-migrate-fable`, PR 72 |
| Scope | Public SDK shape, runtime implementation strategy, package-by-package EffSharp use |

---

## Decision

Do not make EffSharp the default public API model for the F#/Fable rewrite.

Implement Fluent Firegrid primarily as an F# / Fable SDK:

- public authoring APIs should use F# records, discriminated unions,
  computation expressions, modules, `Option`, `Result`, `Async`, `task`, and
  Fable `Promise` interop;
- the durable workflow surface should use a Firegrid-specific computation
  expression, not raw `Effect<'A, 'E, 'R>`;
- generated JavaScript exports should expose Promise-returning APIs and
  compatibility facades;
- EffSharp should be used only where it clearly improves an implementation
  boundary: infrastructure service composition, scoped stream/session handling,
  typed dependency injection, typed error channels, or Effect-compatible stream
  processing.

In short: F# should be the SDK language. EffSharp can be a substrate tool.

## Why This Needs Reassessment

The TypeScript implementation needed Effect because TypeScript does not provide
language-level equivalents for many of the shapes Fluent Firegrid wants:

- algebraic domain modeling;
- lightweight typed errors;
- expression-oriented async composition;
- ergonomic custom syntax for domain-specific workflows;
- a conventional way to separate description, dependency requirements, and
  execution.

F# already covers much of that directly. If the migration simply replaces every
TypeScript `Effect` with EffSharp `Effect<'A, 'E, 'R>`, the F# API inherits
Effect's ceremony while giving up F#'s strongest ergonomics.

That shows up in sketches like this:

```fsharp
service "incident" {
    handle triage (fun input ->
        effect {
            do! Run.step "reserve" (fun _ -> reserveIncident input.IncidentId)
            return { Accepted = true }
        })
}
```

The problem is not just syntax. The author is being asked to understand
EffSharp as the main programming model before they understand the Firegrid
model. In F#, the better shape is to make the Firegrid workflow computation the
thing the user sees:

```fsharp
let triage = handler<TriageInput, TriageOutput> "triage"

let incident =
    service "incident" {
        handle triage (fun input ->
            workflow {
                do! step "reserve" (fun () -> reserveIncident input.IncidentId)
                return { Accepted = true }
            })
    }
```

The implementation behind `workflow { ... }` can use EffSharp if that is useful,
but the user-facing abstraction should be the durable Firegrid program, not the
generic Effect program.

## What F# / Fable Already Provides

### Domain Modeling

F# records and discriminated unions are a better fit than TypeScript object
types plus schema wrappers for internal SDK concepts:

```fsharp
type RunStatus =
    | Pending
    | Running
    | Completed
    | Failed of SerializedError
    | Cancelled

type DurableOutcome<'T> =
    | Done of 'T
    | Parked of ParkReason
    | Failed of FluentFiregridError
```

This removes a major reason to reach for Effect-style tagged errors everywhere.
Expected domain outcomes can be ordinary union cases, and callers handle them
with pattern matching.

### Error Modeling

F# already has `Result<'T, 'Error>` for expected failures:

```fsharp
let decodeInput<'T> (codec: Codec<'T>) (value: obj) : Result<'T, CodecError> =
    codec.Decode value
```

Use `Result` when a function is pure or validation-oriented. Use exceptions only
for defects or JS interop failures that cannot be represented at the boundary.
Use EffSharp's typed error channel only when the function also benefits from
Effect composition or requirements.

### Async

F# has `Async<'T>` and task computation expressions, and Fable adds direct
Promise interop. That is enough for most SDK and client APIs:

```fsharp
type InvocationBinding =
    { Send: InvocationRequest -> JS.Promise<InvocationId>
      Call: InvocationRequest -> JS.Promise<InvocationResult> }

module Client =
    let call handler input : JS.Promise<'Output> =
        promise {
            let! raw = binding.Call(toRequest handler input)
            return decodeOutput handler raw
        }
```

For Fable-generated packages, Promise-returning APIs are also the least
surprising JavaScript surface.

### Custom Syntax

F# computation expressions are the native way to design domain-specific
workflows:

```fsharp
workflow {
    let! order = step "load-order" (fun () -> loadOrder id)
    do! sleep (Duration.minutes 5)
    let! decision = waitFor "approval" Approval.codec
    return decide order decision
}
```

That is the right place to encode Firegrid semantics: journal lookup, step
recording, park/resume, deterministic replay, durable wait registration, and
runtime control outcomes.

### Dependencies

Simple dependencies can be explicit records or parameters:

```fsharp
type HandlerEnv =
    { Clock: Clock
      Clients: ClientRegistry
      Journal: Journal }
```

For most SDK users this is clearer than an ambient typed requirement channel.
EffSharp requirements become useful only when there are many independently
composable infrastructure services.

## What EffSharp Buys

EffSharp still has real value, but its value is narrower than "all public APIs
return Effect".

### Typed Requirements

The S2 wrapper currently uses a typed service tag:

```fsharp
let tag: Tag<S2Client> = Tag.make<S2Client> "@firegrid/log/S2Client"
let service<'E> : Effect<S2Client, 'E, Context> = Effect.service tag
```

That is valuable for infrastructure-heavy code where operations need a client
but should not manually thread it through every function.

### Layer Composition

`Firegrid.Store.Runtime` already builds an S2 runtime with a layer:

```fsharp
{ Basin = ...
  Namespace = ...
  Layer = S2.layer s2Config }
```

That is useful at host composition boundaries. It is less useful in SDK
definition code, where explicit records are usually clearer.

### Promise Interop With Typed Errors

The S2 package wraps JS SDK promises into typed errors:

```fsharp
let private tryPromise promise map =
    Effect.tryPromiseJS promise toError |> Effect.map map
```

This is a good EffSharp use. It normalizes a flaky JS/HTTP boundary and keeps
the error type explicit.

### Streams

The S2 read-session wrapper uses `Effect.Stream`:

```fsharp
Effect.Stream.fromAsyncIterableJS iterable toError
```

This is also a strong fit. Async iterables, cancellation, backpressure, and
typed failure are exactly the kind of implementation boundary where a structured
effect stream can help.

### Shared Semantics With Existing Effect Designs

The current architecture docs describe Effect as owning local computation:
fibers, scopes, interruption, layers, schemas, clocks, and randomness. If
EffSharp faithfully supports those semantics under Fable, it can reduce the gap
from the TypeScript proof design.

That is a portability benefit, not a reason to expose EffSharp everywhere.

## What EffSharp Costs

### It Is Not Idiomatic F# As The First Surface

F# users expect records, unions, functions, modules, CEs, `Result`, `Async`,
`task`, and `Promise`. A public API dominated by `Effect<'A, 'E, 'R>` makes the
SDK feel like a port of a TypeScript library rather than an F# SDK.

### It Adds A Second Abstraction Over Features F# Already Has

If EffSharp is used for every handler, every client call, and every validation
path, it duplicates:

- `Result` for expected errors;
- `Async` / `task` / `Promise` for asynchronous work;
- computation expressions for sequencing;
- records/parameters for dependencies;
- DUs for domain outcomes.

Duplication makes the API harder to explain and harder to keep ergonomic.

### It Can Obscure Firegrid's Own Control Flow

Durable park/resume is not an application error. It is host control flow:

```text
first drive -> append intent -> park
wake drive  -> append resolution -> replay through journal hit
```

Encoding that primarily as generic Effect errors, defects, or interruption
risks leaking implementation details into user code. A Firegrid-specific
workflow CE can keep that control channel private.

### It Raises Fable Compatibility Risk

Fable supports a broad F# subset, but generated JavaScript still has practical
constraints around reflection, runtime metadata, prototype expectations, and JS
interop shape. A small F#-native SDK plus Promise exports is easier to inspect
and support than exposing a large effect runtime through the public JavaScript
API.

### It Makes Type Signatures Noisier

Compare the SDK mental model:

```fsharp
Handler<'Input, 'Output>
```

against:

```fsharp
'Input -> Effect<'Output, FluentFiregridError, Context>
```

The second type may be appropriate internally. As an authoring API, it forces
the user to think about the effect runtime before they can understand handlers.

## Package-by-Package Recommendation

### `Firegrid.Core`

Recommendation: F# native, no EffSharp dependency by default.

`Core` should define durable domain types, errors, identifiers, invocation
metadata, event envelopes, registry data, and state predicates. These are mostly
records, unions, options, and pure functions.

EffSharp does not buy much here. Keeping `Core` effect-free lowers dependency
weight for every package above it.

Use:

- records and single-case wrappers for IDs where useful;
- discriminated unions for events, statuses, outcomes, and errors;
- `Result` for validation/parsing helpers;
- Fable-friendly `Exports` modules for JS object shapes.

Avoid:

- `Effect` in core type definitions;
- ambient service requirements;
- an EffSharp package reference unless a concrete helper needs it.

### `Firegrid.Fluent`

Recommendation: F# native public API with a Firegrid-specific workflow CE.
EffSharp may be an internal backend, but should not be the authoring surface.

This is the package where ergonomics matter most. The API should center on
contracts, typed handler references, service/workflow/object builders, and a
durable workflow computation expression:

```fsharp
let approve = handler<ApprovalInput, ApprovalOutput> "approve"

let approvals =
    service "approvals" {
        handle approve (fun input ->
            workflow {
                let! existing = state.tryGet ApprovalState.codec input.Id
                match existing with
                | Some value -> return { Accepted = value.Accepted }
                | None ->
                    let! result = step "reserve" (fun () -> reserve input.Id)
                    do! state.set ApprovalState.codec input.Id result
                    return { Accepted = true }
            })
    }
```

The CE should expose Firegrid concepts directly:

- `step`
- `sleep`
- `sleepUntil`
- `waitFor`
- `call`
- `send`
- `state.get` / `state.set`
- `workflowPromise.await` / `resolve`, if retained

Internally, the CE can lower to an AST, an interpreter, EffSharp, or a hybrid.
That implementation choice should not leak into normal handler signatures.

### `Firegrid.Clients`

Recommendation: F# native public API plus Promise JS exports. Avoid EffSharp in
the main client surface.

Client calls are transport requests. The ergonomic API should be:

```fsharp
Client.call approvals approve { Id = "inc-1" }
Client.send approvals approve { Id = "inc-1" }
```

with typed handler references carrying input/output information.

Internally this can use `Async` or `JS.Promise`. For Fable output, Promise is the
right JS-facing shape. Add EffSharp wrappers only as optional helpers if
EffSharp-using applications want to compose calls inside an EffSharp program.

### `Firegrid.Runtime`

Recommendation: mixed. Keep public runtime contracts F# native; use EffSharp
selectively inside host composition.

Runtime has two different responsibilities:

- domain runtime semantics: drive, replay, park, append facts, materialize
  state;
- infrastructure composition: journal store, wake sources, clocks, random,
  clients, tracing, host resources.

The domain semantics should be explicit F#:

```fsharp
type DriveResult<'T> =
    | Completed of 'T
    | Parked of ParkReason
    | Failed of FluentFiregridError

type WorkflowExecutionStore =
    { AppendEvents: AppendEventsArgs -> JS.Promise<Result<AppendEventsResult, StoreError>>
      ReadEvents: ReadEventsArgs -> JS.Promise<Result<StoredWorkflowEvent list, StoreError>> }
```

EffSharp may help in the host interpreter if the implementation needs layered
services, typed error composition across multiple stores, scoped resources, or
Effect-compatible streams. It should not be the only way to implement a store.

### `Firegrid.Log`

Recommendation: EffSharp is currently justified internally, but add F# native
wrappers if this package is consumed directly.

This package is the strongest current EffSharp fit. It wraps a JS S2 SDK,
normalizes errors, provides a client layer, and converts async iterables into
streams. Those are real Effect-shaped problems.

Keep EffSharp here if:

- `Effect.Stream` behaves well under Fable;
- layer provisioning remains small and predictable;
- error conversion stays centralized;
- JS exports hide EffSharp behind Promise-returning functions.

Consider adding parallel Promise APIs:

```fsharp
S2Promise.append request : JS.Promise<S2AppendAck>
S2Promise.read request : JS.Promise<S2ReadBatch>
```

so SDK consumers are not forced into EffSharp for simple log operations.

### `Firegrid.Store`

Recommendation: mixed, leaning EffSharp internally because it composes with
`Firegrid.Log`, but expose store interfaces that can be implemented without
EffSharp.

The existing store functions are thin compositions over S2 effects. That is a
reasonable internal implementation. The runtime-facing store contract should
not require EffSharp unless the runtime itself chooses that mode.

Prefer:

```fsharp
type WorkflowExecutionStore =
    { AppendEvents: AppendEventsArgs -> JS.Promise<Result<AppendEventsResult, StoreError>>
      ReadEvents: ReadEventsArgs -> JS.Promise<Result<StoredWorkflowEvent list, StoreError>> }
```

Then provide adapters:

```fsharp
module WorkflowExecutionStore =
    val fromEffSharp :
        EffStore -> WorkflowExecutionStore

    val toEffSharp :
        WorkflowExecutionStore -> EffStore
```

This keeps S2-backed internals efficient without forcing every store
implementation to depend on EffSharp.

### `Firegrid.Trace`

Recommendation: F# native first. Use EffSharp only around streaming/resource
exports if needed.

Trace exporters are mostly data transformation and transport. Use records,
unions, pure encoders, and Promise-returning export functions. EffSharp is worth
adding only if trace export becomes a scoped stream pipeline with retries,
interruptibility, or layered sinks.

## Decision Matrix

| Concern | F# / Fable native | EffSharp | Recommendation |
| --- | --- | --- | --- |
| Domain types | Excellent: records, DUs, options | Adequate, but not needed | F# |
| Expected validation errors | Excellent: `Result` | Good typed error channel | F# unless already in an Effect flow |
| Handler authoring syntax | Excellent with custom CE | Usable, but generic | F# workflow CE |
| Durable park/resume | Needs Firegrid-specific CE/interpreter | Generic Effect does not solve durability by itself | Firegrid CE/interpreter |
| JS client APIs | Excellent with Promise exports | Awkward if exposed directly | F# / Promise |
| Dependency injection | Good with records/params | Strong with tags/layers | F# for simple deps, EffSharp for infra |
| Scoped resources | Good for ordinary `use`; limited for async streams | Strong if scopes/streams work under Fable | EffSharp for S2 sessions/streams |
| Async streams | JS async iterables available; custom work needed | Strong stream abstraction | EffSharp internally |
| Local structured concurrency | F# async/task covers basics | Stronger if fibers/scopes/interruption are complete | Evaluate per runtime need |
| Fable output predictability | Best when using simple F# + Promise | Higher runtime surface area | F# public surface |
| Learning curve | Natural for F# users | Requires Effect knowledge | F# public surface |

## Recommended Architecture

Use four layers:

1. **Domain layer:** F# records, unions, codecs, pure functions, `Result`.
2. **Authoring layer:** Firegrid-specific computation expressions and typed
   definition builders.
3. **Runtime layer:** interprets Firegrid workflow programs; may have EffSharp
   adapters internally.
4. **Interop layer:** Fable exports, JS object facades, Promise APIs, optional
   EffSharp helpers for EffSharp consumers.

The important boundary is between layer 2 and layer 3. Handler authors should
write Firegrid workflows. Runtime maintainers can decide whether the interpreter
uses EffSharp, direct Promises, or a small custom continuation model.

## API Direction

### Definition API

Prefer typed references and builder CEs:

```fsharp
module IncidentApi =
    type TriageInput = { IncidentId: string }
    type TriageOutput = { Accepted: bool }

    let triage = handler<TriageInput, TriageOutput> "triage"

    let contract =
        serviceContract "incident" {
            endpoint triage
        }
```

### Implementation API

The implementation should expose Firegrid concepts:

```fsharp
let incident =
    implement IncidentApi.contract {
        handle IncidentApi.triage (fun input ->
            workflow {
                do! step "reserve" (fun () -> reserveIncident input.IncidentId)
                return { Accepted = true }
            })
    }
```

### Client API

Clients should not expose Effect requirements:

```fsharp
let! result =
    IncidentApi.contract
    |> Client.call IncidentApi.triage { IncidentId = "inc-1" }
```

For Fable exports, this becomes a Promise-returning JS API.

### Optional EffSharp API

If needed, provide explicit adapters instead of making them primary:

```fsharp
module EffSharp =
    val runWorkflow :
        WorkflowProgram<'A> -> Effect<'A, FluentFiregridError, RuntimeContext>

    val clientCall :
        HandlerRef<'I, 'O> -> 'I -> Effect<'O, FluentFiregridError, InvocationBinding>
```

The module name makes the dependency visible and optional.

## Migration Consequences

This changes the PR 72 migration plan:

- Do not port package signatures one-for-one from TypeScript Effect to
  EffSharp.
- Remove EffSharp package references from `Core`, and likely from `Clients` and
  `Trace`, unless a concrete implementation needs them.
- Design `Fluent` around Firegrid-specific computation expressions before
  implementing runtime lowering.
- Keep `Log` and `Store` EffSharp internals for now because they already have
  concrete S2 usage that benefits from typed services and streams.
- Add Promise/F# native wrappers around EffSharp infrastructure packages where
  they are public SDK entrypoints.
- Treat EffSharp as an implementation dependency with adapters, not as the
  language of the SDK.

## Validation Gates

Before committing to EffSharp in any public package, require these checks:

1. A compile-only ergonomic sample in F# showing the API without type
   annotations at every call site.
2. A generated JavaScript sample showing a Promise-returning API that is natural
   for TypeScript consumers.
3. A small implementation proving the same API can run without EffSharp, or a
   documented reason why EffSharp is necessary.
4. A Fable build proving the chosen EffSharp features compile to usable JS.
5. A package-level decision recording whether EffSharp is public, internal, or
   absent.

## Final Recommendation

Build Fluent Firegrid directly in F# / Fable first.

Keep EffSharp where it demonstrably improves the implementation, especially S2
client layers, scoped stream/session work, and possibly runtime host
composition. Do not make EffSharp the public SDK model for definitions, clients,
or ordinary handlers.

The ideal outcome is an SDK that feels like idiomatic F# and exports idiomatic
JavaScript, while still allowing the runtime to use EffSharp internally where
the abstraction pays for itself.

## References

- F# async and task overview:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/tutorials/async>
- F# computation expressions:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/computation-expressions>
- F# discriminated unions:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/discriminated-unions>
- F# Result:
  <https://learn.microsoft.com/en-us/dotnet/fsharp/language-reference/results>
- Fable compatibility notes:
  <https://fable.io/docs/javascript/compatibility.html>
- Fable Promise reference:
  <https://fable.io/fable-promise/reference/Fable.Promise/global-promise.html>
