# effect-s2-durable Rearchitecture Plan

This document is the implementation handoff for making
`packages/effect-s2-durable` easier to understand and maintain. It is the **single
source for the package's structural/rename plan** — `DESIGN.md` keeps the product
model + recovery framing and points here for structure (its older "Package
Structure / Coherence Plan" is superseded by this file).

It does not change the product model:

- one public runtime service: `DurableExecutionRuntime`;
- user code remains plain `Effect.gen` plus free primitives;
- object executions remain backed by S2 owner streams;
- S2 fencing remains the cross-host write-correctness mechanism;
- snapshots and trimming are the default recovery-cost strategy;
- lease / heartbeat / claim-sweep remain optional, only for prompt peer takeover.

The goal is structural: make the code read like one durable engine composed from
small internal services, not several hidden runtimes inside one large file.

## Current Problem

`src/Runtime.ts` currently has too many responsibilities:

- public runtime API definition;
- Effect layer composition;
- service submit / run / complete / recover;
- object submit / workflow start / run-head / body execution;
- shared object calls;
- child `call` / `send` routing;
- boot recovery;
- final assembly of primitive, completion, and ingress services.

That makes the central interface hard to explain. The interface is actually three
surfaces mixed together:

1. host/client lifecycle: `submit`, `attach`, `poll`, `workflowStart`;
2. primitive interpreter: `runStep`, `sleepStep`, `state*`, signals, awakeables;
3. internal dispatch: `callStep`, `sendStep`, `sharedCall`, `resolveExternal`.

The runtime service is valid as a facade, but the implementation should not live
in one closure.

## External References

Effect guidance:

- Use focused `Context.Service` definitions for modular, testable code.
- Use `Effect.fn("name")` for named functions returning effects.
- Adopt a `layerNoDeps` **naming convention** (not an Effect API): a static
  `layerNoDeps` is the `Layer.effect(...)` that still *requires* the service's deps,
  paired with a `layer` that provides them. Callers then use `Layer.provide` to hide
  internals or `Layer.provideMerge` when a dependency must stay exposed.

Reference:

- https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md
- https://github.com/Effect-TS/effect-smol/blob/main/ai-docs/src/01_effect/02_services/20_layer-composition.ts

Restate partition worker lesson:

- `partition/mod.rs` coordinates the event loop.
- `state_machine` applies durable commands and emits typed `Action`s.
- `leadership` owns actuation: invoke work, timers, shuffle/outbox, trimming,
  cleanup.
- `rpc` translates external requests into proposed commands through a narrow
  actuator interface.
- support jobs such as cleaner and storage readers live in dedicated modules.

Reference:

- https://github.com/restatedev/restate/tree/main/crates/worker/src/partition
- https://raw.githubusercontent.com/restatedev/restate/main/crates/worker/src/partition/state_machine/actions.rs
- https://raw.githubusercontent.com/restatedev/restate/main/crates/worker/src/partition/rpc/mod.rs

Do not copy Restate's partition system. This package is intentionally smaller:
S2 owner streams replace a centralized broker and partition log. The useful
lesson is the responsibility split: durable state transition, emitted actions,
actuation, and edge adapters are separate.

## Target Mental Model

The public runtime is a facade:

```ts
export class DurableExecutionRuntime extends Context.Service<
  DurableExecutionRuntime,
  DurableExecutionRuntimeApi
>()("effect-s2-durable/Runtime/DurableExecutionRuntime") {}
```

Everything behind it is internal.

The runtime answers:

> Given an ambient active invocation, how should each durable primitive or
> lifecycle operation be interpreted?

The internals answer smaller questions:

- `PrimitiveInterpreter`: what does `run`, `sleep`, `state`, or `signal` mean
  for service / object / shared invocations?
- `CompletionReader`: how does `attach` / `poll` find a result?
- `IngressRouter`: how does an external signal / awakeable resolution route?
- `ServiceExecutor`: how do plain service executions submit, run, complete, and
  recover?
- `ObjectExecutor`: how do object calls submit, start workflows, resolve
  handlers, and run one accepted head?
- `ChildCallRouter`: how does one active invocation issue a deterministic child
  object call?
- `SharedObjectRunner`: how does a read-only shared handler run over a snapshot?
- `BootRecovery`: what work is re-driven when the runtime layer starts?
- `RuntimeKernel`: how are these services wired into the one public facade?

## Target Directory Shape

Destination shape:

```text
src/
  Runtime.ts
  primitives.ts
  service.ts
  handler.ts
  types.ts
  schema.ts
  errors.ts

  runtime/
    invocation.ts
    state.ts
    durable-stores.ts
    primitive-interpreter.ts
    completion-reader.ts
    ingress-router.ts
    service-executor.ts
    object-executor.ts
    child-call-router.ts
    shared-object-runner.ts
    boot-recovery.ts
    actions.ts

  object/
    events.ts
    log.ts
    drive-session.ts
    invocation-store.ts
    snapshots.ts

  ingress/
    server.ts
    client.ts
    contract.ts

  host.ts
  bin/
    host.ts
```

Near-term rename map from current files:

```text
src/runtime/stores.ts       -> src/runtime/durable-stores.ts
src/runtime/primitives.ts   -> src/runtime/primitive-interpreter.ts
src/runtime/completion.ts   -> src/runtime/completion-reader.ts
src/runtime/ingress.ts      -> src/runtime/ingress-router.ts
src/actor/core.ts           -> src/object/events.ts
src/actor/log.ts            -> src/object/log.ts
src/actor/drive-session.ts  -> src/object/drive-session.ts
src/actor/object.ts         -> src/object/invocation-store.ts
```

These renames should be behavior-preserving and done before deeper extraction.

## Public Runtime Interface

Keep the external API stable, but internally treat it as three groups.

Lifecycle:

```ts
interface RuntimeLifecycleApi {
  readonly submit: DurableExecutionRuntimeApi["submit"]
  readonly attach: DurableExecutionRuntimeApi["attach"]
  readonly poll: DurableExecutionRuntimeApi["poll"]
  readonly workflowStart: DurableExecutionRuntimeApi["workflowStart"]
}
```

Primitives:

```ts
interface RuntimePrimitiveApi {
  readonly runStep: PrimitiveInterpreterApi["runStep"]
  readonly handlerRequest: PrimitiveInterpreterApi["handlerRequest"]
  readonly sleepStep: PrimitiveInterpreterApi["sleepStep"]
  readonly stateGet: PrimitiveInterpreterApi["stateGet"]
  readonly stateSet: PrimitiveInterpreterApi["stateSet"]
  readonly stateDelete: PrimitiveInterpreterApi["stateDelete"]
  readonly awaitDeferred: PrimitiveInterpreterApi["awaitDeferred"]
  readonly resolveLocal: PrimitiveInterpreterApi["resolveLocal"]
  readonly resolvePromise: PrimitiveInterpreterApi["resolvePromise"]
  readonly nextAwakeableId: PrimitiveInterpreterApi["nextAwakeableId"]
}
```

Dispatch:

```ts
interface RuntimeDispatchApi {
  readonly resolveExternal: IngressRouterApi["resolveExternal"]
  readonly sharedCall: SharedObjectRunnerApi["sharedCall"]
  readonly callStep: ChildCallRouterApi["callStep"]
  readonly sendStep: ChildCallRouterApi["sendStep"]
}
```

`DurableExecutionRuntimeApi` can remain the intersection for compatibility. The
implementation should assemble it from internal services rather than implement
everything inline.

## Effect Service Pattern

Each internal service should follow this shape:

```ts
export class ServiceExecutor extends Context.Service<ServiceExecutor, {
  readonly submitService: (...) => Effect.Effect<void, DurableExecutionError, R>
  readonly recoverServices: Effect.Effect<void>
}>()("effect-s2-durable/runtime/ServiceExecutor") {
  static readonly layerNoDeps = Layer.effect(
    ServiceExecutor,
    Effect.gen(function*() {
      const stores = yield* DurableStores
      const state = yield* RuntimeState

      const submitService = Effect.fn("ServiceExecutor.submitService")(function*(...) {
        // ...
      })

      return ServiceExecutor.of({ submitService, recoverServices })
    }),
  )

  static readonly layer = this.layerNoDeps.pipe(
    Layer.provideMerge(RuntimeBase.layer),
  )
}
```

Guidelines:

- Use `Effect.fn("ServiceName.method")` for exported or service methods.
- Use `Layer.effect` for service construction.
- Use `Layer.scoped` only when the service acquires resources requiring release.
- Use `Layer.effectDiscard` for background jobs that expose no API.
- Use `Layer.provide` to hide implementation services.
- Use `Layer.provideMerge` when the caller still needs the provided dependency.
- Keep one service method focused on one workflow; avoid nested local functions
  that become untestable mini-services.

## Runtime Layer Composition

The final runtime layer should read as composition, not implementation.

Target:

```ts
const RuntimeBaseLive = Layer.mergeAll(
  RuntimeState.layer(handlers, objectSeeds),
  DurableStores.layer,
)

const RuntimeInternalLive = Layer.mergeAll(
  PrimitiveInterpreter.layerNoDeps,
  CompletionReader.layerNoDeps,
  IngressRouter.layerNoDeps,
  ServiceExecutor.layerNoDeps,
  ObjectExecutor.layerNoDeps,
  ChildCallRouter.layerNoDeps,
  SharedObjectRunner.layerNoDeps,
  BootRecovery.layerNoDeps,
).pipe(
  Layer.provideMerge(RuntimeBaseLive),
)

export const DurableExecutionRuntimeLive =
  DurableExecutionRuntime.layerNoDeps.pipe(
    Layer.provide(RuntimeInternalLive),
  )
```

In practice there is a recursive knot: handlers need
`DurableExecutionRuntime`, while the runtime needs executors that run handlers.
Do not force a fake acyclic graph. Keep the knot in one small `RuntimeKernel`
module:

```ts
export class RuntimeKernel extends Context.Service<RuntimeKernel, {
  readonly makeApi: Effect.Effect<DurableExecutionRuntimeApi>
}>()("effect-s2-durable/runtime/RuntimeKernel") {}
```

`RuntimeKernel` may assemble the final API object and pass it to executor
factories that need to inject `DurableExecutionRuntime` into handlers. The
important constraint is that handler-running code moves out of `Runtime.ts`.

## Restate-Inspired Action Boundary (NOT part of this refactor)

The Restate lesson worth citing is the boundary between state-machine application
and side effects — but **this package already has it**: the durable state
transition is the pure fold in `object/events.ts` (`replay`/`transition`, no S2
imports), and actuation (forking a drain, signal pokes) lives in the drainer.

A `RuntimeAction` ADT + `RuntimeActionRunner` was considered to make actuation
explicit. It is **deliberately out of scope**: there is no current dispatch-
complexity pain driving it, and today's actuation is trivial (`admitObject` forks a
drain; a local waiter poke). Adding an action vocabulary + runner now would be
speculative indirection — the same kind we have been removing (per-append OCC,
lease). Revisit only if dispatch logic genuinely grows; if so, design it as its own
service without leaking into primitive interpretation or handler state backends.

## Service Responsibilities

### `PrimitiveInterpreter`

Owns all `ActiveInvocation.kind` branching for durable primitives.

Allowed dependencies:

- `ActiveInvocation`;
- `DurableStores`;
- `RuntimeState` for local waiters / counters;
- object state backend through `ObjectInvocation`.

Not allowed:

- service handler forking;
- object owner-drain logic;
- HTTP/host concerns.

### `CompletionReader`

Owns `attach` and `poll`.

It should know how to read:

- service roster entries;
- local service deferred completions;
- object call status by self-routing object call id.

It should not submit work, recover work, or run handlers.

### `IngressRouter`

Owns external resolution routing:

- service signal / awakeable resolution;
- object signal resolution by object call id;
- error on unknown or invalid addresses.

It should append ingress facts; it should not own object draining.

### `ServiceExecutor`

Owns service lifecycle:

- service `submit`;
- `runServiceExecution`;
- `completeServiceExecution`;
- service boot recovery.

This is the temporary legacy path while service executions still use
`WorkflowDb` plus roster.

### `ObjectExecutor`

Owns object lifecycle above `InvocationStore`:

- object branch of `submit`;
- `workflowStart`;
- handler lookup by object/method;
- `runObjectBody`;
- `makeRunHead`;
- object boot recovery.

`InvocationStore` should remain the owner-stream store. `ObjectExecutor` should
bridge from runtime handler registry to store drain functions.

### `ChildCallRouter`

Owns deterministic child call ids:

- reject shared-handler child calls;
- reject same-object/key self-calls;
- derive nonce from parent id and ordinal;
- admit child object call;
- fork the owner drain;
- for `call`, attach to result;
- for `send`, return call id.

### `SharedObjectRunner`

Owns read-only shared handler execution:

- fold object snapshot;
- inject `ActiveInvocation.kind === "shared"`;
- encode/decode input/output;
- reject failures as `DurableExecutionError`.

### `BootRecovery`

Owns layer-start recovery orchestration:

- service roster recovery for legacy service path;
- object owner-key enumeration and drain restart;
- future snapshot-aware object recovery;
- future timer re-arm.

This may be `Layer.effectDiscard` if it only runs on startup, or a service if
tests need to call it directly.

## Object Package Responsibilities

### `object/events.ts`

Pure event vocabulary and projection:

- `ActorEvent`;
- `ActorExit`;
- `ActorSnapshot`;
- `transition`;
- `replay`;
- status helpers;
- call id encode/decode;
- path segment encode/decode.

No S2 imports. No runtime imports. No handler imports.

### `object/log.ts`

Mechanical S2 owner-log IO:

- read decoded events;
- append ingress event;
- conditional admission append;
- tail reads.

No handler lookup. No runtime scope. No business decisions beyond mapping S2
errors to durable errors.

### `object/drive-session.ts`

S2 ownership boundary:

- ensure stream visible;
- claim fence;
- append owner-driver events with `fencingToken`;
- classify lost fence as `FenceLost`;
- optionally wrap append-session / producer later.

No handler code. No state machine fold.

### `object/invocation-store.ts`

Owner-stream behavior:

- admit call;
- drain owner FIFO;
- status by call id;
- resolve signal ingress;
- owner key listing;
- read snapshot.

It may use local locks, started sets, projection caches, and owner drive
sessions. It should not know the runtime's handler registry. It receives a
`RunHead` function from `ObjectExecutor`.

### `object/snapshots.ts`

Future recovery-cost boundary:

- write projection snapshot at a cursor;
- load snapshot and read tail;
- trim records already covered by the snapshot.

This should arrive before lease/heartbeat work.

## Error Boundary Rules

Use tagged errors for expected domain failures.

Important distinction:

- user handler failure: can become durable `Completed(Failure)`;
- runtime/storage infrastructure failure: must not become a durable user result;
- lost fence: stops this drainer and leaves another owner/retry path to continue.

Current rule:

- `object.*` infrastructure failures escape object body execution and prevent
  appending `Completed`;
- wrapped `FenceLost` is unwrapped at the owner-driver boundary;
- normal guardrail failures such as unsupported self-call can still become
  user-visible durable failures.

Keep this distinction explicit when extracting `ObjectExecutor`.

## Recovery Direction

Default recovery should be:

1. S2 owner stream is the source of truth.
2. Object projection is rebuilt from snapshot plus tail.
3. Trimming compacts history already covered by snapshots.
4. Restart-based boot recovery re-drives pending heads.
5. Fencing protects against stale cross-host writers.

Lease, heartbeat, and claim-sweep are not the default next step. They are only
for prompt peer takeover without waiting for process restart.

If prompt peer takeover is later required, design it as a separate
`PeerTakeover` or `OwnerClaimSweeper` service. Do not let it leak into primitive
interpretation or handler state backends.

## Migration Plan

Keep every step behavior-preserving unless explicitly noted.

**Status:** Step 1 (renames) is done. Steps 0 + 2–8 (executor extraction) are
intentionally deferred until object snapshots and the service-path unification land —
both will move this exact code, so extracting first risks redoing it. Do the renames
now for immediate clarity; sequence the extractions after the engine's shape settles.

### Step 0: Resolve the handler/runtime knot FIRST (prerequisite for extraction)

Every executor in Steps 3–5 (`callStep`, `sharedCall`, `runObjectBody`) injects the
runtime API into handlers via `provideService(DurableExecutionRuntime, api)` — and
`api` is assembled *from* those executors. So the extraction is blocked on a way for
executors to receive the assembled API. Decide and land this mechanism (a `Deferred`/
lazy ref, or a `RuntimeKernel.makeApi`) and prove it with one trivial extraction
*before* moving any handler-running code. Do NOT start Step 2 until this exists; the
"low-risk" shared-call extraction is exactly where the knot first bites.

This step is sequenced after Step 1 (renames) but before any executor extraction.

### Step 1: Rename for vocabulary — DONE

Moved current files to target concept names (behavior-preserving, import-only):

- `runtime/primitives.ts` -> `runtime/primitive-interpreter.ts`;
- `runtime/stores.ts` -> `runtime/durable-stores.ts`;
- `runtime/completion.ts` -> `runtime/completion-reader.ts`;
- `runtime/ingress.ts` -> `runtime/ingress-router.ts`;
- `actor/core.ts` -> `object/events.ts`;
- `actor/log.ts` -> `object/log.ts`;
- `actor/drive-session.ts` -> `object/drive-session.ts`;
- `actor/object.ts` -> `object/invocation-store.ts`.

Validation:

- package typecheck;
- package lint;
- durable tests.

### Step 2: Extract `SharedObjectRunner`

Move only `sharedCall` out of `Runtime.ts`.

This is low risk because shared calls are read-only and mostly self-contained.

### Step 3: Extract `ChildCallRouter`

Move `issueCall`, `callStep`, and `sendStep`.

Inputs:

- `ActiveInvocation`;
- `RuntimeState.engineScope`;
- `ObjectExecutor` or `InvocationStore` drain API;
- `CompletionReader`;
- `DurableStores.provideClient`.

### Step 4: Extract `ServiceExecutor`

Move:

- `complete`;
- `runExecution`;
- service branch of `submit`;
- `recoverExecution`;
- service boot recovery query.

This isolates the legacy roster / workflow DB path.

### Step 5: Extract `ObjectExecutor`

Move:

- `runObjectBody`;
- `makeRunHead`;
- `admitObject`;
- object branch of `submit`;
- `workflowStart`;
- object boot recovery.

Keep `InvocationStore` unchanged except for import paths.

### Step 6: Introduce `BootRecovery`

Move startup recovery orchestration out of `Runtime.ts`:

- call service recovery;
- call object recovery;
- future timer re-arm;
- future snapshot-aware recovery.

`Runtime.ts` should call one startup effect or depend on a
`Layer.effectDiscard` that runs it.

### Step 7: Make `Runtime.ts` a facade

After extraction, `Runtime.ts` should mostly:

- define `DurableExecutionRuntimeApi`;
- define `DurableExecutionRuntime`;
- compose base and internal layers;
- assemble the final API object from internal services;
- run boot recovery.

It should not contain handler lifecycle algorithms.

### Step 8: Add object snapshots

After structure is clear:

- add `object/snapshots.ts`;
- teach `InvocationStore.readSnapshot` and boot recovery to load snapshot plus
  tail;
- add trim after snapshot correctness is tested.

## Testing Strategy

Each extraction should preserve existing tests.

Add focused tests as modules split:

- `PrimitiveInterpreter`: service/object/shared primitive behavior by injecting
  fake active invocations.
- `CompletionReader`: service roster result, object result, unknown id.
- `ChildCallRouter`: deterministic id, self-call rejection, shared-call
  rejection, call-vs-send.
- `ServiceExecutor`: idempotent submit, completion, recovery.
- `ObjectExecutor`: object submit, workflow start run-once, infrastructure error
  not committed as `Completed`.
- `InvocationStore`: admission CAS, drain ordering, fenced handoff, signal
  ingress.
- `BootRecovery`: service and object pending work re-driven.

Keep S2-lite integration tests for cross-module confidence. Use smaller unit
tests with fake services for extracted modules where practical.

## Non-Goals

- Do not expose internal services from the package public API.
- Do not introduce a second public runtime.
- Do not copy Restate's partition leadership implementation.
- Do not build lease/heartbeat/claim-sweep as part of this refactor.
- Do not move services onto object streams in the same change as file/module
  extraction.
- Do not replace the public free primitives.

## Acceptance Criteria

The rearchitecture is successful when:

- `Runtime.ts` reads as layer composition plus facade assembly rather than handler
  lifecycle algorithms (a rough ~150-line target, not a hard gate — the handler/
  runtime knot + facade assembly may legitimately need more);
- `ActiveInvocation.kind` branching is concentrated in
  `PrimitiveInterpreter`, `ChildCallRouter`, and explicitly named execution
  modules;
- object S2 fencing lives only in `object/drive-session.ts`;
- object owner-stream behavior lives only in `object/invocation-store.ts`;
- service legacy storage is isolated in `ServiceExecutor`;
- boot recovery is a named module;
- package tests and typecheck remain green after each step;
- the public API remains source-compatible.

