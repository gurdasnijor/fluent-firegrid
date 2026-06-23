# Migration And Testing

Keep every step behavior-preserving unless explicitly noted.

## Current Status

The old `actor/*` files have already been renamed to object concept names:

- `actor/core.ts` -> `object/machine/model.ts`;
- `actor/log.ts` -> `object/log.ts`;
- `actor/drive-session.ts` -> `object/drive-session.ts`;
- `actor/object.ts` -> `object/owner-driver.ts`.

The old `runtime/*` catch-all directory has also been removed. Engine internals
are now consolidated under `engine/` to avoid one-file top-level folders:

- `engine/state.ts`;
- `engine/helpers.ts`;
- `engine/context.ts`;
- `engine/address.ts`;
- `engine/handler-primitives.ts`;
- `engine/result-reader.ts`;
- `engine/resolution-router.ts`;
- `engine/service-deferreds.ts`;
- `engine/durable-stores.ts`.

## Ordered Plan

### Machine Step A: Extract `object/machine/`

Do this before more service extraction.

Move pure owner decisions out of `object/owner-driver.ts`:

- admission result from current projection + incoming call;
- status lookup from current projection;
- next-head selection from current projection;
- whether a call is already started/completed/pending;
- translation of state/journal/signal/completion intents into `ActorEvent`;
- parked/no-op/run decisions.

Validation:

- pure unit tests for admission idempotency, FIFO head selection, completion
  status, signal resolution, and state/journal event emission;
- no S2-lite required.

Current baseline: the machine is co-located under `object/machine/`, with
durable schemas/projection in `model.ts` and command processing in `commands.ts`.

### Machine Step B: Turn `ObjectOwnerDriver` into a driver

After Machine Step A, simplify `object/owner-driver.ts` so it mostly:

- reads/folds the owner stream;
- calls `ObjectStateMachine.decide(...)`;
- appends emitted events through `object/log.ts` or `OwnerDriveSession`;
- runs emitted actions such as `RunHead`;
- maintains local-only concerns: lock, waiters, started cache, projection cache.

Validation:

- existing object and ingress tests remain green;
- dependency graph shows `object/machine/` has no dependency on S2,
  Effect services, or engine modules.

### Machine Step C: Reuse the shape for service unification

Do not extract a generic cross-engine machine yet. First prove the object
machine. When service-path unification starts, model service/workflow execution
with the same shape:

```text
Command + Projection -> Decision -> Events + DriverActions
```

### Substrate Step A: Add `EventStream` to `effect-s2-stream-db`

Add a generic typed stream abstraction beside `Table` / `StreamDb`.

Validation:

- stream-db unit tests for event encode/decode and path derivation;
- S2-lite specs for append/read/tail, guarded append, snapshot+tail recovery, and
  trim.

### Substrate Step B: Move object owner logs onto `EventStream<ActorEvent>`

Change `object/log.ts` to be a thin durable adapter over
`EventStream<ActorEvent>`.

Keep durable semantics in `effect-s2-durable`; move generic ordered record IO,
append batching/session mechanics, checkpoint/snapshot cursor mechanics, trim,
and materialization helpers into `effect-s2-stream-db`.

### Substrate Step C: Add object snapshots and trimming

Use S2 snapshots/trimming through the stream-db substrate:

- write `ActorSnapshot` at a known owner-stream cursor;
- on open/recovery, load snapshot then read only the tail;
- trim records already covered by the snapshot after correctness is tested.

### Nomenclature Step A: Split the public engine API node — done

Created:

```text
src/engine/api.ts
  DurableEngineApi
  DurableEngine
  WorkflowStartStatus
```

Public modules import service tag/types from `engine/api.ts`. The live engine
implementation and layer assembly live in `engine/live.ts`.

### Nomenclature Step B: Split entrypoints by audience — done

Added explicit subpath exports:

- `./engine`;
- `./ingress` or `./server`;
- existing `./client`;
- existing `./host`.

The root barrel is now the authoring surface. Server/client/host adapters live
behind their subpaths.

### Nomenclature Step C: Move files out of `src/runtime` — done

Behavior-preserving moves:

```text
runtime/state.ts                  -> engine/state.ts
runtime/invocation.ts             -> engine/context.ts
runtime/primitive-interpreter.ts  -> engine/handler-primitives.ts
runtime/completion-reader.ts      -> engine/result-reader.ts
runtime/ingress-router.ts         -> engine/resolution-router.ts
runtime/serviceDeferreds.ts       -> engine/service-deferreds.ts
runtime/helpers.ts                -> engine/helpers.ts
runtime/address.ts                -> engine/address.ts
```

### Graph Step D: Split mixed stores

Replace `DurableStores` with:

- `ServiceStores`;
- `ObjectStores`;
- `S2Access`.

### Capability Step 0: Resolve the handler/engine knot — done

Handlers no longer receive the broad engine API for handler-scoped operations.
Every executor that runs handlers provides:

- `ActiveInvocation`, the internal execution record;
- `CurrentInvocationScope`, the semantic authoring capability object.

Root clients still use `DurableEngine`; handler-scoped clients use
`CurrentInvocationScope.calls`. This removes the old recursive pressure where
handlers needed the same wide API object that `engine/live.ts` was assembling.

### Executor Extraction

After the state machine and API knot are clear:

1. Extract `SharedObjectRunner`.
2. Extract `ChildCallCoordinator`.
3. Extract `ServiceExecutor`.
4. Extract `ObjectExecutor`.
5. Introduce `RecoveryCoordinator`.
6. Split `engine/live.ts` further by extracting executor modules only when the
   capability implementation has a clear home.
7. Remove transitional object storage mechanics superseded by `EventStream` and
   snapshots.

## Testing Strategy

Each step should preserve existing tests.

Add focused tests as modules split:

- `ObjectStateMachine`: admission idempotency, FIFO, completion status, signal
  resolution, state/journal event emission;
- `HandlerPrimitives`: service/object/shared primitive behavior by injecting
  fake active invocations;
- `ResultReader`: service roster result, object result, unknown id;
- `ChildCallCoordinator`: deterministic id, self-call rejection,
  shared-call rejection, call-vs-send;
- `ServiceExecutor`: idempotent submit, completion, recovery;
- `ObjectExecutor`: object submit, workflow start run-once, infrastructure error
  not committed as `Completed`;
- `ObjectOwnerDriver`: driver integration, admission CAS, drain ordering, fenced
  handoff, signal ingress;
- `RecoveryCoordinator`: service and object pending work re-driven.

Keep S2-lite integration tests for cross-module confidence. Use pure tests for
state-machine decisions wherever possible.

## Validation Gates

For docs-only changes:

- `git diff --check`.

For import-only or narrow code moves:

- package typecheck;
- package lint;
- relevant package tests.

For storage or state-machine behavior:

- package typecheck;
- package lint;
- package tests with S2-lite specs;
- dependency graph check for expected edge direction.
