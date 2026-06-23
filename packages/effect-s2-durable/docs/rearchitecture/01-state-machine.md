# State Machine Axis

## Core Rule

Durable semantics belong in a state machine, not smeared across Effect services.

Effect services are useful for dependency injection, lifecycle, scopes, and
resource management. They should not be where the durable protocol is invented.
If a service both decides protocol state and performs IO, it becomes hard to tell
whether a bug is in the state transition or in the adapter.

## Target Shape

```text
ObjectStateMachine
  command + current projection + local driver facts
  -> result value
  -> ActorEvent[]
  -> ObjectDriverAction[]

Drivers / ports
  S2 event-stream read/append/fence
  handler execution
  local waiters
  timers / clock
  ingress
  host lifecycle
```

The object machine is co-located under `object/machine/`:

- `model.ts` owns durable schemas, `ActorEvent`, `ActorSnapshot`,
  `transition`, `replay`, call-id codecs, and projection query helpers;
- `commands.ts` owns `ObjectCommand`, `ObjectDecision`, `decide(...)`, typed
  decision helpers, and `ObjectDriverAction`;
- `index.ts` is the only import surface used by owner drivers, engine modules,
  and tests.

The remaining split is intentional: `object/owner-driver.ts` owns IO and local
process facts, while `object/machine/*` owns durable semantics.

The first extraction should focus on the object owner protocol. Do not start by
introducing a generic `DurableStateMachine` abstraction.

## Higher-Level Boundaries To Add First

The state machine should not become the whole architecture. A useful lesson from
Rivet's Effect package is that the public actor model is split above the storage
protocol:

- `Action`: a value-level schema contract for one request/response operation;
- `Actor`: a value-level actor contract that carries name and actions, not server
  implementation;
- `Client`: typed actor accessor that encodes payloads and decodes results/errors;
- `Registry`: a collection layer for actor registrations;
- `State`: a handler-facing state view whose reads/writes are Effect-typed and
  serialized, while the underlying store remains the source of truth.

Adopt that split here, but keep our event-sourced core:

```text
DurableActor / DurableAction
  public contract, schemas, typed client, handler registration

ObjectInvocationContext
  current object/key/callId/method, handler scope, request metadata

ObjectState<A>
  handler-facing typed state facade
  delegates to ObjectStateMachine commands

ObjectStateMachine
  durable protocol decision boundary
  projection + command -> events + driver actions

ObjectDriver / ObjectOwnerDriver
  S2 read/fold/append/fence, handler execution, waiters
```

The boundary rule is:

- actor/action/state facades define **ergonomics and typing**;
- the state machine defines **durable semantics**;
- the driver defines **IO and process-local execution**.

This prevents the machine from becoming a low-level dumping ground and prevents
`ObjectOwnerDriver` from continuing to hide protocol decisions.

### `DurableAction`

Adopt Rivet's `Action` idea as the public operation contract:

```ts
interface DurableAction<
  Tag extends string,
  Payload extends Schema.Top,
  Success extends Schema.Top,
  Error extends Schema.Top,
> {
  readonly _tag: Tag
  readonly payloadSchema: Payload
  readonly successSchema: Success
  readonly errorSchema: Error
}
```

In this package, a `DurableAction` maps to an object method, service handler, or
workflow handler. It should own schema encode/decode and typed expected errors.
It should not own owner-stream state transitions.

### `DurableActor`

Adopt Rivet's `Actor` idea as the object/workflow contract:

```ts
interface DurableActor<Name extends string, Actions extends DurableAction.Any> {
  readonly name: Name
  readonly actions: ReadonlyArray<Actions>
  readonly client: Effect.Effect<DurableActorAccessor<Actions>, never, DurableClient>
  readonly toLayer: (...) => Layer.Layer<never, never, ...>
}
```

This is the semantic home for today's scattered `object(...)`,
`objectClient(...)`, `sharedClient(...)`, `workflow(...)`, and handler registry
plumbing. The contract is not the engine and not the state machine. It registers
handlers and produces typed clients.

### `ObjectInvocationContext`

Adopt Rivet's `CurrentAddress` idea, but with our durable identity:

```ts
interface ObjectInvocationContext {
  readonly object: string
  readonly key: string
  readonly method: string
  readonly callId: string
}
```

This is the semantic replacement for treating `ActiveInvocation` as an unbounded
engine context. It is the ambient identity a handler can rely on. It should not
include S2 tokens, expected tails, projection caches, or driver locks.

### `ObjectState<A>`

Rivet's `State` is useful as a handler-facing facade: read/write closures,
serialized `update`/`modify`, and a replaying change stream. For this package,
the facade must be event-sourced:

```ts
interface ObjectState<A> {
  readonly get: Effect.Effect<Option.Option<A>, DurableExecutionError>
  readonly set: (value: A) => Effect.Effect<void, DurableExecutionError>
  readonly update: (f: (current: Option.Option<A>) => A) => Effect.Effect<A, DurableExecutionError>
  readonly modify: <B>(f: (current: Option.Option<A>) => readonly [B, A]) => Effect.Effect<B, DurableExecutionError>
  readonly changes: Stream.Stream<A, DurableExecutionError>
}
```

But internally:

```text
get/update/modify
  -> ObjectStateMachine.stateGet / stateSet / stateDelete
  -> Journaled / StateChanged events
  -> OwnerDriveSession append
  -> projection advances after ack
```

Do not adopt `State` as an independent latest-value storage cell. The owner event
stream remains the source of truth. The facade is only a typed ergonomic view over
machine commands.

### Dispatcher Boundary

Adopt Rivet's action-dispatcher separation:

```text
wire/client payload
  -> decode by action schema
  -> run typed handler
  -> encode success or expected error
  -> pass ActorExit to state machine complete()
```

The dispatcher owns schema and error envelopes. The state machine owns whether a
`Completed` event should be emitted. The driver owns appending that event.

## Existing Event Contract

Current durable events are:

```ts
type ActorEvent =
  | { readonly _tag: "Accepted"; readonly callId: string; readonly method: string; readonly input?: unknown }
  | { readonly _tag: "StateChanged"; readonly op: "set" | "delete"; readonly table: string; readonly key: string; readonly value?: unknown }
  | { readonly _tag: "Journaled"; readonly callId: string; readonly kind: string; readonly step: string; readonly value: unknown }
  | { readonly _tag: "SignalResolved"; readonly callId: string; readonly name: string; readonly value?: unknown }
  | { readonly _tag: "Completed"; readonly callId: string; readonly exit: ActorExit }
```

Current projection is:

```ts
interface ActorSnapshot {
  readonly order: ReadonlyArray<string>
  readonly results: ReadonlyMap<string, ActorExit>
  readonly state: ReadonlyMap<string, ReadonlyMap<string, unknown>>
  readonly journal: ReadonlyMap<string, ReadonlyMap<string, unknown>>
  readonly signals: ReadonlyMap<string, ReadonlyMap<string, unknown>>
}
```

The state machine builds on this contract instead of replacing it. These values
now live together in `object/machine/model.ts`, while command processing lives in
`object/machine/commands.ts`.

## Durable Facts Vs Local Facts

The machine should distinguish durable projection facts from local driver facts.

Durable facts are in `ActorSnapshot`:

- accepted call order;
- terminal results;
- latest user state;
- journaled primitive records;
- resolved signals.

Local driver facts are not durable protocol truth:

- call ids already started by this process;
- whether a local waiter is registered;
- whether this process currently holds the owner fence;
- cached projection/tail coordinates.

The machine may read local facts as explicit input to avoid duplicate
in-process work, but it must not treat them as durable state.

```ts
export interface ObjectLocalState {
  readonly started: ReadonlySet<string>
}
```

## Public Types

### Results

Keep existing user-visible outcomes:

```ts
export type AdmitResult =
  | { readonly _tag: "Admitted" }
  | { readonly _tag: "AlreadyPending" }
  | { readonly _tag: "AlreadyCompleted" }
```

`CallStatus` lives with the machine projection helpers in
`object/machine/model.ts`.

### Pending Head

The driver needs the full `Accepted` data for a selected head.

```ts
export interface PendingHead {
  readonly callId: string
  readonly method: string
  readonly input: unknown
}
```

### Driver Actions

Driver actions are side-effect instructions. They are not durable state.

```ts
export type ObjectDriverAction =
  | { readonly _tag: "RunHead"; readonly head: PendingHead }
  | { readonly _tag: "NotifySignalWaiter"; readonly callId: string; readonly name: string }
  | { readonly _tag: "RegisterSignalWaiter"; readonly callId: string; readonly name: string }
  | { readonly _tag: "DropLocalStarted"; readonly callId: string }
```

The first implementation may only need `RunHead`, `NotifySignalWaiter`, and
`DropLocalStarted`. `RegisterSignalWaiter` can remain a driver concern until the
signal-await path is extracted.

### Apply Result

Every command returns one shape:

```ts
export interface ObjectApplyResult<A> {
  readonly result: A
  readonly events: ReadonlyArray<ActorEvent>
  readonly actions: ReadonlyArray<ObjectDriverAction>
}

const applied = <A>(
  result: A,
  events: ReadonlyArray<ActorEvent> = [],
  actions: ReadonlyArray<ObjectDriverAction> = [],
): ObjectApplyResult<A> => ({ result, events, actions })
```

The driver is responsible for appending `events` and executing `actions` in the
correct order for the command.

## Commands And Pure Functions

Prefer named functions over a giant command union for the first extraction. A
single union can come later if it removes real duplication.

### `admit`

```ts
export const admit = (
  snapshot: ActorSnapshot,
  input: {
    readonly callId: string
    readonly method: string
    readonly input: unknown
  },
): ObjectApplyResult<AdmitResult> => {
  if (snapshot.order.includes(input.callId)) {
    return applied(snapshot.results.has(input.callId)
      ? { _tag: "AlreadyCompleted" }
      : { _tag: "AlreadyPending" })
  }

  return applied(
    { _tag: "Admitted" },
    [{ _tag: "Accepted", callId: input.callId, method: input.method, input: input.input }],
  )
}
```

Driver notes:

- `ObjectOwnerDriver.admit` still performs CAS around the emitted `Accepted` event.
- On CAS conflict, the driver re-reads and calls `admit` again.
- The machine does not know about `matchSeqNum` or retry budgets.

### `status`

```ts
export const status = (snapshot: ActorSnapshot, callId: string): CallStatus =>
  callStatus(snapshot, callId)
```

Driver notes:

- `ObjectOwnerDriver.status` only reads/folds and calls this helper.
- Unknown vs pending remains a pure projection decision.

### `selectNextHead`

```ts
export const selectNextHead = (
  snapshot: ActorSnapshot,
  accepted: ReadonlyMap<string, PendingHead>,
  local: ObjectLocalState,
): ObjectApplyResult<PendingHead | undefined> => {
  const callId = snapshot.order.find((candidate) =>
    !snapshot.results.has(candidate) && !local.started.has(candidate))

  if (callId === undefined) {
    return applied(undefined)
  }

  const head = accepted.get(callId)
  return applied(head, [], head === undefined ? [] : [{ _tag: "RunHead", head }])
}
```

Driver notes:

- `accepted` can be derived from log entries by the driver while folding.
- If a snapshot order entry lacks a matching `Accepted`, the driver can ignore it
  as corrupt/unreachable or surface an infrastructure error. The current code
  treats it as unreachable.
- The local `started` set is an explicit input, not hidden machine state.

### `stateSet` / `stateDelete`

```ts
export const stateSet = (
  table: string,
  key: string,
  value: unknown,
): ObjectApplyResult<void> =>
  applied(undefined, [{ _tag: "StateChanged", op: "set", table, key, value }])

export const stateDelete = (table: string, key: string): ObjectApplyResult<void> =>
  applied(undefined, [{ _tag: "StateChanged", op: "delete", table, key }])
```

Driver notes:

- object state backend calls these helpers, appends emitted events through the
  owner-drive session, then advances its snapshot ref with `transition`.
- The machine does not know about fencing tokens.

### `stateGet`

`state.get` is subtle because it journals a read result for replay stability.

```ts
export const stateGet = (
  snapshot: ActorSnapshot,
  input: {
    readonly callId: string
    readonly step: string
    readonly table: string
    readonly key: string
  },
): ObjectApplyResult<Option.Option<unknown>> => {
  const recorded = journalValue(snapshot, input.callId, "read", input.step)
  if (Option.isSome(recorded)) {
    const record = recorded.value as { readonly present: boolean; readonly value: unknown }
    return applied(record.present ? Option.some(record.value) : Option.none())
  }

  const live = stateValue(snapshot, input.table, input.key)
  const record = { present: Option.isSome(live), value: Option.getOrNull(live) }
  return applied(live, [{
    _tag: "Journaled",
    callId: input.callId,
    kind: "read",
    step: input.step,
    value: record,
  }])
}
```

Driver notes:

- the backend owns read step allocation via its local `readCounter`;
- the machine owns read replay semantics and the exact journal record shape.

### `journalGet` / `journalPut`

```ts
export const journalGet = (
  snapshot: ActorSnapshot,
  callId: string,
  kind: string,
  step: string,
): Option.Option<unknown> => journalValue(snapshot, callId, kind, step)

export const journalPut = (
  callId: string,
  kind: string,
  step: string,
  value: unknown,
): ObjectApplyResult<void> =>
  applied(undefined, [{ _tag: "Journaled", callId, kind, step, value }])
```

Driver notes:

- durable primitive replay (`run`, future `sleep`) remains expressed as journal
  facts;
- the primitive interpreter should not build `Journaled` events directly once the
  machine exists.

### `resolveSignal`

```ts
export const resolveSignal = (
  snapshot: ActorSnapshot,
  input: { readonly callId: string; readonly name: string; readonly value: unknown },
): ObjectApplyResult<void> => {
  if (Option.isSome(signalValue(snapshot, input.callId, input.name))) {
    return applied(undefined)
  }

  return applied(
    undefined,
    [{ _tag: "SignalResolved", callId: input.callId, name: input.name, value: input.value }],
    [{ _tag: "NotifySignalWaiter", callId: input.callId, name: input.name }],
  )
}
```

Driver notes:

- current `transition` already makes signal resolution first-write-wins;
- making first-write-wins explicit in the command avoids appending known no-op
  duplicate resolutions after a fresh read;
- cross-host concurrent duplicate resolutions can still append duplicate events
  unless ingress admission later uses a CAS/idempotency boundary. The fold remains
  first-write-wins.

### `awaitSignal`

`awaitSignal` does not append by itself. It decides whether to return now or
park.

```ts
export type AwaitSignalDecision =
  | { readonly _tag: "Resolved"; readonly value: unknown }
  | { readonly _tag: "Park" }

export const awaitSignal = (
  snapshot: ActorSnapshot,
  callId: string,
  name: string,
): ObjectApplyResult<AwaitSignalDecision> => {
  const resolved = signalValue(snapshot, callId, name)
  if (Option.isSome(resolved)) {
    return applied({ _tag: "Resolved", value: resolved.value })
  }

  return applied(
    { _tag: "Park" },
    [],
    [{ _tag: "RegisterSignalWaiter", callId, name }],
  )
}
```

Driver notes:

- the driver still owns the race-closing loop: register waiter, re-read/replay,
  re-check, then park;
- the machine only owns the durable decision: resolved vs not resolved.

### `complete`

```ts
export const complete = (
  snapshot: ActorSnapshot,
  callId: string,
  exit: ActorExit,
): ObjectApplyResult<void> => {
  if (snapshot.results.has(callId)) {
    return applied(undefined)
  }
  return applied(undefined, [{ _tag: "Completed", callId, exit }])
}
```

Driver notes:

- a second completion for the same call should be a no-op at the machine level;
- the owner driver should still prevent double-run with local started guards and
  fencing.

## Driver Protocol

The driver should follow a small number of patterns.

### Admission Driver

```text
read owner log
fold snapshot
result = ObjectStateMachine.admit(snapshot, input)
if result.events is empty -> return result.result
cas append Accepted at tail
if conflict -> retry from read
return result.result
```

### Drain Driver

```text
read owner log
merge with warm projection cache
if no selectable head -> cache projection and return without fencing
open owner drive session only if a head exists
loop:
  head = ObjectStateMachine.selectNextHead(snapshot, accepted, { started })
  if none -> cache projection and return
  mark started locally before running
  backend = machine-backed ObjectStateBackend
  exit = runHead(head, backend)
  completion = ObjectStateMachine.complete(snapshot, head.callId, exit)
  append completion through owner drive session
  advance snapshot with appended event
  continue
on FenceLost:
  drop local started/projection for this owner and stop
```

### ObjectStateBackend Driver

Every backend method should become:

```text
call machine helper
append emitted events through current owner drive session
advance snapshot ref with acknowledged seq nums
execute emitted local actions if any
return helper result
```

The backend should not construct `ActorEvent` objects directly after the machine
exists.

### Signal Resolution Driver

```text
read/fold owner snapshot, or use trusted warm snapshot when already under owner lock
result = ObjectStateMachine.resolveSignal(snapshot, input)
append SignalResolved if emitted
execute NotifySignalWaiter action
```

External ingress appends do not require the owner fence token. Handler-side
`deferred.resolve` does go through the owner drive session because it is part of
active owner execution.

## Current-Code Mapping

| Current code | Move to machine | Remains driver |
| --- | --- | --- |
| `admit`: duplicate/pending/completed decision | yes | read, tail, CAS retry, ensure stream |
| `status`: `callStatus(replay(...))` | yes | read/fold |
| `drain`: pending head selection | yes | lock, read merge, open fence, loop |
| `drain`: no pending means do not fence | partly | driver decides not to open session when machine selects no head |
| `makeBackend.get`: read replay + journal record shape | yes | read counter, append, update snapshot ref |
| `makeBackend.set/delete`: event construction | yes | append, update snapshot ref |
| `makeBackend.journal.put`: event construction | yes | append, update snapshot ref |
| `signal.await`: resolved vs park | yes | register/recheck/park loop |
| `signal.resolve` / `resolveSignal`: event construction + first-write-wins check | yes | append, poke waiter |
| `runOne`: completion event construction | yes | handler execution, append completion |
| `FenceLost` handling | no | owner drive session / driver boundary |
| projection cache / started set | no | local driver facts |

## Testing Plan

Add pure tests for `object/machine/commands.ts` before changing drivers.

1. `admit` emits `Accepted` for an unknown call.
2. `admit` returns `AlreadyPending` for accepted but incomplete call.
3. `admit` returns `AlreadyCompleted` for completed call.
4. `selectNextHead` returns the earliest accepted call not completed and not
   locally started.
5. `selectNextHead` returns `undefined` when all calls are completed or locally
   started.
6. `stateGet` returns a live value and emits a read journal when no journal
   exists.
7. `stateGet` returns the recorded journal value even if live state changed.
8. `resolveSignal` emits `SignalResolved` and `NotifySignalWaiter` when
   unresolved.
9. `resolveSignal` emits no event when the signal is already resolved in the
   snapshot.
10. `complete` emits `Completed` once and no-ops if already completed.

These tests should not need S2-lite, Effect layers, clocks, fibers, or handlers.

## Restate Inspiration

Restate's state machine applies log commands, updates durable state, and
generates actions that the partition processor executes after commit. That shape
is useful here even though the surrounding system is different:

```text
logged command
  -> state-machine apply
  -> storage/state updates
  -> collected actions
  -> driver executes actions after commit
```

Useful ideas to copy:

- explicit command vocabulary;
- explicit invocation state transitions;
- command-specific handlers for journal effects like state set, promise
  completion, sleep, call, and notification;
- action collection as a side-effect buffer;
- action execution outside the transition.

Do not copy:

- partition leadership;
- RocksDB table layout;
- VQueues;
- Bifrost/WAL mechanics;
- cluster version barriers.

Our S2 owner stream is the durable log. The machine should be simpler and purer
than Restate's apply path.

Relevant references:

- https://deepwiki.com/restatedev/restate/3.2-statemachine-command-processing
- https://github.com/restatedev/restate/tree/main/crates/worker/src/partition/state_machine

## Service Unification

Do not extract a generic cross-domain machine yet. First prove the object
machine.

When service-path unification starts, model service/workflow execution with the
same shape:

```text
Command + Projection -> Decision -> Events + DriverActions
```

Only then consider a shared `DurableStateMachine` abstraction. Premature
generalization will recreate the current muddy boundary under a different name.
