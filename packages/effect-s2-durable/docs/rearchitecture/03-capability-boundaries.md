# Capability Boundaries Axis

## Problem

The current architecture is still shaped around a wide engine facade. Moving
files reduced the size of the root folder, but it did not remove the main
design smell: implementation verbs are flattened across services.

The clearest example is `DurableEngineApi`. It currently mixes:

- external lifecycle operations: `submit`, `attach`, `poll`, `workflowStart`;
- handler-scoped durable operations: `runStep`, `sleepStep`, `stateGet`,
  `stateSet`, `stateDelete`, `awaitDeferred`, `resolveLocal`;
- child invocation operations: `callStep`, `sendStep`, `sharedCall`;
- ingress routing: `resolveExternal`;
- implementation guards: `assertTopLevel`.

That interface is hard to explain because it is not one concept. It is the
union of several capabilities that happen to need coordination today.

Splitting the same methods into `InvocationOperationsApi` would not fix the
problem. A flat `InvocationOperationsApi` with `stateGet`, `stateSet`,
`stateDelete`, `run`, `sleep`, `call`, and `send` is still a smaller sock
drawer. The target must group operations behind semantic capability objects.

## Design References

### Effect Workflow

Effect's `WorkflowEngine` is narrower than our current engine surface. It owns
workflow lifecycle and a few durable infrastructure hooks: register, execute,
poll, interrupt, resume, activity execution, durable deferred completion, and
clock scheduling.

The important design point is not exact method count. It is that workflow-local
state is modeled separately as `WorkflowInstance`, while the engine facade is
not also the workflow context.

Reference:
<https://github.com/Effect-TS/effect/blob/main/packages/workflow/src/WorkflowEngine.ts>

### Restate TypeScript SDK

Restate separates core definitions, endpoint binding, runtime context,
standalone clients, and generator/free-function ergonomics across distinct
packages:

- `restate-sdk-core`: definition markers and serde contracts;
- `restate-sdk`: endpoint and context implementation;
- `restate-sdk-clients`: remote ingress clients;
- `restate-sdk-gen`: generator/free-function surface, scheduler, operations.

The useful lesson is that free primitives delegate to an active operations
object. They are not all methods on the public engine facade.

References:

- <https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk-core>
- <https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk>
- <https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk-clients>
- <https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk-gen>

### effect-encore

`effect-encore` is a more direct reference for avoiding flattened service
surfaces. It uses deep seams and value objects:

- `Client` is the topology-agnostic transport seam;
- `ActorMailbox` is a narrow dispatch capability;
- `State` is a value object with grouped state operations;
- actor refs expose semantic operations such as execute, send, peek, watch, and
  waitFor.

The useful lesson is: expose semantic objects, not every internal verb.

Reference:
<https://github.com/cevr/effect-encore/tree/main>

## Target Mental Model

The package should be explainable as:

```text
authoring definitions
  -> catalog compiler
  -> public engine facade
  -> invocation-scoped capability object
  -> service/object executors
  -> S2 storage ports
```

The public engine starts and observes durable invocations. The invocation scope
is what handler code uses while an invocation is running. Storage ports own S2
access. Executors interpret durable semantics for service and object paths.

## Public Engine Facade

`DurableEngine` should be a narrow lifecycle facade. It should not expose
handler primitives, child-call internals, state mutation, durable-promise waiter
details, or implementation guards.

Implemented shape after this cutover:

```ts
export interface DurableEngineApi {
  readonly submit: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    invocationId: string,
    input: I,
  ) => Effect.Effect<void, DurableExecutionError, R>

  readonly attach: <A, I>(
    invocationId: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>

  readonly poll: <A, I>(
    invocationId: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<Option.Option<A>, DurableExecutionError>

  readonly query: <A, I>(
    handler: Handler<unknown, unknown, never, never>,
    object: string,
    key: string,
    input: unknown,
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>

  readonly resolveAwakeable: <A, I>(
    invocationId: string,
    id: string,
    schema: Schema.Codec<A, I, never, never>,
    value: A,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly resolveDurablePromise: <A, I>(
    invocationId: string,
    name: string,
    schema: Schema.Codec<A, I, never, never>,
    value: A,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly startWorkflow: <I, O, E, R>(
    handler: Handler<I, O, E, R>,
    workflowRunId: string,
    input: I,
  ) => Effect.Effect<WorkflowStartStatus, DurableExecutionError, R>
}
```

`query` remains on the public engine because shared read-only handlers are a
public observation path, not a handler-only primitive. The stable rule is: if a
method is only valid from inside a running handler, it does not belong here.

## Invocation Scope

Handler code should receive one structured, invocation-scoped capability
object. This is closer to Restate's context/operations split and
effect-encore's capability objects.

Target shape:

```ts
export interface InvocationScope {
  readonly request: HandlerRequestAccess
  readonly steps: StepJournal
  readonly clock: DurableClock
  readonly state: DurableStateFactory
  readonly awakeables: Awakeables
  readonly durablePromises: DurablePromises
  readonly calls: ServiceCommunication
}
```

This object is provided only while a handler is running. Public free primitives
are thin adapters over `CurrentInvocationScope`.

```ts
run(...)                    -> CurrentInvocationScope.steps.run(...)
sleep(...)                  -> CurrentInvocationScope.clock.sleep(...)
state(Table)                -> CurrentInvocationScope.state.table(Table)
awakeable(schema)           -> CurrentInvocationScope.awakeables.create(schema)
durablePromise(name, schema) -> CurrentInvocationScope.durablePromises.get(name, schema)
resolveAwakeable(...)       -> DurableEngine.resolveAwakeable(...)
resolveDurablePromise(...)  -> DurableEngine.resolveDurablePromise(...)
serviceClient(...)          -> CurrentInvocationScope.calls.callService(...)
serviceSendClient(...)      -> CurrentInvocationScope.calls.sendService(...)
objectClient(...)           -> CurrentInvocationScope.calls.callObject(...)
objectSendClient(...)       -> CurrentInvocationScope.calls.sendObject(...)
```

## Capability Objects

### Request Access

```ts
export interface HandlerRequestAccess {
  readonly input: <A, I>(
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<A, DurableExecutionError>
}
```

This is the replacement for a flattened `handlerRequest` method.

### Step Journal

```ts
export interface StepJournal {
  readonly run: <A, E, R, EncodedA, EncodedE>(
    action: Effect.Effect<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>,
  ) => Effect.Effect<A, E | DurableExecutionError, R>
}
```

This owns durable side-effect memoization. It should not know how to submit
child calls or mutate state.

### Durable Clock

```ts
export interface DurableClock {
  readonly sleep: (
    name: string,
    duration: Duration.Duration,
  ) => Effect.Effect<void, DurableExecutionError>
}
```

Clock behavior should not be flattened into the engine. Later timer-wheel
recovery belongs behind this capability or its storage port.

### Durable State

```ts
export interface DurableStateFactory {
  readonly table: <Tbl extends AnyTable>(table: Tbl) => DurableTable<Tbl>
}

export interface DurableTable<Tbl extends AnyTable> {
  readonly get: (
    key: string,
  ) => Effect.Effect<Option.Option<RowOf<Tbl>>, DurableExecutionError>

  readonly set: (
    row: RowOf<Tbl>,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly delete: (
    key: string,
  ) => Effect.Effect<void, DurableExecutionError>
}
```

This is the important correction to the flattened interface. We should not have
`stateGet`, `stateSet`, and `stateDelete` spread across an engine or operations
service. State is a value object, like `effect-encore`'s `State`.

### Awakeables

```ts
export interface Awakeables {
  readonly create: <A, I>(
    schema: Schema.Codec<A, I, never, never>,
  ) => Effect.Effect<Awakeable<A>, DurableExecutionError>

  readonly resolve: <A, I>(
    id: string,
    schema: Schema.Codec<A, I, never, never>,
    value: A,
  ) => Effect.Effect<void, DurableExecutionError>

  readonly reject: (
    id: string,
    reason: string,
  ) => Effect.Effect<void, DurableExecutionError>
}

export interface Awakeable<A> {
  readonly id: string
  readonly promise: Effect.Effect<A, DurableExecutionError>
}
```

This follows Restate's awakeable model: awakeables are ID-based external
callbacks. A handler creates an awakeable, passes the ID to an external system,
and later that ID is resolved or rejected.

Awakeables are appropriate for services and virtual objects where the external
system does not know a logical workflow promise name.

### Durable Promises

```ts
export interface DurablePromises {
  readonly get: <A, I>(
    name: string,
    schema: Schema.Codec<A, I, never, never>,
  ) => DurablePromise<A>
}

export interface DurablePromise<A> {
  readonly await: Effect.Effect<A, DurableExecutionError>
  readonly resolve: (value: A) => Effect.Effect<void, DurableExecutionError>
  readonly reject: (reason: string) => Effect.Effect<void, DurableExecutionError>
}
```

This follows Restate's durable-promise model: durable promises are name-based
and scoped to a durable invocation, especially workflows. Use them when the
logical event name is known by the workflow and its handlers, for example
`durablePromise("review", Review).await`.

The two signaling patterns from Restate map directly:

- **External -> Workflow**: an external/shared handler resolves a durable
  promise that the run handler is awaiting.
- **Workflow -> External**: the run handler resolves a durable promise that
  another workflow handler is awaiting.

Do not introduce a third package-level concept such as "waitpoints" or
"durable signals." In this package, the vocabulary is:

- mailbox / invocation: starts work;
- awakeable: ID-based callback;
- durable promise: name-based durable event.

### Service Communication

This capability preserves Restate-style service communication. It is not
optional and must not be lost during the engine split.

Restate separates service communication from external events:

- service/object/workflow clients start work in another handler;
- send clients enqueue work and continue;
- attach/poll observe an invocation by id;
- awakeables and durable promises resolve parked waits inside existing work.

The same split should exist here. The service-communication capability owns
handler-to-handler calls and sends. Awakeables and durable promises own external
events.

There are two call sites with different ID planning:

- **root clients**: `client(service).method(input)` and
  `sendClient(service).method(input)` start root invocations. They use a fresh
  invocation id unless the caller supplies an idempotency key.
- **handler-scoped clients**: `serviceClient(service).method(input)`,
  `serviceSendClient(service).method(input)`, `objectClient(object, key)`, and
  `objectSendClient(object, key)` delegate to `CurrentInvocationScope.calls`
  and derive a replay-stable child id from the executing invocation:

```text
parent invocation id + child ordinal + target identity -> child invocation id
```

This distinction is mandatory. A root client is not replay-safe inside a
handler. A handler-scoped client is replay-safe because it derives child IDs
from the active invocation.

```ts
export interface ServiceCommunication {
  readonly callService: (...)
  readonly sendService: (...)
  readonly callObject: (...)
  readonly sendObject: (...)
  readonly sharedObject: (...)
}
```

The typed public call surface is still the semantic ref (`serviceClient`,
`serviceSendClient`, `objectClient`, `objectSendClient`, `sharedClient`). The
scope capability is lower-level only so those refs have one place to delegate.
`callStep`, `sendStep`, and `sharedCall` are no longer exposed on the engine.

## Storage Ports

`engine/durable-stores.ts` is a naming and boundary smell. It combines S2 client
access, service workflow DB opening, roster access, and object execution.

Target ports:

```ts
export interface S2Access {
  readonly client: S2ClientApi
  readonly provideClient: <A, E>(
    effect: Effect.Effect<A, E, S2Client>,
  ) => Effect.Effect<A, E>
}

export interface ServiceStore {
  readonly roster: RosterTable
  readonly openWorkflow: (id: string) => Effect.Effect<WfDb, DurableExecutionError>
}

export interface ObjectStore {
  readonly instances: ObjectInstanceExecutorApi
}
```

The engine should depend on storage ports by purpose. It should not import a
mixed `DurableStores` service.

## Object Instance Naming

Avoid the term `owner` in public or top-level internal names. The concept is:

> the durable object instance identified by `(objectName, key)`.

Rename targets:

```text
object/owner-driver.ts   -> object/instance-executor.ts
ObjectOwnerDriver        -> ObjectInstanceExecutor
owner stream             -> object instance stream
owner key                -> object instance key
owner drain              -> object instance drain
```

`owner` may remain as a low-level comment only if it refers specifically to S2
single-writer ownership/fencing. It should not be the architectural noun.

## State Machine Boundary

`object/machine/index.ts` should not export `*`. The state machine import
surface should say exactly what a driver/executor may use.

Target:

```ts
export type {
  ActorEvent,
  ActorExit,
  ActorSnapshot,
  ObjectCommand,
  ObjectDecision,
  ObjectDriverAction,
  LogEntry,
} from "./model.ts"

export {
  replay,
  transition,
  decide,
  callStatus,
  stateValue,
  journalGet,
  awaitSignal,
} from "./commands.ts"
```

The machine should own durable protocol decisions only:

```text
Projection + Command -> Decision
Decision = EventsToAppend + DriverActions + Reply
```

It should not own S2 stream IO, fencing, locks, handler execution, waiters, or
Effect service wiring.

## Proposed Directory Shape

```text
src/
  authoring/
    definition.ts
    handler.ts
    primitives.ts
    types.ts

  catalog/
    compiler.ts
    layer.ts
    index.ts

  engine/
    api.ts              # narrow DurableEngine facade
    kernel.ts           # assembles lifecycle facade from capabilities
    index.ts

  invocation/
    scope.ts            # CurrentInvocationScope tag
    request.ts
    steps.ts
    clock.ts
    state.ts
    awakeables.ts
    durable-promises.ts
    service-communication.ts
    client.ts
    plan.ts

  service/
    executor.ts
    store.ts
    recovery.ts

  object/
    address.ts
    instance-executor.ts
    stream.ts
    state-backend.ts
    promise-waiters.ts
    drive-session.ts
    machine/
      model.ts
      commands.ts
      index.ts

  storage/
    s2-access.ts
    service-tables.ts

  ingress/
    contract.ts
    server.ts
    client.ts
    index.ts

  host/
    index.ts

bin/
  host.ts
```

The package root `bin/` should be outside `src/`. A runnable process entrypoint
is not part of the library source graph.

## Migration Plan

### Step 1: Narrow `DurableEngineApi` (implemented)

Remove handler-scoped operations from `DurableEngineApi`:

- `runStep`;
- `handlerRequest`;
- `sleepStep`;
- `stateGet`;
- `stateSet`;
- `stateDelete`;
- `awaitDeferred`;
- `resolveLocal`;
- `resolvePromise`;
- `nextAwakeableId`;
- `callStep`;
- `sendStep`;
- `assertTopLevel`.

Keep lifecycle, shared query, and external ingress methods only.

The external-event vocabulary rename remains follow-up:

- remove public `signal`;
- remove public `deferred`;
- remove public `resolveSignal`;
- replace them with `durablePromise` and `resolveDurablePromise`;
- keep `awakeable` and `resolveAwakeable`, and add `rejectAwakeable`.

Validation:

- typecheck shows public free primitives no longer depend on `DurableEngine`
  for handler-scoped operations;
- dependency graph shows `authoring/primitives.ts -> invocation/scope.ts`, not
  only `authoring/primitives.ts -> engine/api.ts`.

### Step 2: Introduce `CurrentInvocationScope` (implemented)

Create:

```ts
export class CurrentInvocationScope extends Context.Service<
  CurrentInvocationScope,
  InvocationScope
>()("effect-s2-durable/invocation/CurrentInvocationScope") {}
```

Provide this scope when running a service, object, or shared handler. Public
free primitives read this service.

Validation:

- calling `run` outside a handler fails because `CurrentInvocationScope` is
  missing, not because the broad engine checked `ActiveInvocation`;
- top-level clients no longer need `assertTopLevel`;
- root `client` / `sendClient` reject inside a handler by checking the active
  invocation before minting a root id;
- handler-scoped `serviceClient` and `objectClient` call through
  `CurrentInvocationScope.calls`.

### Step 3: Split Capability Implementations (remaining)

Move `engine/handler-primitives.ts` into capability modules:

- `invocation/request.ts`;
- `invocation/steps.ts`;
- `invocation/clock.ts`;
- `invocation/state.ts`;
- `invocation/awakeables.ts`;
- `invocation/durable-promises.ts`;
- `invocation/service-communication.ts`.

Each module should expose a value object or constructor for one capability, not
a flattened service.

Service communication acceptance criteria:

- root `client` / `sendClient` remain available for callers outside handlers;
- handler-scoped `serviceClient` / `serviceSendClient` are available and derive
  deterministic child invocation ids from the active invocation context;
- handler-scoped `objectClient` / `objectSendClient` keep their existing
  deterministic child-call behavior;
- the migration must not remove typed service definitions or catalog
  compilation support.

### Step 4: Rename Object Executor

Rename:

```text
object/owner-driver.ts -> object/instance-executor.ts
ObjectOwnerDriver      -> ObjectInstanceExecutor
```

Then split implementation details:

- `object/stream.ts`: read/fold/append against S2 stream;
- `object/state-backend.ts`: handler-facing durable state backend;
- `object/promise-waiters.ts`: local durable-promise waiter registry and poke logic;
- `object/instance-executor.ts`: orchestration only.

### Step 5: Split Storage Ports

Replace `DurableStores` with:

- `S2Access`;
- `ServiceStore`;
- `ObjectStore`.

Validation:

- `engine/live.ts` and later executor modules depend on storage ports by purpose;
- object execution no longer appears as a field on a mixed store service.

### Step 6: Tighten Machine Exports

Replace `export *` from `object/machine/index.ts` with explicit exports. This
forces call sites to reveal whether they need machine protocol, object
addressing, or stream/path helpers.

## Acceptance Criteria

This axis is complete when:

- `DurableEngineApi` fits on one screen and contains only lifecycle/ingress
  methods;
- handler free primitives depend on `CurrentInvocationScope`, not
  `DurableEngine`;
- state mutation is reachable through a `DurableTable` value object, not
  flattened methods;
- child calls are reachable through object refs, not `callStep` and `sendStep`
  on an engine service;
- `object/owner-driver.ts` no longer exists;
- `engine/durable-stores.ts` no longer exists;
- `object/machine/index.ts` has explicit exports only;
- the generated dependency graph reads as capability groups, not smaller sock
  drawers.
