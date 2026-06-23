# Engine Boundaries Axis

## Public Engine Interface

The public service is `DurableEngine`. Treat it as the public durable
**engine** service and avoid using `runtime` as the architecture vocabulary.

Treat the API as three groups.

Lifecycle:

```ts
interface EngineLifecycleApi {
  readonly submit: DurableEngineApi["submit"]
  readonly attach: DurableEngineApi["attach"]
  readonly poll: DurableEngineApi["poll"]
  readonly workflowStart: DurableEngineApi["workflowStart"]
}
```

Primitives:

```ts
interface EnginePrimitiveApi {
  readonly runStep: HandlerPrimitivesApi["runStep"]
  readonly handlerRequest: HandlerPrimitivesApi["handlerRequest"]
  readonly sleepStep: HandlerPrimitivesApi["sleepStep"]
  readonly stateGet: HandlerPrimitivesApi["stateGet"]
  readonly stateSet: HandlerPrimitivesApi["stateSet"]
  readonly stateDelete: HandlerPrimitivesApi["stateDelete"]
  readonly awaitDeferred: HandlerPrimitivesApi["awaitDeferred"]
  readonly resolveLocal: HandlerPrimitivesApi["resolveLocal"]
  readonly resolvePromise: HandlerPrimitivesApi["resolvePromise"]
  readonly nextAwakeableId: HandlerPrimitivesApi["nextAwakeableId"]
}
```

Dispatch:

```ts
interface EngineDispatchApi {
  readonly resolveExternal: ResolutionRouterApi["resolveExternal"]
  readonly sharedCall: SharedObjectRunnerApi["sharedCall"]
  readonly callStep: ChildCallCoordinatorApi["callStep"]
  readonly sendStep: ChildCallCoordinatorApi["sendStep"]
}
```

`DurableEngineApi` is the public intersection of these groups. The
implementation should assemble it from internal boundaries rather than
implement everything inline.

## Target Directory Shape

```text
src/
  primitives.ts
  service.ts
  handler.ts
  types.ts
  schema.ts
  errors.ts

  engine/
    api.ts                  # public engine service tag and API types
    live.ts                 # live engine layer and implementation assembly
    kernel.ts               # recursive handler/API assembly once extracted
    address.ts
    context.ts
    durable-stores.ts
    handler-primitives.ts
    helpers.ts
    resolution-router.ts
    result-reader.ts
    service-deferreds.ts
    state.ts

  execution/
    service-executor.ts
    object-executor.ts
    child-call-coordinator.ts
    shared-object-runner.ts
    recovery-coordinator.ts

  object/
    machine/
      model.ts
      commands.ts
      index.ts
    log.ts
    drive-session.ts
    owner-driver.ts
    snapshots.ts

  ingress/
    server.ts
    client.ts
    contract.ts

  host.ts
  bin/
    host.ts
```

## Effect Service Pattern

Use services for ports/adapters, not for protocol decisions.

```ts
export class ServiceExecutor extends Context.Service<ServiceExecutor, {
  readonly submitService: (...) => Effect.Effect<void, DurableExecutionError, R>
  readonly recoverServices: Effect.Effect<void>
}>()("effect-s2-durable/execution/ServiceExecutor") {
  static readonly layerNoDeps = Layer.effect(
    ServiceExecutor,
    Effect.gen(function*() {
      const stores = yield* ServiceStores
      const state = yield* EngineState

      const submitService = Effect.fn("ServiceExecutor.submitService")(function*(...) {
        // ...
      })

      return ServiceExecutor.of({ submitService, recoverServices })
    }),
  )
}
```

Guidelines:

- use `Effect.fn("ServiceName.method")` for service methods;
- use `Layer.effect` for construction;
- use `Layer.scoped` only for resources requiring release;
- use `Layer.effectDiscard` for startup jobs that expose no API;
- use `Layer.provide` to hide implementation services;
- use `Layer.provideMerge` when the caller still needs the provided dependency;
- keep durable protocol decisions in the state machine.

## Engine Layer Composition

Target:

```ts
const EngineBaseLive = Layer.mergeAll(
  EngineState.layer(handlers, objectSeeds),
  S2Access.layer,
  ServiceStores.layer,
  ObjectStores.layer,
)

const EngineInternalLive = Layer.mergeAll(
  HandlerPrimitives.layerNoDeps,
  ResultReader.layerNoDeps,
  ResolutionRouter.layerNoDeps,
  ServiceExecutor.layerNoDeps,
  ObjectExecutor.layerNoDeps,
  ChildCallCoordinator.layerNoDeps,
  SharedObjectRunner.layerNoDeps,
  RecoveryCoordinator.layerNoDeps,
).pipe(
  Layer.provideMerge(EngineBaseLive),
)

export const DurableEngineLive = (
  handlers: ReadonlyArray<RegisteredHandler>,
  objectSeeds: ReadonlyArray<ObjectHandlerSeed>,
) =>
  Layer.effect(DurableEngine, makeEngine).pipe(
    Layer.provide(EngineInternalLive),
  )
```

There is a recursive knot: handlers need `DurableEngine`, while the
engine needs executors that run handlers. Do not force a fake acyclic graph.
Keep the knot in one small `EngineKernel`:

```ts
export class EngineKernel extends Context.Service<EngineKernel, {
  readonly makeApi: Effect.Effect<DurableEngineApi>
}>()("effect-s2-durable/engine/EngineKernel") {}
```

`EngineKernel` may assemble the final API object and pass it to executor
factories that need to inject `DurableEngine` into handlers.

## Internal Boundaries

### `HandlerPrimitives`

Owns `ActiveInvocation.kind` branching for durable primitives.

Allowed:

- `ActiveInvocation`;
- `S2Access` only when the primitive genuinely needs S2 client provisioning;
- `EngineState` for local waiters/counters;
- object state backend through `ObjectInvocation`.

Not allowed:

- service handler forking;
- object owner-drain logic;
- HTTP/host concerns;
- durable protocol decisions that belong in `object/machine/`.

### `ResultReader`

Owns `attach` and `poll`.

It reads service roster entries, local service deferred completions, and object
call status by self-routing object call id. It should not submit work, recover
work, or run handlers.

### `ResolutionRouter`

Owns external resolution routing:

- service signal / awakeable resolution;
- object signal resolution by object call id;
- error on unknown or invalid addresses.

It appends ingress facts; it should not own object draining.

### Executors And Coordinators

- `ServiceExecutor`: legacy service lifecycle.
- `ObjectExecutor`: object lifecycle above `ObjectOwnerDriver`.
- `ChildCallCoordinator`: deterministic child call ids and call/send flow.
- `SharedObjectRunner`: read-only shared handler execution.
- `RecoveryCoordinator`: engine-start recovery orchestration.
