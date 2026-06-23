# effect-s2-durable Design Handoff

This package has one public durable execution engine:
`DurableEngine`.

There is no separate `Runtime` boundary. Internal code should keep engine
internals together under `engine/` as semantic modules, with separate vertical
boundaries for `object`, `ingress`, and `host`.

The detailed canonical design still lives in:

- `docs/sdds/effect-durable-execution-sdd.md`
- `docs/sdds/effect-s2-durable-consolidation-sdd.md`
- `docs/sdds/effect-s2-durable-host-process-model-sdd.md`

This document is a compact map for implementation and refactoring work.

For the detailed refactor plan that applies Effect layer-composition guidance
and the Restate partition-worker responsibility split, see
[`REARCHITECTURE.md`](./REARCHITECTURE.md), the index for the focused
rearchitecture sub-documents.

The most important structural rule: durable semantics belong in a state machine,
not smeared across Effect services. Services should be ports/drivers around the
machine: S2 reads/appends/fencing, handler execution, timers, waiters, ingress,
and host lifecycle.

## Core Model

User code calls public free primitives such as `run`, `sleep`, `state`,
`signal`, `deferred`, `awakeable`, `attach`, and `poll`.

Those primitives do not own execution. They look up the ambient
`DurableEngine` service and delegate to it.

`DurableEngine` then interprets the operation according to the active
handler invocation:

| Invocation kind | Starts from         | Durable backing                               | Writes state    | Durable primitives                                    |
| --------------- | ------------------- | --------------------------------------------- | --------------- | ----------------------------------------------------- |
| `service`       | plain execution id  | per-execution `WorkflowDb` plus global roster | yes             | yes                                                   |
| `object`        | object call id      | per-`(object,key)` owner `ActorEvent` log     | yes, serialized | yes                                                   |
| `shared`        | `sharedClient(...)` | folded object snapshot                        | no              | mostly forbidden; `resolvePromise` is special ingress |

The unifying switchboard is `ActiveInvocation`, which lives under the `engine`
boundary. A handler is always run with exactly one active invocation value.
`HandlerPrimitives` branches on that value.

## Engine Composition

`src/engine/api.ts` is the public engine API. The target
composition root is `engine/live.ts`, with the recursive handler/API assembly
isolated in `engine/kernel.ts`.

The engine wires these internal boundaries:

- `EngineState`: in-process engine state such as the engine scope, handler
  registries, running service fibers, and local waiters.
- `S2Access`, `ServiceStores`, and `ObjectStores`: opened S2-backed stores and
  helpers, including the global roster, per-execution workflow DB opener, and
  object owner driver.
- `HandlerPrimitives`: implementation of durable primitives against the
  active invocation.
- `ResolutionRouter`: external signal / awakeable resolution routing.
- `ResultReader`: `attach` / `poll` behavior for service and object ids.
- `ObjectOwnerDriver`: object-owner admission, draining, status, signals, and
  snapshot reads.

Only the public engine service should be exported from the root authoring
surface. The other engine services are internal implementation services.

## Service Execution Path

A service execution is selected when `submit` receives a plain execution id.

Flow:

1. Encode handler input.
2. Open the execution's `WorkflowDb`.
3. Write the execution row and global roster row.
4. Fork the handler into the engine scope.
5. Provide `ActiveInvocation` with `kind: "service"`.
6. On completion, write the result to the roster, drop the execution stream, and
   remove the in-process running entry.

For service invocations, primitive facts are stored in the per-execution
workflow DB:

- `run` writes `steps`.
- `state.get` journals reads in `stateReads`.
- `sleep` writes `clockWakeups`.
- `deferred` / `signal` / `awakeable` use `deferreds` plus local waiters.

Boot recovery queries the roster for running or suspended service executions,
looks up handlers by name, and re-runs them from the top. Durable primitive facts
short-circuit replay.

## S2 Owner Stream Pattern

The cleaner implementation frame is S2-native:

- an owner stream is the durable event log for one durable entity;
- the latest state is a replayed projection, not a second source of truth;
- external callers append ingress events to that stream;
- one owner driver claims the stream and appends state/journal/completion events;
- low-level S2 append conditions are hidden behind a small scoped writer.

This mirrors the S2 examples:

- agent/session examples use one stream as the replayable log for a run or
  private memory;
- the dinner-party example uses a shared bus stream plus per-agent private
  streams;
- producer/append-session examples hide batching, backpressure, and retry behind
  append sessions;
- the fencing example treats fencing as writer ownership, not as the event model.

For this package, map those concepts as:

| S2 pattern                | effect-s2-durable object path                    |
| ------------------------- | ------------------------------------------------ |
| per-agent private stream  | per-`(object,key)` owner stream                  |
| shared bus stream         | `Accepted` and external `SignalResolved` ingress |
| private memory records    | `StateChanged`, `Journaled`, and `Completed`     |
| replay from `seqNum`      | `replay(entries)` into `ActorSnapshot`           |
| append session / producer | scoped owner drive session                       |
| fencing token             | owner-driver write capability                    |

Do not push every correctness concern into per-append optimistic CAS. That is the
main complexity driver: if a `StateChanged`, `Journaled`, or `Completed` append
can hit a seq-num conflict in the middle of a user handler, the engine needs
out-of-band abort/retry machinery to unwind and replay the handler. Keep that out
of the handler drive path.

Use the right tool for each concern:

| Concern                              | Preferred mechanism                                       |
| ------------------------------------ | --------------------------------------------------------- |
| cross-host stale owner writes        | S2 fencing token                                          |
| two drains racing in one process     | small in-process per-owner lock                           |
| stale read re-running a settled head | in-process started guard / projection cache               |
| call admission idempotency           | CAS on `Accepted` admission                               |
| external signal ingress              | append ingress event, optionally idempotent by projection |

This is still S2-native: S2 is the durable source of truth and the arbiter for
cross-host ownership. The in-process guards are only for local scheduling and
read-after-write lag inside one resident host.

Do not let fence tokens, expected tails, and conflict refs leak through every
layer. Ideally, expected tails and conflict refs should not exist in the handler
state backend at all.

The intended internal shape is:

```ts
interface OwnerDriveSession {
  readonly token: string
  readonly append: (
    event: ActorEvent
  ) => Effect.Effect<number, FenceLost, S2Client>
  readonly refreshTail: Effect.Effect<void, DurableExecutionError, S2Client>
}
```

`OwnerDriveSession.append` is the only place that should combine:

- encoded `ActorEvent`;
- `fencingToken`;
- lost-fence classification;
- projection/tail bookkeeping after a successful append, if the implementation
  keeps local cached projection state.

Do not use `matchSeqNum` on every owner-driver append unless there is a very
specific reason. A mid-handler seq-num conflict is expensive to recover from.
The simpler owner-driver default is: local lock serializes same-process drains,
fencing rejects stale cross-host writers, and the resident drainer advances its
own projection without relying on a fresh read after each write.

Construction should be scoped: claim/fence the stream, read its current tail, open
any S2 append-session machinery if useful, and release/close it when the drainer
exits. If the codebase lacks a convenient typed append-session wrapper for
`ActorEvent`, keep the abstraction anyway and implement it with a fenced append
internally until the lower-level effect-s2 API can support the cleaner session.

Ingress is different from owner driving. Admission and external signal resolution
are bus-style appends from outside the owner driver. They may use CAS for
idempotent admission, but they should not require the current owner token.

## Object Execution Path

An object execution is selected when `submit`, `attach`, or `poll` receives an
object call id. Object call ids are schema-owned and self-routing: they encode
`{ object, key, method, nonce }`.

Each `(object,key)` has one owner stream:

```text
obj/<escaped object>/<escaped key>
```

The owner stream is an ordered `ActorEvent` log. Its projection is pure and
re-derivable by folding events.

Important events:

- `Accepted`: call admitted to the owner FIFO.
- `StateChanged`: durable user state mutation.
- `Journaled`: per-call primitive fact such as `run`, `sleep`, or read journal.
- `SignalResolved`: durable ingress / named promise resolution.
- `Completed`: terminal call result.

The `ObjectOwnerDriver` owns object admission and draining:

1. `admit` CAS-appends `Accepted`, idempotent by call id.
2. `drain` opens a scoped owner drive session for the stream.
3. The drainer runs accepted calls serially by owner key.
4. The engine resolves the method handler and runs it with
   `ActiveInvocation.kind === "object"`.
5. Object state and primitive journals append through the owner drive session.
6. The drainer appends `Completed` through the same session.

The owner log is the source of truth. In-process locks, snapshots, started sets,
and waiters are only caches or safety guards for the current process.

Keep the local per-owner drainer lock until there is a simpler proven replacement.
S2 fencing is the storage-level safety rail; the local lock prevents duplicate
same-process work before the first durable owner-driver write.

Keep the projection cache / started guard until there is a simpler proven
alternative. They solve local read-after-write lag and duplicate same-process
head selection without forcing seq-num conflicts into the middle of handler
execution.

## Shared Object Path

Shared calls are read-only object calls. They do not admit work and do not use
the exclusive drainer.

Flow:

1. Fold the owner log into an `ActorSnapshot`.
2. Run the shared handler with `ActiveInvocation.kind === "shared"`.
3. Allow reads from the snapshot.
4. Reject state writes and most durable primitives.

Shared handlers are intended for queries and signal-style ingress that should
not block on, or be blocked by, the exclusive object drainer.

## Workflows

Workflows are object specializations, not a third runtime.

A workflow's `run` is the single exclusive object method. Its id is
deterministic for `(workflow name, workflow id)` so start is run-once:

```text
{ object: workflowName, key: workflowId, method: "run", nonce: workflowId }
```

Workflow query / signal handlers are shared object handlers over the workflow
owner projection.

Avoid introducing `WorkflowRuntime` or workflow-specific storage unless it is a
thin facade over the object owner-log model.

## Package Structure & Coherence Plan

The structural plan — state-machine extraction, storage substrate, target
directory shape, module-rename map, service extraction steps, naming rules, and
code conventions — lives under **[`REARCHITECTURE.md`](./REARCHITECTURE.md)**.
This section previously duplicated it and drifted, so it has been collapsed to
this pointer.

## Host Process Alignment

The host process SDD is the production target for this package. Keep these points
in sync with it:

- The deployable is a long-running, namespace-scoped host worker. Namespace is
  the S2 basin.
- The engine core remains binding-free. Node runtime, Node context, and HTTP
  server modules belong behind the host subpath and bin entrypoint.
- The handler catalog is compiled into the worker. Definition values are the
  contract; v1 does not need dynamic remote registration.
- Ingress is optional and layered at the edge. The host can run headless as a
  recovery / embedded-caller worker.
- Object owner streams should use fenced ownership for write correctness. The
  default recovery path should be snapshot/trim-based state rebuild plus
  restart-driven liveness, not lease/heartbeat.
- Treat `effect-s2-stream-db` as the shared S2 storage substrate, but keep the
  stream/table split clear: object owner logs are ordered typed event streams;
  service/user state and read models are latest-value tables/materialized views.
  Expand stream-db with first-class `EventStream<ActorEvent>` support before
  adding more bespoke object-log IO in this package.
- Fencing should be encapsulated by owner drive sessions. Do not thread raw
  tokens, expected tails, or conflict refs through handler state backends.
- Per-append OCC inside a running handler is not the default architecture. It is
  a last resort for a narrow boundary, because mid-handler seq-num conflicts
  require abort/retry machinery.
- Service execution is still the legacy path: per-execution workflow DB, roster,
  in-process running map. The host SDD's target is to unify services onto the
  owner-stream/fence model and eventually retire the in-process ownership map.
- Boot recovery should evolve toward snapshot-aware owner recovery and timer
  re-arm. Proactive claim-sweep is a later optional mode for prompt peer
  takeover, not the default recovery mechanism.

## Naming Rules

Use these terms consistently:

- **engine**: the durable execution engine service. The public name is
  `DurableEngine`; internal modules should use engine vocabulary.
- **runtime**: avoid for package internals. Use it only for a language/platform
  runtime such as Node.
- **invocation context**: the active handler context: service, object, or shared.
- **handler primitives**: logic that implements `run`, `sleep`, `state`, signals,
  and awakeables for an active invocation.
- **executor**: lifecycle owner for service or object handler execution.
- **coordinator**: orchestration that routes child calls, recovery, or external
  resolutions without owning storage semantics.
- **result reader**: `attach` / `poll` lookup.
- **store**: opened durable storage dependencies.
- **owner log**: the S2 stream for one `(object,key)`.
- **ingress append**: an external bus-style append such as `Accepted` or external
  `SignalResolved`; it does not require the owner token.
- **owner drive session**: scoped writer that owns the current fence token and
  appends state/journal/completion events with the fencing token.
- **projection**: pure fold of an owner log.
- **drainer**: exclusive runner for pending calls on one owner log.
- **shared handler**: read-only handler over a projection.
- **workflow**: object-backed run-once entrypoint plus shared handlers.

Avoid names that imply new runtimes. Prefer semantic boundaries such as
`engine`, `invocation`, `execution`, `completion`, `signals`, `storage`, `object`,
`ingress`, and `host`.

## Build Agent Guardrails

- Preserve the public API unless the task explicitly requests a breaking change.
- Keep object call ids self-routing; do not add a side index for object status.
- Keep object state, primitive journals, signals, and completions on the owner
  log.
- Do not model ordered object history as a latest-value table keyed by seq num.
  Add/reuse a first-class typed event-stream abstraction in `effect-s2-stream-db`
  and keep tables for projections/read models.
- Keep file/module responsibilities narrow. Do not add more long nested engine
  closures when a named internal module would make the behavior clearer.
- Put object durable protocol decisions in `object/machine/` before
  adding more driver/services. `ObjectOwnerDriver` should trend toward "read/fold,
  decide, append, run emitted action" rather than owning the protocol itself.
- Treat Effect services as ports/adapters unless they are the public engine
  service. A service that both decides durable state and performs IO is probably
  hiding the state machine.
- Keep public surface, engine assembly, invocation context, execution lifecycle,
  completion reads, signal resolution, storage, object log mechanics, ingress
  adapters, and host composition in separate directories/modules.
- Do not add new implementation files under a generic `src/runtime` namespace.
  Use the semantic directory that describes the responsibility.
- Keep S2 ownership mechanics localized. `ObjectStateBackend` should call an
  owner-session append function, not know about fencing tokens, tail refs, or
  conflict refs.
- Avoid per-append `matchSeqNum` inside handler execution. Prefer local
  per-owner serialization plus fenced owner writes.
- Keep admission and external signal resolution as ingress appends; do not make
  callers know the current owner token.
- Keep fence tokens within S2's maximum length.
- Keep local per-owner serialization until a simpler proven replacement exists.
- Keep service recovery roster-backed until deliberately migrated.
- Do not strengthen the current service `running` map as the long-term
  ownership mechanism; it is transitional under the host process SDD.
- Prefer snapshots and trimming for replay cost before adding lease/heartbeat
  machinery.
- Treat lease + heartbeat + claim-sweep as optional, only for prompt
  coordinator-free peer takeover.
- Do not let shared handlers mutate user state.
- Do not make workflows independent from the object owner-log model.
- When moving files, update imports in the smallest possible patch and run the
  narrowest relevant `pnpm` validation.
