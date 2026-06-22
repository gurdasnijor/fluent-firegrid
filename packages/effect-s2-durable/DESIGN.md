# effect-s2-durable Design Handoff

This package has one public durable execution engine:
`DurableExecutionRuntime`.

Several internal modules are named like runtime components, but they are not
independent runtimes. They are interpreters, stores, and object-log mechanics
that the single runtime composes.

The detailed canonical design still lives in:

- `docs/sdds/effect-durable-execution-sdd.md`
- `docs/sdds/effect-s2-durable-consolidation-sdd.md`
- `docs/sdds/effect-s2-durable-host-process-model-sdd.md`

This document is a compact map for implementation and refactoring work.

## Core Model

User code calls public free primitives such as `run`, `sleep`, `state`,
`signal`, `deferred`, `awakeable`, `attach`, and `poll`.

Those primitives do not own execution. They look up the ambient
`DurableExecutionRuntime` service and delegate to it.

`DurableExecutionRuntime` then interprets the operation according to the active
handler invocation:

| Invocation kind | Starts from | Durable backing | Writes state | Durable primitives |
| --- | --- | --- | --- | --- |
| `service` | plain execution id | per-execution `WorkflowDb` plus global roster | yes | yes |
| `object` | object call id | per-`(object,key)` owner `ActorEvent` log | yes, serialized | yes |
| `shared` | `sharedClient(...)` | folded object snapshot | no | mostly forbidden; `resolvePromise` is special ingress |

The unifying switchboard is `ActiveInvocation` in
`src/runtime/invocation.ts`. A handler is always run with exactly one active
invocation value. The primitive interpreter branches on that value.

## Runtime Composition

`src/Runtime.ts` is the composition root and the public runtime service.

It wires these internal services:

- `RuntimeState`: in-process engine state such as the runtime scope, handler
  registries, running service fibers, and local waiters.
- `RuntimeStores`: opened S2-backed stores and helpers, including the global
  roster, per-execution workflow DB opener, and object invocation store.
- `PrimitiveInterpreter`: implementation of durable primitives against the
  active invocation.
- `IngressRouter`: external signal / awakeable resolution routing.
- `CompletionReader`: `attach` / `poll` behavior for service and object ids.
- `InvocationStore`: object-owner admission, draining, status, signals, and
  snapshot reads.

Only `DurableExecutionRuntime` should be exported as the runtime. The others are
internal implementation services.

## Service Execution Path

A service execution is selected when `submit` receives a plain execution id.

Flow:

1. Encode handler input.
2. Open the execution's `WorkflowDb`.
3. Write the execution row and global roster row.
4. Fork the handler into the runtime scope.
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

| S2 pattern | effect-s2-durable object path |
| --- | --- |
| per-agent private stream | per-`(object,key)` owner stream |
| shared bus stream | `Accepted` and external `SignalResolved` ingress |
| private memory records | `StateChanged`, `Journaled`, and `Completed` |
| replay from `seqNum` | `replay(entries)` into `ActorSnapshot` |
| append session / producer | scoped owner drive session |
| fencing token | owner-driver write capability |

Do not push every correctness concern into per-append optimistic CAS. That is the
main complexity driver: if a `StateChanged`, `Journaled`, or `Completed` append
can hit a seq-num conflict in the middle of a user handler, the runtime needs
out-of-band abort/retry machinery to unwind and replay the handler. Keep that out
of the handler drive path.

Use the right tool for each concern:

| Concern | Preferred mechanism |
| --- | --- |
| cross-host stale owner writes | S2 fencing token |
| two drains racing in one process | small in-process per-owner lock |
| stale read re-running a settled head | in-process started guard / projection cache |
| call admission idempotency | CAS on `Accepted` admission |
| external signal ingress | append ingress event, optionally idempotent by projection |

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
    event: ActorEvent,
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

The `InvocationStore` owns object admission and draining:

1. `admit` CAS-appends `Accepted`, idempotent by call id.
2. `drain` opens a scoped owner drive session for the stream.
3. The drainer runs accepted calls serially by owner key.
4. The runtime resolves the method handler and runs it with
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

## Package Structure

The package should read as one engine with a few clear internal subsystems. The
current code is harder to follow because public surface, runtime composition,
primitive interpretation, service execution, object execution, ingress, and host
composition are spread across similarly named files.

Use directory names for concepts, not implementation history:

```text
src/
  Runtime.ts                 public runtime API + layer composition only
  primitives.ts              public free primitives
  service.ts                 public definitions and typed clients
  handler.ts                 low-level handler definition helper
  types.ts                   public type helpers
  schema.ts                  durable storage schemas

  runtime/
    invocation.ts            ActiveInvocation and invocation variants
    primitive-interpreter.ts implementation of public primitives
    durable-stores.ts        S2-backed store wiring
    completion.ts            attach / poll
    ingress-router.ts        resolveExternal routing
    service-execution.ts     service submit/run/complete/recover internals
    object-execution.ts      object submit/run-head/shared-call internals
    child-calls.ts           call/send child id rules

  object/
    events.ts                ActorEvent, ActorExit, projection fold
    log.ts                   typed owner-log read/write helpers
    drive-session.ts         scoped fenced owner writer
    invocation-store.ts      admission, drain, status, snapshot, owner keys

  ingress/
    server.ts                HTTP ingress adapter
    client.ts                HTTP client adapter

  host.ts                    Node host composition; the only Node-bound module
```

Do not move everything at once. Use this as the destination shape. Prefer small
renames/splits that make ownership clear and keep the behavior unchanged.

### Code Conventions

- Keep public free functions thin. `src/primitives.ts` should only delegate to
  `DurableExecutionRuntime`.
- Keep `Runtime.ts` shallow. It should assemble services and expose the API; long
  bodies such as service execution, object execution, child calls, and recovery
  should live in named internal modules.
- Keep all `ActiveInvocation.kind` branching in the primitive interpreter or in
  explicitly named execution modules. Avoid scattering `active.kind === ...`
  checks across unrelated files.
- Use one file per durable boundary:
  - public definition/client boundary: `service.ts`;
  - runtime API boundary: `Runtime.ts`;
  - primitive interpreter boundary: `runtime/primitive-interpreter.ts`;
  - object owner-log boundary: `object/invocation-store.ts`;
  - host/process boundary: `host.ts`.
- Keep object event vocabulary and projection pure. `object/events.ts` should not
  import S2, runtime services, or host concerns.
- Keep S2 IO helpers small and mechanical. `object/log.ts` should not know about
  handlers, state tables, workflows, or runtime registries.
- Keep owner-driver state in `object/drive-session.ts`. Handler state backends
  should receive an append function, not raw S2 tokens or tail refs.
- Keep ingress adapters at the edge. HTTP server/client code should not leak into
  runtime internals.
- Keep host composition at the edge. Node imports belong in `host.ts` and bin
  entrypoints, not the engine core.
- Prefer named internal APIs over large closures. A function like
  `runServiceExecution`, `makeObjectRunHead`, or `openOwnerDriveSession` is easier
  to test and discuss than a long nested closure inside `makeRuntime`.
- Prefer package-local vocabulary consistently: service, object, shared handler,
  workflow, owner log, ingress append, owner drive session, projection, drainer.

## Coherence Plan

The current behavior is coherent, but the file names make the design feel like
several runtimes. Prefer refactors that make the architecture explicit without
changing semantics.

This cleanup should stay subordinate to the host process model SDD. The host SDD
defines the deployable target: a namespace-scoped worker process that composes
`S2Client`, `serviceLayer(...catalog)`, optional ingress, boot recovery, and
eventual fenced multi-host ownership. Naming and file moves are useful only if
they make that target easier to build.

Recommended cleanup order:

1. Keep `DurableExecutionRuntime` as the only public runtime service.
2. Add the host composition from the host SDD without importing Node platform
   modules into the engine core:
   - `effect-s2-durable/host`
   - env/config driven `S2Client.layerConfig`
   - catalog-driven `serviceLayer(...catalog)`
   - optional `durableIngress(catalog)`
   - run-forever `startHost`
3. Introduce an `OwnerDriveSession` abstraction before further OCC/fencing work:
   - generate fixed-size S2 fencing tokens (`<= 36` UTF-8 bytes);
   - claim/fence, append owner-driver events with `fencingToken`, and classify
     lost-fence failures in one place;
   - do not put per-append `matchSeqNum` on every state/journal/completion write;
   - keep per-owner local serialization and projection caching for intra-host
     races and stale reads;
   - keep admission and external signals as ingress appends outside the owner
     session;
   - log forked drainer failures so attach timeouts do not hide owner-driver
     crashes.
4. Rename internal modules to describe their role:
   - `src/runtime/primitives.ts` -> `src/runtime/primitive-interpreter.ts`
   - `src/runtime/stores.ts` -> `src/runtime/durable-stores.ts`
   - `src/actor/core.ts` -> `src/object/events.ts`
   - `src/actor/log.ts` -> `src/object/log.ts`
   - `src/actor/object.ts` -> `src/object/invocation-store.ts`
5. Split `Runtime.ts` after names are clear and keep it as the composition root:
   - service execution and service recovery
   - object execution and object recovery
   - shared calls
   - durable child calls (`call` / `send`)
   - runtime API and layer composition
6. Keep public free primitives in `src/primitives.ts`; keep their interpreter in
   the runtime internals.
7. Keep workflow code visibly object-backed. Workflow lifecycle helpers can stay
   public, but runtime internals should route through object call ids and owner
   streams.

## Recovery Model

Separate two concerns that are easy to conflate:

- **state rebuild**: how cheaply a host can reconstruct an owner projection when
  it opens an owner stream;
- **ownership takeover**: how quickly a peer decides a different host died while
  driving that stream.

The default plan should optimize state rebuild first and keep liveness simple:

1. Keep S2 fencing for cross-host writer correctness.
2. Add S2 snapshots to bound owner-log replay cost. A snapshot is a materialized
   projection at a stream cursor; recovery loads the snapshot, then reads from
   that cursor.
3. Add trimming after snapshots when history covered by the snapshot no longer
   needs to be retained. Trimming is compaction, not ownership.
4. Use restart-based recovery as the default liveness model. On process restart,
   boot recovery enumerates owner streams and re-drives pending heads from the
   snapshot-plus-tail projection.

Snapshots and trimming do not replace fencing: they make replay cheap. They also
do not detect a dead writer. They reduce the need for prompt peer takeover by
making restart recovery fast enough for the default deployment model.

Lease, heartbeat, and proactive claim-sweep are therefore optional, not the next
default milestone. They are only justified if the product requires prompt,
coordinator-free peer takeover of a crashed host's in-flight owner stream without
waiting for that host to restart. If that requirement appears later, design it as
an explicit recovery mode:

- **lease**: a peer may consider an owner abandoned after no owner write for
  `leaseDurationMs`;
- **heartbeat**: a live owner periodically writes so slow active work is not
  mistaken for a dead process;
- **release-on-park**: preferred for long signal/human waits, so parked handlers
  do not hold ownership just to heartbeat an idle stream.

Do not build heartbeat without lease-based takeover; it has no independent
correctness role. Do not build lease-based takeover without deciding the
operational tradeoff: quicker peer recovery versus false-positive ownership
churn for slow-but-live work.

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

- **runtime**: only the public `DurableExecutionRuntime`.
- **interpreter**: logic that implements primitives for an active invocation.
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

Avoid names that imply new runtimes unless a component really owns execution
end-to-end.

## Build Agent Guardrails

- Preserve the public API unless the task explicitly requests a breaking change.
- Keep object call ids self-routing; do not add a side index for object status.
- Keep object state, primitive journals, signals, and completions on the owner
  log.
- Keep file/module responsibilities narrow. Do not add more long nested runtime
  closures when a named internal module would make the behavior clearer.
- Keep public surface, runtime internals, object log mechanics, ingress adapters,
  and host composition in separate directories/modules.
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
