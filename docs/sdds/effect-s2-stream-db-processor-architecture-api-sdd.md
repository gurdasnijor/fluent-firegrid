# SDD: Processor Architecture API for effect-s2-stream-db

| | |
|---|---|
| **Status** | Draft for implementation |
| **Date** | 2026-06-23 |
| **Primary package** | `packages/effect-s2-stream-db` |
| **Primary consumer** | `packages/effect-s2-durable` |
| **References** | Pulsar IO sources/sinks, Pulsar Functions, Pulsar transactions, pulsar-client-dotnet |

## 1. Purpose

Define a processor architecture API for `effect-s2-stream-db` that can support:

- typed append-only streams;
- external source connectors that write to streams;
- external sink connectors that consume streams;
- function-style processors that consume one or more streams and publish to one
  or more streams;
- durable checkpoints, materialized state, and explicit transaction boundaries.

The design target is not a Pulsar clone. Pulsar is the vocabulary reference:
sources feed data into the log, sinks move data out of the log, functions
consume from topics, execute user logic, and publish outputs. The local target
is an S2-backed, Effect-native substrate that `effect-s2-durable` can use for
object logs, activity request streams, activity result streams, durable promise
events, and future read models.

## 2. Problem

The current storage discussion is too centered on "an EventStream helper for
object logs." That is necessary but not sufficient.

`effect-s2-durable` needs a broader streaming substrate:

```text
HTTP / SDK ingress
  -> durable invocation streams
  -> processor logic
  -> durable output streams
  -> sink processors that perform side effects
  -> durable result streams
  -> invocation processor resumes
```

If stream-db only exposes a bag-shaped `EventStreamInstance` with `read`,
`append`, and `materialize`, durable will keep reinventing queues, output
topics, result routing, checkpoints, and idempotency on top. The package needs
clearer semantic pieces:

- source;
- sink;
- stream;
- processor;
- checkpoint;
- state store / table view;
- transaction.

## 3. Reference Model

### 3.1 Pulsar IO

Pulsar IO defines two connector directions:

| Pulsar term | Direction | Local term |
| --- | --- | --- |
| Source | external system -> Pulsar topic | external system -> S2 event stream |
| Sink | Pulsar topic -> external system | S2 event stream -> external system |

Pulsar also names processing guarantees for connectors: at-most-once,
at-least-once, and effectively-once. It explicitly notes that sink guarantees
depend on the sink implementation and the external system's retry/idempotency
behavior. We should keep that honesty in our API.

### 3.2 Pulsar Functions

A function instance has:

- consumers for one or more input topics;
- an executor that invokes user logic;
- a producer that sends outputs to an output topic;
- function-scoped state.

This maps well onto stream-db:

```text
EventSource(s)
  -> ProcessorFunction
  -> EventSink(s)
  -> CheckpointStore + StateStore
```

### 3.3 Pulsar Transactions

Pulsar transactions combine consume, process, produce, and acknowledge under a
transaction coordinator. S2 does not provide that broker-side transaction
coordinator directly, so the local API must not claim broker-equivalent
semantics unless we implement a coordinator.

The useful design lessons are:

- a transaction has an identity and lifecycle;
- output visibility and input checkpoint movement should be coupled;
- transaction state must be durable;
- aborted work must not advance the input checkpoint;
- commit finalization must be idempotent.

### 3.4 pulsar-client-dotnet

`fsprojects/pulsar-client-dotnet` is a useful client-library shape reference
because it exposes a large subset of practical Pulsar capabilities: producer,
consumer, reader, batching, schemas, multi-topic consumers, key-shared
subscriptions, transactions, effectively-once behavior, delayed messages, dead
letter policy, interceptors, statistics, and table view.

We should not copy that API literally. The useful lesson is breadth and
separation of roles: producer, consumer, reader, transaction, and table view are
distinct surfaces. We should avoid collapsing those roles into one generic
"stream instance" object.

## 4. Requirements

1. Make append-only streams, sources, sinks, processors, checkpoints, and state
   stores first-class concepts in `effect-s2-stream-db`.
2. Model the source direction explicitly: external systems produce records into
   S2 event streams through typed source connectors.
3. Model the sink direction explicitly: S2 event streams drive external systems
   through typed sink connectors with declared processing guarantees.
4. Model function-style processing explicitly: a function consumes one or more
   input streams, applies user-defined Effect logic, and publishes outputs to
   one or more output streams.
5. Give `effect-s2-durable` an activity-stream architecture:
   durable handlers emit activity requests; sink processors execute activities;
   result streams resume invocations.
6. Support one-input and multi-input processors without forcing every caller
   into the same high-level abstraction.
7. Preserve Effect semantics: resource lifetimes through `Scope`/`Layer`,
   expected failures as typed errors, processing logic as `Effect`.
8. Model processing guarantees explicitly and conservatively.
9. Introduce a transaction API boundary early, even if v1 implements only an
   S2-backed transactional outbox/checkpoint protocol rather than full
   cross-stream atomicity.
10. Couple output publication and input checkpoint movement through the
    transaction/checkpoint protocol.
11. Require deterministic ids for processor outputs that need effectively-once
    semantics.
12. Keep `effect-s2` as the raw transport wrapper. Stream-db must use it; it must
   not re-wrap the upstream SDK again.

## 5. Non-Goals

1. Do not implement Pulsar's broker, worker scheduler, subscription protocol, or
   transaction coordinator.
2. Do not promise exactly-once arbitrary external side effects. The realistic
   guarantee is exactly-once durable intent/result and effectively-once external
   behavior only when the sink and external system use idempotency keys.
3. Do not make `effect-s2-durable` own generic stream processing machinery.
4. Do not hide source, sink, processor, and transaction semantics inside a
   single convenience object.
5. Do not add SQL or relational planning in this slice. The relational IVM SDD
   can build on top of this substrate later.

## 6. Load-Bearing Decisions

1. **Stream-db owns the processor substrate.** Durable owns durable invocation
   semantics, not generic event stream IO or processor execution mechanics.
2. **Streams are facts, tables are state.** `EventStream<A>` is ordered history;
   `Table<K, V>` / `TableView<K, V>` is latest state or a materialized view.
3. **Source and sink are directions, not data structures.** A source writes into
   a stream. A sink consumes from a stream. A processor can contain both.
4. **Processors are declared topologies.** A processor declares inputs, outputs,
   state stores, checkpoint store, processing guarantee, and handler.
5. **Functions are a processor subset.** A function is a named processor with
   stream inputs, user logic, and stream outputs. Do not introduce a separate
   function runtime if the processor runtime can host it.
6. **Transactions are an API concept from v1.** The first implementation can be
   the single-stream output-plus-checkpoint couple. Multi-stream transaction
   logs are reserved for later; v1 must not introduce a coordinator or
   read-committed isolation protocol.
7. **Key affinity is first class.** Durable object and invocation streams need
   key-ordered processing. The processor API must model key selection and
   assignment instead of making callers create ad hoc locks.
8. **Per-key physical streams are load-bearing.** The current `StreamDb` model
   opens `${basePath}/${encode(key)}`: one physical S2 stream, one ordered tail,
   one in-memory fold, and one CAS sequence per key. Durable execution should
   extend this sharded model, not replace it with global domain streams and
   logical keys.
9. **Materialization is a consumer of sources.** Materializers consume
   `EventSource<A>` and write `Table`/`TableView` state. They are not hardwired
   into the event stream handle.
10. **TableView is read-only.** Following Pulsar's `TableView`, latest-value
   reads are a separate surface from writes. `TableStore` writes changelog facts;
   `TableView` observes facts and exposes a live key-value map.
11. **TableView is not deterministic handler state.** Table views are for
    query/read-model surfaces and bounded-cardinality subscriptions. Durable
    handler state must come from the same key-scoped stream fold at the current
    processing position.
12. **Refresh is a semantic boundary.** A `TableView.refresh` operation must mean
    "fold through at least this source cursor before serving subsequent reads."
    It is useful for query freshness, but it is not a deterministic replay
    primitive.
13. **Fencing plus CAS is the writer model.** CAS protects stream corruption.
    Fencing provides single-active-writer ownership for a key stream. Keyed
    table stores and processors must be able to thread an owner fencing token
    into every append.

## 7. Conceptual Stack

```text
effect-s2
  S2 client, append, guarded append, read, producers, sessions, snapshots, trim

effect-s2-stream-db
  EventStream declarations
  EventSource / EventSink
  KeyedEventStore: one physical stream per key
  SourceConnector / SinkConnector
  Processor / Function
  CheckpointStore
  SingleStreamTransactionScope
  TableStore / TableView / Materializer

effect-s2-durable
  Keyed invocation processors
  Keyed object processors
  Activity sink processors
  Keyed durable promise facts
  Restate-like authoring APIs
```

## 8. Core API Shape

The examples below are target shapes, not final TypeScript signatures. The
important part is the semantic split.

### 8.1 Event Stream Declaration

```ts
const AuditEvents = StreamDb.eventStream("audit.events", {
  key: AuditEventId,
  value: AuditEvent,
})

const CucumberEnvelopes = StreamDb.eventStream("cucumber.envelopes", {
  key: CucumberRunId,
  value: CucumberEnvelope,
})
```

`EventStream<K, A>` is a declaration. It carries name, key schema, value schema,
and naming/path rules. It is not the opened stream instance.

### 8.1.1 Keyed Event Store

Durable processors should normally use a keyed event store: one physical S2
stream per key, derived through the key schema.

```ts
const InvocationInbox = StreamDb.keyedEventStore("durable/invocation", {
  key: InvocationId,
  value: InvocationRecord,
})

const ObjectInbox = StreamDb.keyedEventStore("durable/object", {
  key: ObjectKey,
  value: ObjectRecord,
})
```

Opening `InvocationInbox.open(invocationId)` targets one physical stream:

```text
durable/invocation/{encode(invocationId)}
```

This is the durable execution default because it preserves:

- one ordered inbox per durable key;
- one tail cursor;
- one local fold at the processing position;
- bounded in-memory state per opened key;
- CAS and fencing scoped to that key stream.

Global event streams remain useful for bounded read models, exports, and
connector-style integration. They are not the default stateful durable execution
path.

### 8.2 Records, Cursors, And Acks

```ts
interface EventRecord<K, A> {
  readonly stream: string
  readonly key: K
  readonly value: A
  readonly cursor: EventCursor
  readonly headers: ReadonlyMap<string, string>
  readonly eventTime?: Date
}

interface EventCursor {
  readonly stream: string
  readonly seqNum: number
}

interface EventAppendAck {
  readonly stream: string
  readonly first: EventCursor
  readonly next: EventCursor
}
```

The cursor is the portable checkpoint unit. Durable code should not need to know
raw S2 record details to checkpoint progress.

### 8.3 Event Source

```ts
interface EventSource<K, A> {
  readonly stream: EventStream<K, A>
  readonly read: (range: EventRange) => Stream.Stream<EventRecord<K, A>, StreamDbError>
  readonly tail: (from: EventCursor) => Stream.Stream<EventRecord<K, A>, StreamDbError>
}
```

An event source is a read capability. It is used by processors, materializers,
table views, and durable recovery.

### 8.4 Event Sink

```ts
interface EventSink<K, A> {
  readonly stream: EventStream<K, A>
  readonly append: (
    event: EventEnvelope<K, A>,
    options?: EventAppendOptions,
  ) => Effect.Effect<EventAppendAck, StreamDbError>
  readonly appendBatch: (
    events: ReadonlyArray<EventEnvelope<K, A>>,
    options?: EventAppendOptions,
  ) => Effect.Effect<EventAppendAck, StreamDbError>
}
```

An event sink is a write capability. Guarded append and fencing remain delegated
to `effect-s2`; stream-db only exposes them as typed stream operations.

### 8.5 Source Connector

A source connector moves data from outside S2 into an event stream.

```ts
interface SourceConnector<OutK, Out> {
  readonly name: string
  readonly output: EventStream<OutK, Out>
  readonly run: (
    sink: EventSink<OutK, Out>,
  ) => Effect.Effect<void, StreamDbError, Scope.Scope>
}
```

Examples:

- Cucumber NDJSON file -> `cucumber.envelopes`;
- webhook receiver -> `webhook.events`;
- external queue consumer -> `activity.results`.

### 8.6 Sink Connector

A sink connector moves records from an event stream to an external system.

```ts
interface SinkConnector<InK, In> {
  readonly name: string
  readonly input: EventStream<InK, In>
  readonly guarantee: ProcessingGuarantee
  readonly run: (
    source: EventSource<InK, In>,
    checkpoint: CheckpointStore,
  ) => Effect.Effect<void, StreamDbError, Scope.Scope>
}
```

Examples:

- `activity.requests` -> external activity executor;
- `notifications` -> email provider;
- `audit.events` -> external warehouse.

### 8.7 Processor Function

```ts
interface ProcessorFunction<I, O, R> {
  readonly process: (
    input: I,
  ) => Effect.Effect<ProcessorOutput<O>, ProcessorFailure, R>
}

interface ProcessorOutput<O> {
  readonly records: ReadonlyArray<O>
  readonly state?: ReadonlyArray<StateMutation>
}
```

The function consumes typed records and returns outputs. It does not directly
own S2 clients or global runtime APIs.

Processor capabilities should be provided through the Effect `R` channel, not a
dynamic stringly context bag. Declared outputs, views, state stores, and
checkpoint services become typed services available to the handler. A handler
cannot emit to an undeclared output because the service is not in its
environment.

### 8.8 Processor Declaration

```ts
const AuditProjector = StreamDb.processor("audit.projector", {
  input: AuditEvents,
  output: AuditRows,
  guarantee: ProcessingGuarantee.atLeastOnce,
  handler: Effect.fn("AuditProjector.handler")(function*(input) {
    const output = yield* AuditRowsOutput
    yield* output.append({
      key: input.value.id,
      value: toAuditRow(input.value),
    })
  }),
})
```

The processor declaration is the unit the host runs. It has explicit inputs,
outputs, state, and guarantee. Stateful durable processors use
`keyedProcessor`, not arbitrary multi-input `processor`.

## 9. Processing Guarantees

Use Pulsar's names for generic source/sink processing, but do not let the
generic substrate imply deterministic replay. Generic processors are
at-least-once unless they use stricter durable primitives.

| Guarantee | Local meaning |
| --- | --- |
| `atMostOnce` | checkpoint may advance before processing or output is durable; records can be skipped on failure |
| `atLeastOnce` | input checkpoint advances only after processing completes; outputs or side effects can repeat |
| `idempotentOutput` | processing may repeat, but deterministic output ids plus guarded append / append-if-absent ensure one durable output fact per id |

For external sinks, effectively-once external behavior additionally requires an
idempotent sink contract:

```ts
interface IdempotentSinkRequest {
  readonly idempotencyKey: string
}
```

Without an external idempotency key or compare-and-set equivalent, the sink is
`atLeastOnce` no matter how strong the S2-side protocol is.

Durable execution earns stronger behavior only through a narrower deterministic
processor profile:

```ts
interface DeterministicProcessorRequirements {
  readonly input: "single-keyed-physical-stream"
  readonly reads: "local-fold-at-input-position"
  readonly writes: "deterministic-id-guarded-append"
  readonly owner: "fenced-single-active-writer"
  readonly checkpoint: "conditional-single-stream-cursor"
}
```

This profile belongs to `effect-s2-durable` and keyed stream-db processors, not
to arbitrary multi-input stream processing.

## 10. Transaction Model

### 10.1 Public Concept

```ts
interface Transaction {
  readonly id: TransactionId
  readonly append: <K, A>(
    sink: EventSink<K, A>,
    event: EventEnvelope<K, A>,
  ) => Effect.Effect<void, StreamDbError>
  readonly checkpoint: (
    input: EventRecord<unknown, unknown>,
  ) => Effect.Effect<void, StreamDbError>
  readonly commit: Effect.Effect<TransactionCommitAck, StreamDbError>
  readonly abort: Effect.Effect<void, StreamDbError>
}
```

The API models the same lifecycle shape as Pulsar:

1. begin transaction;
2. consume/process/produce;
3. acknowledge by recording checkpoint movement;
4. commit or abort;
5. recover unfinished transactions on restart.

### 10.2 V1 Implementation

S2 does not currently give us a native multi-stream transaction coordinator. V1
must not introduce a transaction log, materialization hop, or read-committed
isolation protocol before a durable use case requires it.

The v1 transaction implementation is the single-stream couple:

1. append output facts to the keyed stream with deterministic ids;
2. use guarded append / append-if-absent for terminal facts that must be unique;
3. commit the processor cursor conditionally on the previous checkpoint cursor;
4. on restart, re-read from the committed cursor and re-emit deterministic
   outputs safely.

The current `StreamDb.transact` shape is the precedent: buffer table intents,
encode them, and append one CAS-guarded S2 batch to one physical stream. The
current `checkpoint` shape is also the precedent: append snapshot records and a
trim command in one batch, so the durable replacement lands before destructive
trim.

Multi-stream transaction APIs remain reserved:

```ts
interface Transaction {
  readonly id: TransactionId
  readonly append: ...
  readonly checkpoint: ...
  readonly commit: ...
  readonly abort: ...
}
```

Do not implement `TransactionOpened`, `TransactionCommitted`, or generic
multi-stream output materialization in v1.

### 10.3 Durable Activity Use Case

Activity execution should use transactions as durable intent, not as magic
external exactly-once:

```text
Invocation processor:
  append ActivityIntent.Requested(activityId, idempotencyKey)
  to durable/activity/{activityId}
  checkpoint invocation progress

Activity sink processor:
  read durable/activity/{activityId}
  call external activity with idempotencyKey
  append-if-absent ActivityFact.Completed or ActivityFact.Failed
  append completion intent to durable/invocation/{invocationId}
  checkpoint activity cursor

Invocation processor:
  read completion intent from durable/invocation/{invocationId}
  resume invocation
```

If the process crashes after the external call but before the terminal activity
fact is durable, the sink can retry. The external activity must treat
`idempotencyKey` as the dedupe key for effectively-once side effects.

## 11. Checkpoints

Every processor instance needs a durable checkpoint per input assignment:

```ts
interface CheckpointStore {
  readonly load: (processor: ProcessorId, input: InputId) =>
    Effect.Effect<Option.Option<EventCursor>, StreamDbError>
  readonly commit: (
    processor: ProcessorId,
    input: InputId,
    cursor: EventCursor,
    options?: CheckpointCommitOptions,
  ) => Effect.Effect<void, StreamDbError>
}
```

Checkpoint commits should be conditional on the previous cursor when possible.
That gives processors idempotent restart and prevents stale instances from
rewinding or skipping input.

The checkpoint store should be backed by a dedicated checkpoint stream, not by
the table API. Tables and table views may depend on processor checkpoints, so
using tables as the checkpoint substrate creates a layering cycle.

S2 checkpoint compaction means snapshot-plus-trim, not assumed Kafka-style
key-compaction:

```text
checkpoint stream
  -> periodic checkpoint snapshot record
  -> trim records before the snapshot cursor
```

Snapshot cadence is therefore a real tuning knob for cold-start cost.

## 12. State Stores And Table Views

The current `packages/effect-s2-stream-db/src/StreamDb.ts` is already closest
to this layer. It preloads an S2 stream, folds table change records into
`MaterializedState`, exposes latest-value reads, and supports
checkpoint/compact/trim. The restructure should preserve that logic but split
the public roles.

### 12.1 Table Store

A table store is the write side. It appends table changelog facts.

```ts
const Attempts = StreamDb.table("durable.activity.attempts", {
  key: ActivityId,
  value: ActivityAttempt,
})
```

Target write surface:

```ts
interface TableStore<K, V> {
  readonly insert: (value: V) => Effect.Effect<void, S2StreamDbError>
  readonly insertOrGet: (value: V) => Effect.Effect<InsertOrGetResult<V>, S2StreamDbError>
  readonly upsert: (value: V) => Effect.Effect<void, S2StreamDbError>
  readonly delete: (key: K) => Effect.Effect<void, S2StreamDbError>
  readonly batch: <A>(body: (batch: TableBatch) => A) => Effect.Effect<A, S2StreamDbError>
}
```

The current `TableFacade.insert/upsert/delete/insertOrGet/transact` maps here.
Rename the current `Transaction` type to `TableBatch`; reserve `Transaction`
for processor-level output-plus-checkpoint commits.

### 12.2 Table View

A table view is the read side. It observes a changelog/event stream and exposes
a latest-value map. This follows Pulsar's `TableView` shape: map-like reads,
separate snapshot iteration, live listeners, and explicit refresh.

An in-memory `TableView` is appropriate for bounded-cardinality read models. It
must not be used as the correctness gate for dedupe, and it must not be the
state source for deterministic durable handler execution. Durable handlers read
the local fold of their keyed physical stream at the processing position.

```ts
interface TableView<K, V> {
  readonly size: Effect.Effect<number, S2StreamDbError>
  readonly isEmpty: Effect.Effect<boolean, S2StreamDbError>
  readonly containsKey: (key: K) => Effect.Effect<boolean, S2StreamDbError>
  readonly get: (key: K) => Effect.Effect<Option.Option<V>, S2StreamDbError>
  readonly entries: Effect.Effect<ReadonlyArray<readonly [K, V]>, S2StreamDbError>
  readonly keys: Effect.Effect<ReadonlyArray<K>, S2StreamDbError>
  readonly values: Effect.Effect<ReadonlyArray<V>, S2StreamDbError>
  readonly snapshot: Effect.Effect<ReadonlyMap<K, V>, S2StreamDbError>
  readonly changes: Stream.Stream<TableViewChange<K, V>, S2StreamDbError>
  readonly snapshotAndChanges: Stream.Stream<TableViewChange<K, V>, S2StreamDbError>
  readonly refresh: Effect.Effect<EventCursor, S2StreamDbError>
}
```

Semantics:

- `snapshot` is the current in-memory latest-value map at the view's folded
  cursor.
- `changes` emits future changes only.
- `snapshotAndChanges` emits the current map as changes, then follows future
  changes. This is the Effect-native equivalent of Pulsar's `forEachAndListen`.
- `refresh` reads the source tail, folds through that cursor, and returns the
  cursor. Subsequent reads from this view must be at least as fresh as that
  cursor, though they may include newer updates.
- `refresh` is a freshness lower bound, not an exact replay cursor. It is
  unsuitable for deterministic branch decisions during replay.

`TableView` is acquired and released through Effect scope/layers. Do not expose
`close()` as a normal user operation.

### 12.3 Processor State

Processors may declare state stores and table views:

```ts
const AttemptsStore = StreamDb.tableStore("durable.activity.attempts", {
  key: ActivityId,
  value: ActivityAttempt,
})

const AttemptsView = StreamDb.tableView("durable.activity.attempts.view", {
  source: AttemptsStore.changelog,
  key: (change) => change.key,
  fold: foldLatestValue,
})
```

State stores and views serve two roles:

- internal processor state, such as attempts, dedupe records, and assignment;
- queryable materialized views for callers.

This keeps "table view" out of durable-specific code. Durable can define
domain-specific tables, but stream-db owns the generic latest-value mechanics.

### 12.4 StreamDb.ts Restructure

`StreamDb.ts` should become a small facade over role-specific modules:

```text
src/
  StreamDb.ts

  table/
    definition.ts
    table-store.ts
    table-view.ts
    change.ts
    materialized-state.ts
    checkpoint.ts

  event-stream/
    definition.ts
    source.ts
    sink.ts
    keyed-store.ts
    record.ts
    admin.ts

  processor/
    definition.ts
    runner.ts
    checkpoint-store.ts
    guarantees.ts
    assignment.ts

  transaction/
    definition.ts
    log.ts
    scope.ts
```

Facade target:

```ts
export const StreamDb = {
  table: Table.define,
  tableStore: TableStore.define,
  tableView: TableView.define,
  eventStream: EventStream.define,
  keyedEventStore: KeyedEventStore.define,
  processor: Processor.define,
  sinkProcessor: Processor.sink,
  sourceConnector: Processor.source,
}
```

Keep from current `StreamDb.ts`, but move:

| Current item | Target module |
| --- | --- |
| `primaryKey`, `Table`, `RowOf`, `AnyTable` | `table/definition.ts` |
| `TableFacade.insert/upsert/delete/insertOrGet` | `table/table-store.ts` |
| `Transaction` | rename to `TableBatch` in `table/table-store.ts` |
| `MaterializedState` fold/preload usage | `table/table-view.ts` |
| `checkpoint`, `compact`, `trim` | `table/checkpoint.ts` |
| keyed stream name derivation / list/openExisting | shared stream naming/admin helpers |

Do not route ordered event streams through `ChangeMessage`. `ChangeMessage`
belongs to table changelogs; event streams carry domain facts.

The event-stream layer should be thin. `effect-s2` already has schema-backed
JSON event helpers in `Channel.ts` (`publish`, `readDecoded`, `guardedAppend`).
`EventStream` and `KeyedEventStore` should primarily add typed declarations,
key-to-stream-path derivation, admin helpers, and error context.

`TableStore` and `KeyedEventStore.open` must accept an optional owner fencing
token and pass it to every append, alongside `matchSeqNum`:

```ts
interface OpenWriterOptions {
  readonly fencingToken?: string
}
```

CAS remains the corruption-safety guard. Fencing is the single-active-writer
guard for a physical key stream.

## 13. Key Affinity And Assignment

Durable object and invocation processing require all records for a key to enter
one physical stream and be processed in that stream order by one active writer.

The processor API should model assignment explicitly:

```ts
interface AssignmentStrategy<K> {
  readonly keyOf: (record: EventRecord<K, unknown>) => K
  readonly assign: (key: K, instances: ReadonlyArray<ProcessorInstanceId>) =>
    ProcessorInstanceId
}
```

For v1, stateful processors must have one input: the keyed physical stream. If
multiple external sources can affect the same durable key, they must first append
tagged intent records into that key's inbox:

```text
InvocationStarted
ActivityCompleted
DurablePromiseResolved
ChildCallCompleted
  -> durable/invocation/{invocationId}
  -> one processor
  -> one cursor
  -> one deterministic fold
```

Arbitrary multi-input stateful processors with independent checkpoints are out
of scope for v1 because they reintroduce cross-input ordering and replay
interleaving problems.

## 14. Error And Retry Model

Expected failures:

- decode failure;
- source connector failure;
- sink connector failure;
- processor failure;
- checkpoint conflict;
- transaction conflict;
- fenced writer / stale owner;
- external side-effect failure.

Retries belong at the processor runner boundary, not inside stream declarations.
Each processor should declare retry policy and dead-letter behavior:

```ts
interface ProcessorRetryPolicy {
  readonly maxAttempts: number
  readonly backoff: Schedule.Schedule<unknown, unknown>
  readonly deadLetter?: EventStream<DeadLetterKey, DeadLetterEvent>
}
```

## 15. Impact On effect-s2-durable

This SDD should simplify durable architecture in three ways:

1. `object/log.ts` becomes a domain adapter over a keyed
   `EventStream<ActorEvent>` / `KeyedEventStore<ActorEvent>`.
2. Inline opaque `run(Effect)` activity execution can evolve into named
   activity request/result facts with idempotent sink processors.
3. The durable host becomes a composition of processors:

```text
Ingress
  -> keyed invocation inbox
  -> InvocationProcessor
  -> keyed object inbox
  -> ObjectProcessor
  -> keyed activity request
  -> ActivitySinkProcessor
  -> keyed invocation inbox
```

That removes pressure to keep widening a central "engine" interface. The engine
hosts processors; processors consume keyed inboxes, write guarded facts, and use
the local fold for deterministic handler state.

## 16. Target Durable Implementation

Use `effect-s2-durable` as the proving ground for the processor substrate. The
target implementation should be intentionally small: enough to validate whether
source/sink/function/table-view abstractions make durable execution simpler and
more correct, without rebuilding every durable feature at once.

### 16.1 Target Package Shape

The durable package should converge toward this structure:

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

  streams/
    invocation.ts
    object.ts
    activity.ts
    durable-promise.ts

  processors/
    invocation.ts
    object.ts
    activity-sink.ts
    durable-promise.ts

  views/
    invocation-status.ts
    object-query.ts

  host/
    index.ts

  ingress/
    contract.ts
    server.ts
    client.ts

  index.ts
```

The important change is not the folder names by themselves. The important
change is that each folder has one semantic job:

- `streams/` declares durable facts;
- `processors/` transforms input facts into output facts;
- `views/` folds facts into queryable state;
- `authoring/` exposes the Restate-like Effect authoring interface;
- `host/` wires processor runners, views, S2, and HTTP ingress.

There should be no generic `engine/` drawer for mixed concerns.

### 16.2 Durable Keyed Streams

The initial durable validation slice should declare keyed physical streams, not
global domain streams:

```ts
const InvocationInbox = StreamDb.keyedEventStore("durable/invocation", {
  key: InvocationId,
  value: InvocationRecord,
})

const ObjectInbox = StreamDb.keyedEventStore("durable/object", {
  key: ObjectKey,
  value: ObjectRecord,
})

const ActivityInbox = StreamDb.keyedEventStore("durable/activity", {
  key: ActivityId,
  value: ActivityRecord,
})
```

Each `open(key)` targets one physical S2 stream:

```text
durable/invocation/{invocationId}
durable/object/{objectName}/{objectKey}
durable/activity/{activityId}
```

Records inside a keyed stream should distinguish intents from facts:

```ts
type InvocationRecord =
  | { readonly _tag: "Intent"; readonly intent: InvocationIntent }
  | { readonly _tag: "Fact"; readonly fact: InvocationFact }
  | { readonly _tag: "System"; readonly system: SystemRecord }
```

Processors consume intents, emit facts, and fold facts into local state.
Query/read-model table views fold facts only.

### 16.3 Durable Query Views

The first validation slice may materialize bounded query views:

```ts
const InvocationStatusView = StreamDb.tableView("durable.invocation.status", {
  source: InvocationFactsForQuery,
  key: (fact) => fact.invocationId,
  fold: foldInvocationStatus,
})
```

These views answer external queries such as `attach` and `poll`. They are not
handler state and they are not dedupe gates.

Do not materialize a global `ObjectStateView` that holds all object state for all
keys in memory. Object state for handler execution comes from the opened
`ObjectInbox` local fold. Shared/read-only object queries should either:

- open and fold the one object key stream on demand; or
- use a bounded or embedded-store-backed point-lookup view when the access
  pattern justifies it.

### 16.4 Durable Processors

The host should run keyed processors rather than a wide engine object.

```ts
const InvocationProcessor = StreamDb.keyedProcessor("durable.invocation", {
  input: InvocationInbox,
  outputs: {
    objectInbox: ObjectInbox,
    activityInbox: ActivityInbox,
  },
  owner: { fencing: true },
  checkpoint: "single-stream-conditional",
  handler: invocationProcessorHandler,
})

const ObjectProcessor = StreamDb.keyedProcessor("durable.object", {
  input: ObjectInbox,
  outputs: {
    invocationInbox: InvocationInbox,
    activityInbox: ActivityInbox,
  },
  owner: { fencing: true },
  checkpoint: "single-stream-conditional",
  handler: objectProcessorHandler,
})

const ActivitySinkProcessor = StreamDb.keyedSinkProcessor("durable.activity", {
  input: ActivityInbox,
  output: InvocationInbox,
  terminalWrite: "append-if-absent",
  checkpoint: "single-stream-conditional",
  handler: activitySinkHandler,
})
```

`InvocationProcessor` owns one invocation stream at a time. `ObjectProcessor`
owns one object-key stream at a time. `ActivitySinkProcessor` owns one activity
stream at a time and performs the external side effect.

This is still compatible with Pulsar's consumer -> executor -> producer shape,
but the durable execution guarantee comes from the keyed S2 stream: one inbox,
one cursor, one local fold, fenced ownership, guarded writes.

### 16.5 Restate-like Authoring Interface

The authoring layer remains ergonomic and Restate-sdk-gen-like. Users should not
see streams, checkpoints, or table views in normal handler code.

```ts
const Counter = durable.object("Counter", {
  add: durable.handler({
    input: Schema.Number,
    output: Schema.Number,
    handler: Effect.fn("Counter.add")(function*(amount) {
      const current = yield* durable.state(CounterState).get("value")
      const next = Option.getOrElse(current, () => 0) + amount
      yield* durable.state(CounterState).set({ id: "value", value: next })
      return next
    }),
  }),
})

const Billing = durable.service("Billing", {
  charge: durable.handler({
    input: ChargeInput,
    output: ChargeResult,
    handler: Effect.fn("Billing.charge")(function*(input) {
      return yield* durable.activity(ChargeCard, input)
    }),
  }),
})
```

The generated/compiled handler code should lower authoring primitives into
durable commands:

| Authoring primitive | Lowered durable fact |
| --- | --- |
| `ctx.serviceClient(Svc).method(input)` | intent appended to the child invocation inbox |
| `ctx.objectClient(Obj, key).method(input)` | intent appended to the object-key inbox |
| `durable.state(table).set(row)` | fact appended to the current keyed stream |
| `durable.activity(Activity, input)` | intent appended to the activity-id inbox |
| `durable.promise(name).await(schema)` | read local fold; park until a resolved fact reaches the same keyed stream |
| external resolve durable promise | intent/fact appended to the target invocation or object inbox |

The ergonomic authoring layer sits on top of the stream processor model. It does
not need a wide `DurableEngineApi`.

### 16.6 Activity Sink Validation

Use named, schema-registered activities for the spike:

```ts
const ChargeCard = durable.activity("ChargeCard", {
  input: ChargeInput,
  output: ChargeResult,
  handler: Effect.fn("ChargeCard.handler")(function*(input, options) {
    return yield* PaymentProvider.charge({
      ...input,
      idempotencyKey: options.idempotencyKey,
    })
  }),
})
```

The processor emits:

```ts
ActivityIntent.Requested({
  activityId,
  invocationId,
  name: "ChargeCard",
  input,
  idempotencyKey: activityId,
})
```

The activity sink processor opens `ActivityInbox.open(activityId)`, executes the
catalogued activity, and records one terminal fact per `activityId` with a
guarded write:

```ts
ActivityFact.Completed({
  activityId,
  invocationId,
  result,
})
```

The uniqueness rule is write-time, not view-time:

```ts
yield* activityInbox.appendIfAbsent(activityId, ActivityFact.Completed(...))
```

An activity dedupe view can skip redundant work as an optimization, but
correctness must not depend on checking a lagging view before acting.

This validates the important guarantee boundary:

- durable intent is exactly once by `activityId`;
- durable terminal result is exactly once by guarded append on `activityId`;
- physical external execution is at-least-once;
- external side effects are effectively-once only when the external provider
  honors `idempotencyKey`.

### 16.7 Host Shape

The host should be a composition root:

```ts
export const DurableHostLive = Layer.mergeAll(
  StreamDb.keyedProcessorRunner(InvocationProcessor),
  StreamDb.keyedProcessorRunner(ObjectProcessor),
  StreamDb.keyedProcessorRunner(ActivitySinkProcessor),
  StreamDb.tableViewRunner(InvocationStatusView),
  IngressHttpLive,
)
```

The host should not expose a broad internal runtime object. Public ingress can
still expose a narrow client surface:

```ts
interface DurableClient {
  readonly submit: ...
  readonly attach: ...
  readonly poll: ...
  readonly resolveDurablePromise: ...
}
```

Internally, those methods append commands or query views.

### 16.8 Expected Deletions And Replacements

This target should let us delete or radically shrink the current complex files:

| Current file | Target replacement |
| --- | --- |
| `engine/live.ts` | `host/index.ts` plus processor declarations |
| `engine/handler-primitives.ts` | authoring lowering + processor command emission |
| `engine/result-reader.ts` | `views/invocation-status.ts` |
| `engine/resolution-router.ts` | append to the target keyed invocation/object inbox |
| `engine/service-deferreds.ts` | keyed promise facts folded in the invocation stream |
| `engine/durable-stores.ts` | stream-db `KeyedEventStore` / checkpoint services |
| `object/log.ts` | `streams/object.ts` keyed event-store declaration |
| `object/owner-driver.ts` | `processors/object.ts` |
| broad in-process projection caches | keyed local folds plus snapshots |

The validation metric is not only line count. The stronger metric is whether
each remaining module has a single sentence purpose:

- declare facts;
- fold facts;
- process facts;
- expose authoring API;
- host processors;
- serve ingress.

### 16.9 Spike Acceptance Criteria

The target implementation is successful if a small counter/billing scenario can
pass with this dataflow:

1. HTTP/client ingress appends `InvocationCommand.Started`.
2. The command lands in `durable/invocation/{invocationId}`.
3. `InvocationProcessor` owns the invocation stream with a fence and runs a
   service handler from the local fold.
4. The service handler calls an object method through the authoring client.
5. The object call intent lands in `durable/object/{objectName}/{objectKey}`.
6. `ObjectProcessor` owns the object stream with a fence and runs the method
   from the local fold.
7. Object state is visible to the handler through the object stream's local fold.
8. The service handler requests a named activity.
9. The activity request lands in `durable/activity/{activityId}`.
10. `ActivitySinkProcessor` executes the activity and appends a terminal fact
    with append-if-absent.
11. The terminal activity fact fans into the invocation inbox.
12. `InvocationProcessor` resumes and appends `InvocationCompleted`.
13. `attach` reads a bounded `InvocationStatusView`.
14. Killing and restarting the host resumes from checkpoints without duplicate
    durable outputs.

If this spike requires rebuilding broad engine-like interfaces, the substrate is
not strong enough yet. The fix should be in stream-db processor/view/checkpoint
abstractions, not in a new durable-specific runtime drawer.

## 17. Build Plan

### Step 1: Keyed Event Store Walking Skeleton

Add the smallest keyed event-store API needed to process one physical stream per
key:

- `KeyedEventStore`;
- `EventSource`;
- `EventSink`;
- `EventRecord`;
- `EventCursor`;
- typed append/read adapters;
- owner fencing token threaded through appends.

Validation:

- schema encode/decode errors are typed;
- empty and missing streams are handled consistently;
- guarded append and fencing options pass through to `effect-s2`;
- one opened key has one tail cursor and one local fold;
- existing table API remains green.

### Step 2: Checkpoint Store

Add a dedicated checkpoint stream/protocol. Do not back checkpoints with
`TableStore`.

Validation:

- load missing checkpoint;
- commit next cursor;
- reject stale conditional commit;
- restart resumes from committed cursor.

### Step 3: Keyed Processor Runner

Add a keyed processor runner:

- one physical input stream per key;
- one local fold at the input position;
- fenced single-active-writer ownership;
- typed output services;
- checkpoint after successful processing;
- retry and dead-letter hooks;
- scoped lifecycle.

Validation:

- at-least-once replay on crash-before-checkpoint;
- no checkpoint movement on handler failure;
- deterministic output ids plus guarded append dedupe repeated processing;
- no arbitrary multi-input stateful processors.

### Step 4: Durable Walking Skeleton

Build the durable counter/billing scenario over keyed streams:

- invocation inbox;
- object inbox;
- activity inbox;
- local fold handler state;
- guarded terminal activity fact;
- bounded invocation status query view.

Validation:

- duplicate activity request produces one durable result;
- crash after request before result retries safely;
- external idempotency key is stable across retries.

## 18. Open Questions

1. What exact owner-token / lease-epoch shape should `KeyedEventStore.open`
   accept so stream-db can thread S2 fencing through every append without
   owning durable host membership?
2. What is the smallest append-if-absent API that can enforce one terminal fact
   per logical id on a keyed stream?
3. What is the smallest named, schema-registered activity catalog that proves
   the activity stream design without reintroducing opaque `run(Effect)` as the
   long-term execution model?
4. Which bounded query views are needed for the first durable walking skeleton?
   `InvocationStatusView` is in scope; global `ObjectStateView` is not.

## 19. References

- Pulsar IO overview: https://pulsar.apache.org/docs/4.2.x/io-overview/
- Pulsar Functions concepts: https://pulsar.apache.org/docs/4.2.x/functions-concepts/
- Pulsar transactions: https://pulsar.apache.org/docs/4.2.x/txn-how/
- Pulsar TableView concept: https://pulsar.apache.org/docs/4.2.x/concepts-clients/#tableview
- Pulsar TableView API: https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/TableView.java
- Pulsar TableViewBuilder API: https://github.com/apache/pulsar/blob/master/pulsar-client-api/src/main/java/org/apache/pulsar/client/api/TableViewBuilder.java
- pulsar-client-dotnet: https://github.com/fsprojects/pulsar-client-dotnet/tree/develop
- Existing stream-db relational SDD: ./effect-s2-stream-db-relational-ivm-sdd.md
