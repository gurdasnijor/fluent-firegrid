# SDD: ksqlDB-Style Streams and Tables for effect-s2-stream-db

## 1. Status

Proposed.

This SDD defines the next shape for `effect-s2-stream-db`: a ksqlDB-inspired
stream/table model over S2.

It also defines the Cucumber integration point:

```text
Cucumber Messages NDJSON / Envelope producer
  -> append-only typed S2 event stream
  -> materializers
  -> queryable/listenable tables
```

The key correction from the previous durable-table-only design:

> The canonical Cucumber output is an append-only stream. Tables are derived
> materialized views over that stream.

Do not continuously transact into an `EnvelopeEvent` table just to preserve
message order. Preserve order by writing envelopes to a normal S2 stream. Use
tables for projections.

## 2. Background

ksqlDB's useful split is:

- **Stream:** unbounded, append-only event sequence. Every row is a new fact.
- **Table:** mutable latest state keyed by primary key. New rows update the
  current value for a key.
- **Persistent query/materialization:** a continuous query that derives a stream
  or table from one or more streams/tables.
- **Push query:** subscribe to future changes.
- **Pull query:** read current materialized state.

Official references:

- ksqlDB stream processing concepts:
  <https://docs.confluent.io/platform/current/ksqldb/concepts/overview.html>
- `CREATE TABLE AS SELECT`: creates a materialized table view and streams the
  query result as a changelog to a sink topic:
  <https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/create-table-as-select.html>
- `CREATE STREAM AS SELECT`: creates a derived stream through a persistent query:
  <https://docs.confluent.io/platform/current/ksqldb/developer-guide/ksqldb-reference/create-stream-as-select.html>

S2 maps well to this:

- an S2 stream is the append-only event log;
- `effect-s2-stream-db` `Table`s are latest-state materializations;
- `effect/Stream` is the push-query/read API;
- `StreamDb.open(...).table(...).query(...)` is the pull-query API.

## 3. Problem

`effect-s2-stream-db` currently exposes a durable state DB:

```ts
Table
StreamDb
TableFacade.insert/upsert/delete/get/query
StreamDb.transact/checkpoint/compact
```

That is good for latest-state rows, but it does not represent an append-only
event stream as a first-class user API.

This leads to an awkward design for Cucumber:

- Cucumber already produces an ordered NDJSON event stream.
- The runner should append each `Envelope` to an S2 stream as a fact.
- Materialized Cucumber tables should be derived from that stream.
- Instead, the prior design proposed an `EnvelopeEvent` table whose only purpose
  was to simulate append order inside `StreamDb`.

That is backwards. An ordered event log should be an event stream. Tables should
be derived from it.

### 3.1 Cucumber Evidence

This SDD was checked against:

- Cucumber Compatibility Kit samples under
  `compatibility-kit/devkit/samples` on `main`.
- cucumber-js local reference at
  `/Users/gnijor/gurdasnijor/fluent-firegrid/repos/cucumber-js`, commit
  `7fa9c439`.

The relevant cucumber-js runtime semantics are already evented. The central
channel is:

```ts
eventBroadcaster.emit("envelope", envelope)
eventBroadcaster.on("envelope", consumer)
```

Observed cucumber-js write sites:

- `src/api/run_cucumber.ts`
  - creates the root `EventEmitter`;
  - attaches public `onMessage`;
  - forwards envelopes to plugins;
  - initializes `EventDataCollector` and formatters;
  - emits `testRunStarted` / `testRunFinished`.
- `src/api/emit_support_code_messages.ts`
  - emits `meta`;
  - emits `parameterType`, `stepDefinition`, `hook`,
    `undefinedParameterType`.
- `src/api/gherkin.ts` and `src/api/run_cucumber.ts`
  - parser output enters through `onEnvelope`, then re-emits as
    `eventBroadcaster.emit("envelope", envelope)`.
- `src/assemble/assemble_test_cases.ts`
  - emits static `testCase` envelopes.
- `src/runtime/worker.ts`
  - emits `testRunHookStarted` / `testRunHookFinished`.
- `src/runtime/test_case_runner.ts`
  - emits `testCaseStarted`;
  - emits `testStepStarted`;
  - emits `attachment` from `World.attach/log/link`;
  - emits `suggestion` for undefined steps;
  - emits `testStepFinished`;
  - emits `testCaseFinished`.
- `src/runtime/parallel/worker.mts`
  - worker-local broadcaster listens to `"envelope"` and posts `ENVELOPE`
    messages to the parent thread.
- `src/runtime/parallel/adapter.ts`
  - parent receives worker `ENVELOPE` messages and re-emits them on the root
    `eventBroadcaster`.

Observed cucumber-js read sites:

- `src/formatter/helpers/event_data_collector.ts`
  - listens to `"envelope"` and materializes in-memory maps.
- `src/formatter/*_formatter.ts`
  - built-in formatters listen to `"envelope"` as streaming consumers.
- plugin manager forwarding listens to root envelopes and emits plugin
  `"message"` events.

The direct architecture mapping is:

```text
cucumber-js emit("envelope", e)       -> CucumberEnvelopeStream.append(e)
cucumber-js on("envelope", consumer)  -> CucumberEnvelopeStream.tail(...)
cucumber-js EventDataCollector maps   -> materialized StreamDb tables
cucumber-js formatters                -> stream consumers or table subscribers
cucumber-js worker ENVELOPE forwarding-> multiple producers into one canonical stream
```

This is why an in-memory queue or process-local pub-sub bus is not the production
primitive. The existing event channel should become durable.

### 3.2 CCK Envelope Trace Shape

The CCK samples exercise these envelope kinds:

```text
meta
source
gherkinDocument
pickle
parameterType
stepDefinition
hook
undefinedParameterType
testRunStarted
testRunHookStarted
testRunHookFinished
testCase
testCaseStarted
testStepStarted
suggestion
attachment
testStepFinished
testCaseFinished
testRunFinished
```

Aggregate facts from the current CCK sample `.ndjson` files:

- 44 samples total.
- All 44 emit `meta`, `source`, `gherkinDocument`, `pickle`,
  `testRunStarted`, and `testRunFinished`.
- 42 samples emit `testCase`, `testCaseStarted`, and `testCaseFinished`.
- `test-run-exception` has no test cases.
- `global-hooks-beforeall-error` emits run-hook execution and
  `testRunFinished`, but no `testCase` / scenario execution.
- 41 samples emit step execution envelopes.
- 11 samples emit `hook` metadata.
- 4 samples emit run-hook execution envelopes:
  `testRunHookStarted` / `testRunHookFinished`.
- 5 samples emit `attachment`.
- 7 samples emit `suggestion`.
- 1 sample emits `parameterType`.
- 1 sample emits `undefinedParameterType`.

The broad ordered pattern is:

```text
meta
source / gherkinDocument / pickle*
parameterType* / stepDefinition* / hook* / undefinedParameterType*
testRunStarted
testRunHookStarted / attachment* / testRunHookFinished*
testCase*
testCaseStarted
  testStepStarted
    suggestion?
    attachment*
  testStepFinished
testCaseFinished
testRunHookStarted / attachment* / testRunHookFinished*
testRunFinished
```

Ordering constraints to preserve:

- `attachment` is bookended by a running step or run hook:
  - step attachment:
    `testStepStarted -> attachment* -> testStepFinished`;
  - run-hook attachment:
    `testRunHookStarted -> attachment* -> testRunHookFinished`.
- `suggestion` is emitted between `testStepStarted` and `testStepFinished` for
  undefined steps.
- `testCase` is static plan output and is emitted once per pickle.
- retries produce multiple `testCaseStarted` / `testCaseFinished` attempts for a
  single `testCase`.
- cucumber-js gates `testCase` emission behind successful BeforeAll hooks. If
  BeforeAll fails, no test cases are assembled/emitted.

These traces confirm the stream/table model:

- the raw Cucumber protocol is an ordered append-only stream;
- formatter/query state is a materialized view over that stream;
- CCK should validate the canonical stream first, then validate projections.

## 4. Goals

- Add a first-class typed event stream abstraction to `effect-s2-stream-db`.
- Keep append-only streams and latest-state tables distinct.
- Add materialization APIs that turn streams into tables or streams.
- Make the Cucumber event stream the canonical durable output.
- Let Cucumber tables be materialized read models over that stream.
- Support both pull queries and push subscriptions over derived tables.
- Avoid process-local queues, PubSub, callback event buses, and array batching.

## 5. Non-Goals

- No SQL parser in this slice.
- No full ksqlDB compatibility.
- No distributed query planner.
- No repartitioning or multi-partition semantics. S2 stream order is per stream.
- No stream-stream joins or windowing in the first milestone.
- No replacement of `Table`/`StreamDb`; this extends them.

## 6. Core Concepts

### 6.1 EventStream

An `EventStream<A>` is a typed append-only S2 stream.

```ts
class CucumberEnvelopeStream extends EventStream<CucumberEnvelopeStream>()(
  "cucumber.envelopes",
)(EnvelopeSchema, CucumberRunId) {}
```

Opening a stream gives append/read/tail operations:

```ts
interface EventStreamInstance<A> {
  readonly append: (value: A, options?: AppendEventOptions) =>
    Effect.Effect<EventAppendAck, EventStreamError, S2Client>

  readonly appendBatch: (values: ReadonlyArray<A>, options?: AppendEventOptions) =>
    Effect.Effect<EventAppendAck, EventStreamError, S2Client>

  readonly read: (options?: EventReadOptions) =>
    Stream.Stream<EventRecord<A>, EventStreamError, S2Client>

  readonly tail: (options?: EventTailOptions) =>
    Stream.Stream<EventRecord<A>, EventStreamError, S2Client>

  readonly sink: Sink.Sink<void, A, A, EventStreamError, S2Client>
}
```

`EventRecord<A>` exposes both S2 metadata and decoded value:

```ts
interface EventRecord<A> {
  readonly seqNum: number
  readonly timestamp: Date
  readonly key?: string
  readonly headers: ReadonlyMap<string, string>
  readonly value: A
}
```

Design notes:

- `EventStream` uses S2 `seq_num` as the canonical order.
- `key` is logical. It supports materialization and later joins, but does not
  imply Kafka-style partitions.
- `append` must not update a table. It only appends event records.
- `tail` is the push-query primitive for streams.

### 6.2 Table

`Table` keeps its current meaning: latest state by primary key.

```ts
class TestStepResultTable extends Table<TestStepResultTable>("cucumber.test_step_results")({
  id: Schema.String.pipe(primaryKey),
  runId: CucumberRunId,
  testCaseStartedId: Schema.String,
  testStepId: Schema.String,
  status: Schema.String,
  seqNum: Schema.Number,
}) {}
```

Tables are not append-only event logs. They are materialized state.

### 6.3 Materializer

A materializer is a persistent query:

```text
source stream/table
  -> filter/map/key/reduce
  -> sink stream/table
```

Two first-class forms are needed:

- **CSAS**: create stream as select, deriving an append-only stream.
- **CTAS**: create table as select, deriving latest-state rows.

API names should be TypeScript/Effect-native rather than SQL strings:

```ts
const failedSteps = CucumberEnvelopes
  .from(runId)
  .filter(hasTestStepFinished)
  .filter((record) => record.value.testStepFinished.testStepResult.status === "FAILED")
  .map((record) => record.value)
  .intoStream(FailedStepEnvelopeStream)

const stepResults = CucumberEnvelopes
  .from(runId)
  .filterMap(projectTestStepResult)
  .materialize(TestStepResultTable, {
    key: (row) => row.id,
    mode: "upsert",
  })
```

## 7. Proposed API Additions

### 7.1 EventStream Definition

Add a class-factory API parallel to `Table` and `StreamDb`:

```ts
export const EventStream =
  <Self>() =>
  <Name extends string>(
    name: Name,
  ) =>
  <A, I, Key extends Schema.Schema.Any = typeof Schema.String>(
    value: Schema.Codec<A, I>,
    key?: Key,
  ): EventStreamClass<A, I, Key>
```

Usage:

```ts
class CucumberEnvelopeStream extends EventStream<CucumberEnvelopeStream>()(
  "cucumber.envelopes",
)(EnvelopeSchema, CucumberRunId) {}
```

The static surface mirrors `StreamDb.open`:

```ts
interface EventStreamClass<A, I, Key extends Schema.Schema.Any> {
  readonly name: string
  readonly value: Schema.Codec<A, I>
  readonly key: Key
  readonly open: (
    key: Key["Type"],
    options?: OpenEventStreamOptions,
  ) => Effect.Effect<EventStreamInstance<A>, EventStreamError, S2Client>

  readonly openExisting: (
    key: Key["Type"],
  ) => Effect.Effect<Option.Option<EventStreamInstance<A>>, EventStreamError, S2Client>

  readonly list: () => Effect.Effect<ReadonlyArray<Key["Type"]>, EventStreamError, S2Client>
}
```

### 7.2 Stream Query Builder

Add a minimal query builder for one-source persistent queries:

```ts
interface StreamQuery<A> {
  readonly filter: (predicate: (record: EventRecord<A>) => boolean) => StreamQuery<A>
  readonly map: <B>(f: (record: EventRecord<A>) => B) => StreamQuery<B>
  readonly filterMap: <B>(f: (record: EventRecord<A>) => Option.Option<B>) => StreamQuery<B>

  readonly intoStream: <B>(
    target: EventStreamClass<B, unknown, any>,
    options?: IntoStreamOptions<B>,
  ) => Materializer

  readonly materialize: <Tbl extends AnyTable>(
    target: Tbl,
    options: MaterializeOptions<RowOf<Tbl>, A>,
  ) => Materializer
}
```

This is not a SQL engine. It is a typed persistent pipeline API.

### 7.3 Materializer Runtime

Materializers must be durable processes with checkpoints:

```ts
interface Materializer {
  readonly name: string
  readonly run: Effect.Effect<void, MaterializerError, S2Client>
}
```

The runtime stores:

- source stream name;
- target stream/table name;
- last processed `seqNum`;
- materializer version/hash;
- status and error details.

Checkpointing must happen only after target writes are durable. The invariant:

> A source record is considered processed only after all derived writes for that
> record have been acknowledged.

### 7.4 Table Change Streams

Tables need push-query support:

```ts
interface TableFacade<Row, Key extends string = string> {
  readonly changes: (
    options?: TableChangesOptions,
  ) => Stream.Stream<TableChange<Row>, S2StreamDbError, S2Client>
}

interface TableChangesOptions {
  readonly fromSeq?: number
  readonly live?: boolean
  readonly includeSnapshot?: boolean
}
```

`changes` reads the underlying state-protocol stream, filters by table type,
decodes rows, and emits `Insert | Update | Delete` changes in S2 order.

### 7.5 Stream to Table Sink

Expose sink helpers for ordinary ingestion:

```ts
const tableSink = <Tbl extends AnyTable>(
  table: TableFacade<RowOf<Tbl>>,
  mode: "insert" | "upsert",
) => Sink.Sink<void, RowOf<Tbl>, RowOf<Tbl>, S2StreamDbError, S2Client>
```

This is useful for simple imports but is not a replacement for materializers.
Materializers own checkpointing.

## 8. Cucumber Integration

### 8.1 Canonical Cucumber Stream

Define one stream per run:

```ts
class CucumberEnvelopeStream extends EventStream<CucumberEnvelopeStream>()(
  "cucumber/envelopes",
)(EnvelopeSchema, CucumberRunId) {}
```

The runner writes every produced Cucumber `Envelope` to this stream:

```ts
const run = yield* CucumberEnvelopeStream.open(runId)

yield* run.append(metaEnvelope())
yield* run.append(sourceEnvelope)
yield* run.append(gherkinDocumentEnvelope)
yield* run.append(pickleEnvelope)
yield* run.append(testRunStartedEnvelope)
yield* run.append(testCaseEnvelope)
yield* run.append(testCaseStartedEnvelope)
yield* run.append(testStepStartedEnvelope)
yield* run.append(testStepFinishedEnvelope)
yield* run.append(testRunFinishedEnvelope)
```

The NDJSON CLI is a direct tail:

```ts
const ndjson = CucumberEnvelopeStream.open(runId).pipe(
  Effect.map((events) =>
    events.tail({ fromSeq }).pipe(
      Stream.map((record) => record.value),
      Stream.map((envelope) => JSON.stringify(envelope) + "\n"),
    ),
  ),
)
```

No table is involved in producing NDJSON.

### 8.2 Cucumber Materialized Tables

Define a `CucumberRunViewDb` for tables derived from the envelope stream:

```ts
class CucumberRunViewDb extends StreamDb<CucumberRunViewDb>("cucumber/views")({
  sources: SourceRows,
  gherkinDocuments: GherkinDocumentRows,
  pickles: PickleRows,
  parameterTypes: ParameterTypeRows,
  stepDefinitions: StepDefinitionRows,
  hooks: HookRows,
  undefinedParameterTypes: UndefinedParameterTypeRows,
  testCases: TestCaseRows,
  testCaseStarted: TestCaseStartedRows,
  testStepStarted: TestStepStartedRows,
  suggestions: SuggestionRows,
  attachments: AttachmentRows,
  testRunHooks: TestRunHookRows,
  testStepFinished: TestStepFinishedRows,
  testCaseFinished: TestCaseFinishedRows,
  testRunFinished: TestRunFinishedRows,
  scenarioResults: ScenarioResultRows,
}, CucumberRunId) {}
```

Then define materializers:

```ts
const materializeTestStepFinished =
  CucumberEnvelopeStream
    .from(runId)
    .filterMap((record) =>
      record.value.testStepFinished === undefined
        ? Option.none()
        : Option.some({
            id: `${record.value.testStepFinished.testCaseStartedId}/${record.value.testStepFinished.testStepId}`,
            runId,
            seqNum: record.seqNum,
            testCaseStartedId: record.value.testStepFinished.testCaseStartedId,
            testStepId: record.value.testStepFinished.testStepId,
            result: record.value.testStepFinished.testStepResult,
          }),
    )
    .materialize(TestStepFinishedRows, {
      key: (row) => row.id,
      mode: "upsert",
      target: CucumberRunViewDb.open(runId),
    })
```

### 8.3 Tables That Could Be Listened For

The initial listenable tables should be:

- `testRunFinished`: run completion/success.
- `testCaseStarted`: scenario admission/progress.
- `testCaseFinished`: scenario completion/retry state.
- `testStepStarted`: step progress.
- `testStepFinished`: step result/status/error.
- `attachments`: proof artifacts, logs, links, screenshots, trace summaries.
- `scenarioResults`: derived aggregate row per scenario.
- `testRunHooks`: global hook attempts and results.
- `suggestions`: undefined-step snippets keyed by attempt/step.
- `undefinedParameterTypes`: support-code definition problems.
- static reference tables: `sources`, `gherkinDocuments`, `pickles`,
  `parameterTypes`, `stepDefinitions`, `hooks`, `testCases`.

Example subscription:

```ts
const failedSteps = CucumberRunViewDb.open(runId).pipe(
  Effect.map((db) =>
    db.testStepFinished.changes({ live: true, includeSnapshot: true }).pipe(
      Stream.filter((change) => change._tag !== "Delete"),
      Stream.map((change) => change.row),
      Stream.filter((row) => row.result.status === "FAILED"),
    ),
  ),
)
```

### 8.4 EventDataCollector as Materializers

cucumber-js `EventDataCollector` currently maintains these in-memory maps:

```text
gherkinDocumentMap[uri]
pickleMap[pickleId]
testCaseMap[testCaseId]
testCaseAttemptDataMap[testCaseStartedId]
stepAttachments[testStepId]
stepResults[testStepId]
worstTestStepResult
undefinedParameterTypes[]
```

In Firegrid these should be durable materializers over
`CucumberEnvelopeStream`:

```text
CucumberEnvelopeStream
  -> GherkinDocumentRows by uri
  -> PickleRows by pickle.id
  -> TestCaseRows by testCase.id
  -> TestCaseAttemptRows by testCaseStarted.id
  -> AttachmentRows by testCaseStartedId/testStepId/ordinal
  -> TestStepResultRows by testCaseStartedId/testStepId
  -> ScenarioResultRows by testCaseStartedId
  -> UndefinedParameterTypeRows by generated id or source key
```

Formatters can then either:

- tail `CucumberEnvelopeStream` directly, matching cucumber-js formatter
  semantics; or
- subscribe to materialized tables when they need indexed/current state.

### 8.5 Scenario Result Materialization

`scenarioResults` is not a direct envelope projection. It is an aggregate over
`testCaseStarted`, `testStepFinished`, and `testCaseFinished`.

First milestone can derive it in a simple single-stream reducer over the canonical
envelope stream:

```ts
type ScenarioAccumulator = {
  readonly testCaseStartedId: string
  readonly testCaseId: string
  readonly statuses: ReadonlyArray<string>
  readonly finished: boolean
}
```

The materializer updates the accumulator row as events arrive. When
`testCaseFinished` arrives, the row becomes terminal.

Windowing is not required because Cucumber runs are finite and keyed by
`testCaseStartedId`.

## 9. Runner Architecture With Stream/Table Split

### 9.1 Coordinator

The coordinator remains the Cucumber protocol authority:

- parses features;
- builds support metadata;
- creates test cases;
- starts scenario attempts;
- writes Cucumber envelopes to `CucumberEnvelopeStream`.

It does not write projection tables.

### 9.2 Scenario Attempt Object

Scenario attempt objects execute steps and hooks and return outcomes to the
coordinator, or write step-local events to the coordinator through a controlled
append API.

The simplest safe direction is:

```text
coordinator -> scenario attempt -> outcome
coordinator -> append envelope stream
```

This avoids object-to-object write cycles.

### 9.3 Materializer

Materializers are independent of runner execution:

```text
CucumberEnvelopeStream tail
  -> projection logic
  -> CucumberRunViewDb tables
```

They may run:

- inline for tests;
- as a durable service;
- as a background worker;
- on-demand when a listener subscribes and the view is stale.

## 10. CCK Validation

Validate the canonical stream first:

1. run Cucumber;
2. tail `CucumberEnvelopeStream` until `testRunFinished`;
3. map `EventRecord.value` to `Envelope`;
4. normalize with CCK rules;
5. compare to fixture NDJSON.

Then validate materializers:

1. ingest fixture NDJSON into `CucumberEnvelopeStream`;
2. run materializers;
3. query `CucumberRunViewDb`;
4. assert expected rows for step results, attachments, scenario results, and run
   result.

This separates protocol conformance from read-model correctness.

Minimum stream-validation matrix:

- `minimal`: smallest passing run.
- `attachments`: step attachments and failed step with attachment.
- `examples-tables`: multiple pickles/test cases from one feature.
- `hooks`: before/after hook steps inside `testCase.testSteps`.
- `hooks-undefined`: suggestions and undefined hook/step behavior.
- `global-hooks`: run-hook started/finished envelopes.
- `global-hooks-beforeall-error`: run hooks without any test-case emission.
- `global-hooks-attachments`: attachments between run-hook start/finish.
- `retry`: repeated attempts for one `testCase`.
- `undefined`: suggestion placement before undefined result.
- `unknown-parameter-type`: `undefinedParameterType`.
- `test-run-exception`: run can finish without test cases.

## 11. API Sketch

```ts
// define
class CucumberEnvelopeStream extends EventStream<CucumberEnvelopeStream>()(
  "cucumber/envelopes",
)(EnvelopeSchema, CucumberRunId) {}

class TestStepFinishedRows extends Table<TestStepFinishedRows>("cucumber.testStepFinished")({
  id: Schema.String.pipe(primaryKey),
  runId: CucumberRunId,
  seqNum: Schema.Number,
  testCaseStartedId: Schema.String,
  testStepId: Schema.String,
  result: TestStepResultSchema,
}) {}

class CucumberRunViewDb extends StreamDb<CucumberRunViewDb>("cucumber/views")({
  testStepFinished: TestStepFinishedRows,
}, CucumberRunId) {}

// produce
const stream = yield* CucumberEnvelopeStream.open(runId)
yield* stream.append(envelope)

// tail canonical NDJSON
yield* stream.tail({ fromSeq: 0 }).pipe(
  Stream.map((record) => record.value),
  Stream.run(/* ndjson sink */),
)

// materialize
const materializer = CucumberEnvelopeStream
  .from(runId)
  .filterMap(projectTestStepFinished(runId))
  .materialize(TestStepFinishedRows, {
    target: CucumberRunViewDb.open(runId),
    key: (row) => row.id,
    mode: "upsert",
  })

yield* materializer.run

// listen to table changes
const view = yield* CucumberRunViewDb.open(runId)
yield* view.testStepFinished.changes({ live: true }).pipe(
  Stream.run(/* UI/proof listener */),
)
```

## 12. Milestones

### M1: Typed EventStream

- Add `EventStream` class factory.
- Add `open`, `openExisting`, `list`.
- Add `append`, `appendBatch`, `read`, `tail`, `sink`.
- Add schema encode/decode tests.
- Add tail-resume tests using S2 `seq_num`.

### M2: Table Change Streams

- Add `TableFacade.changes`.
- Add `StreamDbInstance.changes` for all decoded state-protocol changes.
- Add live tail tests while writes are happening.
- Add snapshot-plus-live option if needed by UI consumers.

### M3: Materializer Runtime

- Add `Materializer`.
- Add checkpoint table/state for last processed `seqNum`.
- Add stream-to-table materialization.
- Add stream-to-stream materialization.
- Ensure source offsets advance only after target writes are acked.

### M4: Cucumber Canonical Stream

- Add `CucumberEnvelopeStream`.
- Make runner append envelopes directly to the stream.
- Make CLI tail the stream to NDJSON.
- Validate CCK against the stream, not arrays.
- Include the CCK trace classes from section 3.2 in the test matrix:
  minimal, attachments, examples-tables, hooks, global-hooks,
  global-hooks-beforeall-error, global-hooks-attachments, retry, undefined,
  unknown-parameter-type, and test-run-exception.

### M5: Cucumber View Tables

- Add `CucumberRunViewDb`.
- Materialize direct envelope-kind tables.
- Materialize `scenarioResults`.
- Add tests for listenable table changes.
- Port cucumber-js `EventDataCollector` semantics to materializers.

### M6: Advanced Stream Processing

- Add stream-table joins for enrichment.
- Add table-table joins if required by dashboards.
- Add windowed aggregations only after a real use case appears.

## 13. Design Decisions

- **D1: Streams are first-class.** A stream is not a table with a sequence key.
- **D2: Tables are materialized views.** Tables represent latest state, not the
  canonical append-only record of facts.
- **D3: S2 `seq_num` is canonical stream order.** Do not invent app sequence
  numbers unless a domain needs them.
- **D4: Cucumber NDJSON tails the stream.** No projection table is required for
  Cucumber protocol output.
- **D5: Cucumber dashboards/proofs use tables.** Tables are derived from the
  canonical stream and can be queried/listened to.
- **D6: Materializers checkpoint after durable writes.** This gives at-least-once
  processing with idempotent table upserts.
- **D7: No SQL syntax yet.** Typed Effect APIs come first; SQL-like syntax can be
  layered later if it proves useful.
- **D8: cucumber-js EventEmitter maps to streams/materializers.** `emit` becomes
  `append`; `on` becomes `tail` or table `changes`; `EventDataCollector` becomes
  durable materialized tables.
- **D9: CCK trace classes drive implementation order.** Minimal protocol,
  attachments, hooks, run hooks, undefined suggestions, retries, and no-test-case
  run failures must each be represented in stream tests.

## 14. Open Questions

- Should `EventStream` live in `effect-s2-stream-db`, or should the package be
  renamed to reflect streams and tables?
- Should materializer checkpoints use a dedicated `StreamDb`, or be stored in S2
  stream metadata/control records?
- Should `EventRecord.key` be required for materializable streams, or optional
  with `keyBy` required at materialization time?
- Should `TableFacade.changes({ includeSnapshot: true })` emit synthetic
  snapshot changes, or should callers compose `query` plus `changes` explicitly?
- How should materializers handle projection code version changes?
- Should Cucumber materializers run inline during tests or as background durable
  services?
- Should parallel scenario attempts append directly to `CucumberEnvelopeStream`,
  or should the coordinator remain the single appender for CCK-strict ordering?
  The first implementation should use coordinator-mediated appends.

## 15. New Session Handoff

Start from this file, not from the earlier durable-table-only SDD.

Local/reference context used while writing this SDD:

- cucumber-js reference:
  `/Users/gnijor/gurdasnijor/fluent-firegrid/repos/cucumber-js`
- CCK samples:
  `https://github.com/cucumber/compatibility-kit/tree/main/devkit/samples`
- superseded local SDD:
  `docs/sdds/cucumber-durable-table-stream-sdd.md`

Implementation order:

1. Add `EventStream` to `effect-s2-stream-db`.
2. Add `TableFacade.changes`.
3. Add a minimal materializer runtime with durable checkpoints.
4. Define `CucumberEnvelopeStream`.
5. Make the Cucumber runner append `Envelope`s to that stream.
6. Tail the stream for NDJSON / CCK validation.
7. Materialize Cucumber read-model tables from that stream.
8. Port `EventDataCollector` semantics into table materializers.

The critical invariant:

> Cucumber protocol output is the event stream. Cucumber tables are projections.

Do not reintroduce an `EnvelopeEvent` table as the canonical message log unless
there is a concrete reason S2 stream records cannot carry typed envelopes.
