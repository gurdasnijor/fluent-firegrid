# SDD: Relational Streams and Incremental Views for effect-s2-stream-db

## 1. Purpose

Define the next build slice for `packages/effect-s2-stream-db`: first-class
typed event streams, schema-backed durable tables, value-composed relational
derivations, and incremental materialized views.

The target model:

```text
S2 append-only streams
  -> Effect Schema typed stream/table declarations
  -> fluent relational derivations
  -> durable materialized tables and indexes
  -> pull queries and live subscriptions
```

Cucumber Messages are the first validation domain because they are already an
ordered event stream with relational ids. They should validate the generic
stream-db design; they must not become special cases in the core package.

## 2. Load-Bearing Decisions

1. **Streams are first-class facts.** Do not model append-only streams as tables
   with sequence keys.
2. **Tables are latest-state projections.** Tables represent current state by
   key, not canonical history.
3. **`effect-s2` remains the transport layer.** `S2Client` owns raw S2 access,
   sessions, producers, retries, basin/stream admin, and read mechanics.
4. **Typed event streams are schema/path adapters.** They encode/decode domain
   values and derive stream paths, then delegate IO to `S2Client`.
5. **Effect Schema is the durable contract.** Every stream/table/index/view
   boundary must carry a schema for type inference, runtime validation,
   encoding, docs, and error reporting.
6. **The public API is value composition.** Prefer
   `Source.filterMap(...).toTable(...)` over callback registries such as
   `materializedView({ query: (...) => ... })`.
7. **`@tanstack/db-ivm` is internal if used.** It may power joins/aggregates, but
   `D2`, `MultiSet`, and graph wiring must not leak into public APIs.
8. **No second change protocol.** The package already has `ChangeMessage`; any
   IVM integration adapts that internally to bag deltas.
9. **Cucumber NDJSON is the canonical stream.** Cucumber tables are projections
   over the envelope stream.
10. **SQL comes later.** A typed DSL and logical plan come first; SQL can compile
    into the same plan only after the model is stable.

## 3. Existing Layers

### 3.1 `effect-s2`

`packages/effect-s2/src/S2Client.ts` is the low-level client facade. It already
has the required IO primitives:

- `S2Client.ensureStream`;
- `S2Client.append`;
- `S2Client.read`;
- `S2Client.readBytes`;
- `S2Client.producer`;
- `S2Client.appendSession`;
- `S2Client.checkTail`;
- stream/basin admin and metrics.

`packages/effect-s2/src/Channel.ts` already demonstrates the codec adapter
shape with `publish` and `readDecoded`. Stream-db should reuse or generalize
that pattern rather than creating a parallel transport abstraction.

### 3.2 `effect-s2-stream-db`

The current package already provides:

- `Table`;
- `StreamDb`;
- `TableFacade.insert/upsert/delete/get/query`;
- `StreamDb.transact/checkpoint/compact/trim/drop`;
- `ChangeMessage` with `insert | update | delete`;
- `MaterializedState`, the fold over state-protocol messages.

The new work extends this package. It should not break the current table API.

## 4. Public API Target

The authoring surface should read like ksqlDB concepts expressed as TypeScript
values.

### 4.1 Sources

```ts
const CucumberRunId = Schema.String.pipe(Schema.brand("CucumberRunId"))

const CucumberEnvelopes = StreamDb.stream("cucumber_envelopes", {
  key: CucumberRunId,
  value: Envelope,
})

const Users = StreamDb.table("users", {
  key: (row: User) => row.id,
  value: User,
})
```

The exact namespace can change during implementation. The invariant is that
streams and tables are first-class values.

### 4.2 Stream to Table

```ts
const TestCases = CucumberEnvelopes
  .select("testCase", TestCase)
  .toTable("test_cases", {
    key: (testCase) => testCase.id,
  })
```

`select("testCase", TestCase)` is a schema-preserving convenience:

- read the optional property;
- drop records where absent;
- decode/validate with `TestCase`;
- carry `TestCase` as downstream schema.

### 4.3 Shape-Changing Projection

Shape-changing projections must introduce a schema with `mapTo`.

```ts
const TestStepFinishedRow = Schema.Struct({
  id: Schema.String.pipe(Schema.brand("TestStepFinishedRowId")),
  testCaseStartedId: TestCaseStartedId,
  testStepId: TestStepId,
  status: TestStepResultStatus,
  durationNanos: Schema.Number.check(Schema.isInt()),
}).annotate({
  title: "Cucumber Test Step Finished Row",
})

const TestStepFinished = CucumberEnvelopes
  .select("testStepFinished", TestStepFinishedEnvelope)
  .mapTo(TestStepFinishedRow, (event) => ({
    id: TestStepFinishedRow.fields.id.make(`${event.testCaseStartedId}/${event.testStepId}`),
    testCaseStartedId: TestCaseStartedId.make(event.testCaseStartedId),
    testStepId: TestStepId.make(event.testStepId),
    status: event.testStepResult.status,
    durationNanos: toNanos(event.testStepResult.duration),
  }))
  .toTable("test_step_finished", {
    key: (row) => row.id,
    indexes: {
      byTestCaseStartedId: {
        key: TestCaseStartedId,
        project: (row) => row.testCaseStartedId,
      },
    },
  })
```

### 4.4 Aggregates

```ts
const ScenarioResults = TestStepFinished
  .groupBy((row) => row.testCaseStartedId)
  .aggregate({
    stepCount: count(),
    failedCount: countWhere((row) => row.status === "FAILED"),
    status: maxBySeverity((row) => row.status),
    durationNanos: sum((row) => row.durationNanos),
  })
  .toTable("scenario_results")
```

Initial aggregate helpers:

- `count`;
- `countWhere`;
- `sum`;
- `min`;
- `max`;
- custom `Aggregate.make`.

### 4.5 Reads and Subscriptions

```ts
yield* TestStepFinished
  .index("byTestCaseStartedId")
  .get(testCaseStartedId)

yield* ScenarioResults.get(testCaseStartedId)

yield* ScenarioResults.changes({ live: true }).pipe(
  Stream.filter((change) => change.row.status === "FAILED"),
  Stream.runDrain,
)
```

The core package should expose generic table/index/query primitives. Cucumber
helpers such as `findTestCaseBy` belong in a Cucumber read-model package above
stream-db.

## 5. Typed Event Stream Layering

Use a name like `TypedEventStreamInstance<A>` rather than `DurableStream` to
avoid implying a second transport client.

```ts
interface TypedEventStreamInstance<A> {
  readonly append: (value: A) => Effect.Effect<AppendAck, StreamDbError>
  readonly appendBatch: (values: ReadonlyArray<A>) => Effect.Effect<AppendAck, StreamDbError>
  readonly read: (options?: ReadOptions) => Stream.Stream<EventRecord<A>, StreamDbError>
  readonly tail: (options?: TailOptions) => Stream.Stream<EventRecord<A>, StreamDbError>
}
```

Delegation table:

| Typed stream operation | Underlying `effect-s2` operation |
| --- | --- |
| create/open if needed | `S2Client.ensureStream({ stream })` |
| append one/many values | `Schema.encode` then `S2Client.append(name, AppendInput.create(records))` |
| read finite records | `S2Client.read(name, options)` then schema decode |
| tail live records | `S2Client.read(name, liveOptions)` then schema decode |
| high-throughput sink | `S2Client.producer(name)` then encode before `producer.submit(record)` |
| current tail/resume planning | `S2Client.checkTail(name)` |

The typed stream adds only:

- schema ownership;
- key/path derivation;
- typed `EventRecord<A>`;
- stream-db errors containing stream name, seq number, and schema context;
- metadata for materializers, indexes, and checkpoints.

It must not duplicate basin admin, append sessions, producer backpressure, raw
read sessions, or SDK retry behavior.

## 6. Schema Rules

Effect Schema is mandatory at durable boundaries.

### 6.1 What schemas provide

- TypeScript `Type`;
- encoded storage representation;
- runtime decode/encode;
- branded ids;
- validation errors;
- JSON Schema/tooling;
- table/index documentation;
- logical plan readability.

### 6.2 Encoded vs decoded values

S2 stores encoded values. User code receives decoded values. Logical plans must
preserve this distinction:

- decoded `Date` may encode as a string;
- branded strings encode as strings;
- richer Cucumber rows can encode as JSON-safe structs.

IVM should operate on decoded values unless an explicit performance decision
chooses encoded values.

### 6.3 Brands and classes

Use brands for id domains:

```ts
const PickleId = Schema.String.pipe(Schema.brand("PickleId"))
const TestCaseId = Schema.String.pipe(Schema.brand("TestCaseId"))
const TestStepId = Schema.String.pipe(Schema.brand("TestStepId"))
```

Use `Schema.Class` when named rows benefit from methods or clearer tooling:

```ts
class ScenarioResult extends Schema.Class("ScenarioResult")({
  testCaseStartedId: TestCaseStartedId,
  status: TestStepResultStatus,
  failedSteps: Schema.Number.check(Schema.isInt()),
}) {
  get failed() {
    return this.status === "FAILED"
  }
}
```

Use `Schema.Struct` or opaque schemas when no methods are needed.

### 6.4 Decode failures

Decode failures are data-plane failures:

- source stream decode failure fails the read/materializer with stream name,
  seq number, and schema context;
- projection decode failure fails before target writes are checkpointed;
- table query decode failure fails the query instead of returning unknown rows.

## 7. Internal Architecture

### 7.1 Logical plan

The DSL should build an inspectable logical plan:

```text
Source<CucumberEnvelope, EnvelopeSchema>
  -> Select<"testStepFinished", TestStepFinishedEnvelope>
  -> MapTo<TestStepFinishedRow>
  -> Table<TestStepFinishedRow>
```

Plans must expose:

- source dependencies;
- target streams/tables;
- schemas at durable nodes;
- materializer names;
- materializer version/hash;
- checkpoint keys.

Transient nodes produced by arbitrary functions are not persistable until a
schema is introduced with `mapTo`, `select`, `decodeWith`, or another
schema-preserving operator.

### 7.2 IVM kernel

For incremental joins and aggregates, compile logical plans to an internal IVM
engine. Candidate: `@tanstack/db-ivm`.

```text
stream/table changes
  -> internal IVM delta adapter
  -> D2 graph
  -> output deltas
  -> durable target writes
```

Keep IVM internals private. Do not expose `D2`, `MultiSet`, or graph wiring.

### 7.3 ChangeMessage to IVM adapter

Do not introduce `RowDelta` or any second public change protocol. The durable
table protocol already exists:

```ts
{
  type,
  key,
  value,
  old_value,
  headers: { operation: "insert" | "update" | "delete" }
}
```

Internal IVM mapping:

```text
ChangeMessage insert -> MultiSet([[decoded value, 1]])
ChangeMessage delete -> MultiSet([[decoded old_value or current row, -1]])
ChangeMessage update -> MultiSet([[decoded old_value, -1], [decoded value, 1]])
EventRecord<A>       -> MultiSet([[decoded event, 1]])
```

If `old_value` is unavailable for update/delete, the materializer must read the
previous row from the materialized table before feeding the IVM graph.

Target writes remain `ChangeMessage` writes:

- derived insert/upsert -> state-protocol insert/update;
- derived delete -> state-protocol delete.

### 7.4 Materializer checkpoints

A materializer checkpoint stores:

- source stream id;
- processed-through seq number;
- target stream/table id;
- materializer hash;
- last success time;
- last failure details.

Invariant:

> Advance a checkpoint only after every target write for that source record or
> batch has been durably acknowledged.

The first implementation can be at-least-once with idempotent upserts. Exactly
once can come later with source offset, materializer id, and target write CAS.

## 8. Cucumber Validation Model

### 8.1 Canonical stream

```ts
const CucumberEnvelopes = StreamDb.stream("cucumber_envelopes", {
  key: CucumberRunId,
  value: Envelope,
})
```

Cucumber runner:

```text
produce Envelope -> CucumberEnvelopes.append(envelope)
```

NDJSON CLI:

```text
CucumberEnvelopes.tail(runId)
  -> record.value
  -> JSON.stringify(envelope) + "\n"
```

No projection table is required for protocol output.

### 8.2 Projection tables

Initial tables:

- `sources`;
- `gherkin_documents`;
- `pickles`;
- `parameter_types`;
- `step_definitions`;
- `hooks`;
- `undefined_parameter_types`;
- `test_cases`;
- `test_case_started`;
- `test_step_started`;
- `attachments`;
- `suggestions`;
- `test_run_hooks`;
- `test_step_finished`;
- `test_case_finished`;
- `test_run_finished`;
- `scenario_results`.

### 8.3 `@cucumber/query` oracle

Use `@cucumber/query` as a semantic oracle:

1. ingest CCK NDJSON into `@cucumber/query`;
2. ingest the same NDJSON into `CucumberEnvelopes`;
3. run materializers;
4. compare selected generic table/query results.

Do not mirror `@cucumber/query`'s exact method names in the core API.

### 8.4 Minimum trace matrix

Validate against these CCK samples:

- `minimal`;
- `attachments`;
- `examples-tables`;
- `hooks`;
- `hooks-undefined`;
- `global-hooks`;
- `global-hooks-beforeall-error`;
- `global-hooks-attachments`;
- `retry`;
- `undefined`;
- `unknown-parameter-type`;
- `test-run-exception`.

Important envelope kinds:

```text
meta, source, gherkinDocument, pickle, parameterType, stepDefinition, hook,
undefinedParameterType, testRunStarted, testRunHookStarted,
testRunHookFinished, testCase, testCaseStarted, testStepStarted, suggestion,
attachment, testStepFinished, testCaseFinished, testRunFinished
```

Ordering constraints:

- attachments are bookended by step/run-hook start and finish;
- suggestions occur between step start and finish;
- retries produce multiple attempts for one test case;
- BeforeAll failure can emit run-hook events and run finish without test cases.

## 9. Implementation Plan

### M1: Source declarations

- Add stream declarations with schemas.
- Add table declarations in the fluent model or adapt existing `Table`.
- Preserve existing `Table`/`StreamDb` APIs.
- Tests: schema encode/decode, path/key derivation.

### M2: Typed stream runtime

- Add append, appendBatch, read, tail.
- Delegate to `S2Client`.
- Reuse/generalize `Channel.publish/readDecoded`.
- Tests: NDJSON-style append/read, tail resume by S2 seq num.

### M3: Table changes and indexes

- Add table `changes`.
- Add durable secondary indexes.
- Support snapshot-plus-live subscriptions.
- Tests: insert/update/delete change ordering and index lookups.

### M4: Fluent DSL

- Add `map`, `filter`, `filterMap`, `select`, `mapTo`, `keyBy`.
- Add `toStream` and `toTable`.
- Ensure logical plans are pure and inspectable.
- Tests: no durable sink without schema after shape-changing map.

### M5: Materializer runtime

- Add materializer hash/version.
- Add checkpoint table/state.
- Run one-source stream-to-table materializers.
- Ensure checkpoint advances only after target writes ack.

### M6: IVM spike

- Add `@tanstack/db-ivm` behind `internal/ivm`.
- Feed `ChangeMessage`/event inputs through internal delta adapter.
- Maintain `scenario_results` incrementally.
- Verify no public `db-ivm` types leak.
- Decide keep/fork/remove.

### M7: Relational operators

- Add groupBy/aggregate helpers.
- Add table-table join.
- Add stream-table join.
- Add custom aggregate support.

### M8: Cucumber proof

- Define `CucumberEnvelopes`.
- Define projection tables.
- Validate canonical stream against CCK NDJSON.
- Validate selected materialized tables against `@cucumber/query`.

## 10. Build Agent Execution Brief

Start with M1-M2 only. Do not begin with joins, IVM, materializers, SQL, or
Cucumber-specific tables.

First implementation slice:

1. Add the typed event stream declaration surface.
2. Implement append/read/tail by delegating to `S2Client`.
3. Reuse or generalize `Channel.publish` and `Channel.readDecoded`.
4. Preserve existing `Table` and `StreamDb` APIs.
5. Add tests for schema encode/decode, path derivation, append/read, and
   seq-num resume.

Guardrails:

- Do not create a new low-level S2 client.
- Do not expose `@tanstack/db-ivm`.
- Do not add `@tanstack/db-ivm` until M6.
- Do not introduce `RowDelta` or another public change protocol.
- Do not add Cucumber-specific query methods to `effect-s2-stream-db`.
- Do not allow a durable sink after a shape-changing map unless a schema is
  introduced with `mapTo`, `select`, or equivalent.

A useful first PR after this SDD should probably touch:

```text
packages/effect-s2-stream-db/src/EventStream.ts
packages/effect-s2-stream-db/src/index.ts
packages/effect-s2-stream-db/test/event-stream.test.ts
packages/effect-s2-stream-db/test/usage.ts
```

Acceptance for that first PR:

- existing stream-db tests still pass;
- current `Table`/`StreamDb` usage still typechecks;
- a schema-backed stream can append/read typed values through `S2Client`;
- invalid persisted data fails with a typed decode error that includes stream
  and seq-number context;
- no new dependency on `@tanstack/db-ivm`.

## 11. Proposed Package Layout

```text
packages/effect-s2-stream-db/src/
  StreamDb.ts              existing Table/StreamDb API
  EventStream.ts           typed event stream adapter over S2Client
  QueryDsl.ts              value-composition declarations
  Materializer.ts          runtime/checkpoints
  Index.ts                 durable secondary indexes
  Aggregate.ts             aggregate helper definitions
  internal/
    ivm.ts                 optional db-ivm adapter, no public exports
    logical-plan.ts        inspectable operator graph
    checkpoints.ts         materializer checkpoint rows
```

Add `@tanstack/db-ivm` only when starting M6:

```json
{
  "dependencies": {
    "@tanstack/db-ivm": "^0.1.18"
  }
}
```

## 12. Cucumber-js Event Mapping

The cucumber-js runtime is already evented:

```ts
eventBroadcaster.emit("envelope", envelope)
eventBroadcaster.on("envelope", consumer)
```

Durable mapping:

```text
emit("envelope", e)       -> CucumberEnvelopes.append(e)
on("envelope", consumer)  -> CucumberEnvelopes.tail(...)
EventDataCollector maps   -> stream-db materialized tables
formatters                -> stream consumers or table subscribers
parallel worker ENVELOPE  -> multiple producers into one canonical stream
```

Producer sites observed in cucumber-js:

- `src/api/run_cucumber.ts`;
- `src/api/emit_support_code_messages.ts`;
- `src/api/gherkin.ts`;
- `src/assemble/assemble_test_cases.ts`;
- `src/runtime/worker.ts`;
- `src/runtime/test_case_runner.ts`;
- `src/runtime/parallel/worker.mts`;
- `src/runtime/parallel/adapter.ts`.

Consumer sites observed in cucumber-js:

- `src/formatter/helpers/event_data_collector.ts`;
- built-in formatters under `src/formatter`;
- plugin forwarding.

## 13. Open Questions

- Should the public namespace be `StreamDb.stream(...)` or a new module?
- Should fluent `StreamDb.table(...)` wrap or replace the class-factory `Table`
  for new code?
- Should derived table state live in the existing state-protocol stream or one
  stream per derived table?
- How much IVM state should be snapshotted versus rebuilt?
- Should indexes be declared inside `toTable(..., { indexes })`, independently,
  or both?
- How should schema evolution work: versioned schemas, decode middleware, or
  materializer migrations?
- How should materializer version changes handle existing targets: rebuild,
  dual-write, or explicit migration?

## 14. References

- Current stream-db package: `packages/effect-s2-stream-db`
- Transport facade: `packages/effect-s2/src/S2Client.ts`
- Existing codec adapter: `packages/effect-s2/src/Channel.ts`
- State protocol: `packages/effect-s2-stream-db/src/ChangeMessage.ts`
- Effect Schema guide:
  <https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/SCHEMA.md>
- TanStack DB IVM:
  <https://github.com/TanStack/db/tree/main/packages/db-ivm>
- Cucumber Query:
  <https://github.com/cucumber/query>
- Cucumber Compatibility Kit samples:
  <https://github.com/cucumber/compatibility-kit/tree/main/devkit/samples>
