# SDD: Cucumber Messages as Durable Tables

> Superseded by
> [`effect-s2-stream-db-stream-table-sdd.md`](./effect-s2-stream-db-stream-table-sdd.md).
>
> This document captured an intermediate design where the ordered Cucumber
> message log was represented as an `EnvelopeEvent` table. The current design is
> ksqlDB-style: the canonical Cucumber output is an append-only S2 event stream,
> and tables are materialized projections over that stream.

## 1. Status

Proposed.

This SDD supersedes the earlier "return `Envelope[]` from the runner" shape for
the Cucumber-compatible runner. The runner should be fully streaming by making
the Cucumber Messages stream itself durable state.

The design is intentionally Restate-shaped:

- a stateless coordinator service orchestrates a run;
- keyed virtual objects own durable, per-run and per-scenario state;
- non-deterministic step work is wrapped in durable `run(...)` boundaries;
- clients observe progress by tailing durable state changes, not by subscribing to
  an in-memory queue.

Restate references:

- service/object communication:
  <https://docs.restate.dev/develop/ts/service-communication>
- keyed object/workflow state:
  <https://docs.restate.dev/develop/ts/state>
- durable steps:
  <https://docs.restate.dev/develop/ts/durable-steps>
- durable concurrent tasks:
  <https://docs.restate.dev/develop/ts/concurrent-tasks>
- competitive racing, for later proof/provider racing only:
  <https://docs.restate.dev/ai/patterns/competitive-racing>

## 2. Problem

Cucumber's canonical output is an ordered NDJSON stream of
`@cucumber/messages` `Envelope`s:

```text
{"meta":{...}}
{"source":{...}}
{"gherkinDocument":{...}}
{"pickle":{...}}
{"stepDefinition":{...}}
{"testRunStarted":{...}}
{"testCase":{...}}
{"testCaseStarted":{...}}
{"testStepStarted":{...}}
{"testStepFinished":{...}}
{"testCaseFinished":{...}}
{"testRunFinished":{...}}
```

The previous durable-runner sketch still batched this into `Envelope[]` and
returned it at the end of the run. That is useful for the CCK gate, but it is not
the desired production model. It hides the real data-flow:

1. parse/generate Cucumber Messages;
2. execute scenarios and steps;
3. append each observed event as it happens;
4. let CLIs, formatters, dashboards, and proof systems tail the durable event
   stream.

The missing abstraction is not a pub-sub bus. It is an Effect/S2 table-stream
abstraction: convert an input `Stream<Envelope>` or producer-side envelope writes
into durable `Table` rows, and expose a typed change stream over those rows.

## 3. Goals

- Preserve the exact Cucumber Messages NDJSON order.
- Project every envelope kind into a typed durable table.
- Make the durable tables the production source of truth.
- Let clients stream NDJSON by tailing durable table changes.
- Avoid in-memory queues, process-local event buses, and callback-style
  broadcasters.
- Keep CCK validation simple: compare the tailed `EnvelopeEvent` projection to
  expected `.ndjson`.
- Keep user-defined state in `effect-s2-stream-db` tables, not in bespoke runner
  arrays.

## 4. Non-Goals

- No fan-out `PubSub` in the runner.
- No live formatter protocol in this slice.
- No full replay adapter for historical Cucumber output beyond tailing durable
  rows.
- No competitive racing for scenario execution. Racing is for later provider/proof
  optimization, not Cucumber's ordered step semantics.

## 5. Core Model

Each Cucumber run has one durable stream-db instance:

```ts
class CucumberRunDb extends StreamDb<CucumberRunDb>("cucumber/run")({
  envelopeEvents: EnvelopeEvent,
  meta: MetaEnvelopeRow,
  sources: SourceEnvelopeRow,
  gherkinDocuments: GherkinDocumentEnvelopeRow,
  pickles: PickleEnvelopeRow,
  stepDefinitions: StepDefinitionEnvelopeRow,
  parameterTypes: ParameterTypeEnvelopeRow,
  hooks: HookEnvelopeRow,
  testRunStarted: TestRunStartedEnvelopeRow,
  testCases: TestCaseEnvelopeRow,
  testCaseStarted: TestCaseStartedEnvelopeRow,
  testStepStarted: TestStepStartedEnvelopeRow,
  attachments: AttachmentEnvelopeRow,
  testStepFinished: TestStepFinishedEnvelopeRow,
  testCaseFinished: TestCaseFinishedEnvelopeRow,
  testRunFinished: TestRunFinishedEnvelopeRow,
}, CucumberRunId) {}
```

`envelopeEvents` preserves the ordered NDJSON stream. The per-envelope-kind
tables are read models for direct queries and subscriptions.

The invariant:

> Every envelope append writes exactly one `EnvelopeEvent` row and at most one
> typed projection row in the same `StreamDb.transact`.

That gives one durable commit point per envelope while keeping typed views
strongly consistent with the ordered log.

## 6. Durable Tables

### 6.1 EnvelopeEvent

This table is the canonical ordered Cucumber Messages stream.

```ts
class EnvelopeEvent extends Table<EnvelopeEvent>("cucumber.envelope.events")({
  seq: Schema.Number.pipe(primaryKey),
  runId: CucumberRunId,
  kind: EnvelopeKind,
  envelope: EnvelopeSchema,
}) {}
```

`seq` is a run-local monotonic sequence number, not a Cucumber message id. It is
the durable replacement for "array push order" and the cursor clients use to
resume tailing.

### 6.2 Typed Projection Rows

Each envelope kind gets a row keyed by its natural message identity when it has
one, otherwise by `seq`.

Examples:

```ts
class PickleEnvelopeRow extends Table<PickleEnvelopeRow>("cucumber.pickle")({
  id: Schema.String.pipe(primaryKey),
  runId: CucumberRunId,
  seq: Schema.Number,
  pickle: PickleSchema,
}) {}

class TestCaseEnvelopeRow extends Table<TestCaseEnvelopeRow>("cucumber.testCase")({
  id: Schema.String.pipe(primaryKey),
  runId: CucumberRunId,
  seq: Schema.Number,
  testCase: TestCaseSchema,
}) {}

class TestStepFinishedEnvelopeRow extends Table<TestStepFinishedEnvelopeRow>("cucumber.testStepFinished")({
  id: Schema.String.pipe(primaryKey), // `${testCaseStartedId}/${testStepId}`
  runId: CucumberRunId,
  seq: Schema.Number,
  testStepFinished: TestStepFinishedSchema,
}) {}

class AttachmentEnvelopeRow extends Table<AttachmentEnvelopeRow>("cucumber.attachment")({
  id: Schema.String.pipe(primaryKey), // `${testCaseStartedId}/${testStepId}/${ordinal}`
  runId: CucumberRunId,
  seq: Schema.Number,
  attachment: AttachmentSchema,
}) {}
```

This keeps `EnvelopeEvent` append-only by `seq` while allowing the typed
projection to act like durable state.

### 6.3 Envelope Schema Boundary

The row schemas should be derived from `@cucumber/messages` types using
`effect/Schema` wrappers. Where upstream does not publish Effect schemas, this
package owns the serialization boundary:

```ts
const EnvelopeSchema = Schema.Struct({
  // one optional field per Cucumber message envelope kind
})
```

The goal is not to hand-roll Cucumber domain types. The goal is to encode/decode
the imported Cucumber message shapes at the durable boundary.

## 7. Producer Data Flow

The producer path is linear:

```text
feature files
  -> gherkin/messages stream
  -> support metadata / test plan messages
  -> scenario execution messages
  -> CucumberRunDb.transact(envelope event + typed row)
```

The coordinator and scenario actors should call an `appendEnvelope` capability
that writes to `CucumberRunDb`:

```ts
const appendEnvelope = (runDb: StreamDbInstance<typeof CucumberRunDb.tables>) =>
  (envelope: Envelope) =>
    runDb.transact((tx) => {
      const seq = nextEnvelopeSeq()
      tx.insert(EnvelopeEvent, {
        seq,
        runId,
        kind: envelopeKind(envelope),
        envelope,
      })

      const projection = projectEnvelope(runId, seq, envelope)
      if (projection !== undefined) {
        tx.upsert(projection.table, projection.row)
      }
    })
```

The actual implementation must not use `nextEnvelopeSeq()` as mutable process
state. Use one of:

- a `RunCursor` durable row updated in the same transaction;
- an S2 append-assigned sequence projected back into the table row;
- an `effect-s2-stream-db` helper that exposes a transaction-local monotonic
  sequence.

The important invariant is that the client-visible `seq` is durable and unique
for the run.

## 8. Consumer Data Flow

The NDJSON CLI is a reader:

```text
CucumberRunDb EnvelopeEvent changes
  -> Stream<EnvelopeEvent>
  -> map event.envelope
  -> MessageToNdjsonStream / JSON line encoding
  -> stdout
```

The public API should be shaped like:

```ts
export const runFeaturesStream = (
  input: RunInput,
): Stream.Stream<Envelope, RunnerError, S2Client | FileSystem> =>
  Stream.unwrapScoped(
    Effect.gen(function*() {
      yield* sendClient(cucumberRun, input.runId).start(input, {
        idempotencyKey: input.runId,
      })

      return CucumberRunDb
        .open(input.runId)
        .pipe(Effect.map((db) =>
          db.envelopeEvents.changes({ fromSeq: 0, live: true }).pipe(
            Stream.map((change) => change.row.envelope),
            Stream.takeUntil((envelope) => envelope.testRunFinished !== undefined),
          )
        ))
    }),
  )
```

This is fully streaming:

- the producer appends durable rows while it runs;
- the consumer tails rows as they commit;
- reconnect starts from the last seen `seq`;
- no in-memory queue is part of the production path.

## 9. Required `effect-s2-stream-db` Additions

The current `StreamDb` has `insert`, `upsert`, `delete`, `get`, `query`,
`transact`, and compaction. It materializes state by folding S2 records, but it
does not expose a first-class live change stream over tables.

Add these primitives.

The user-facing concept this unlocks is a `DurableTable`: a normal
`effect-s2-stream-db` `TableFacade` plus a durable change stream and stream
ingestion helpers.

```ts
interface DurableTable<Row, Key extends string = string> extends TableFacade<Row, Key> {
  readonly changes: (
    options?: TableChangesOptions,
  ) => Stream.Stream<TableChange<Row>, S2StreamDbError, S2Client>

  readonly sink: Sink.Sink<void, Row, Row, S2StreamDbError>
}
```

This should not replace `Table` or `StreamDb`. It is the live/streaming view of
an opened table facade.

### 9.1 Table Change Stream

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

type TableChange<Row> =
  | {
      readonly _tag: "Insert" | "Update"
      readonly seq: number
      readonly key: string
      readonly row: Row
    }
  | {
      readonly _tag: "Delete"
      readonly seq: number
      readonly key: string
    }
```

`changes` decodes raw `ChangeMessage`s, filters by the table's `type`, decodes
the row schema, and emits changes in S2 order.

### 9.2 DB Change Stream

```ts
interface StreamDbInstance<T extends Tables> {
  readonly changes: (
    options?: DbChangesOptions,
  ) => Stream.Stream<DbChange, S2StreamDbError, S2Client>
}
```

This is the untyped/shared primitive for tooling, debugging, and generic table
replication.

### 9.3 Stream Ingestion

```ts
const ingestEnvelopes = (
  input: Stream.Stream<Envelope, E, R>,
  target: CucumberRunDbInstance,
) =>
  input.pipe(
    Stream.mapEffect((envelope) => appendEnvelope(target)(envelope)),
    Stream.runDrain,
  )
```

For Cucumber, input may come from:

- `@cucumber/gherkin` / `gherkin-streams`;
- `@cucumber/message-streams` `NdjsonToMessageStream`;
- internally produced execution envelopes.

The generic stream-db primitive should be a `Sink` or helper that lets callers
write `Stream<Row>` into a table with backpressure:

```ts
const tableSink = <T extends AnyTable>(
  table: TableFacade<RowOf<T>>,
): Sink.Sink<void, RowOf<T>, RowOf<T>, S2StreamDbError>
```

The Cucumber-specific adapter is responsible for splitting one `Envelope` into
`EnvelopeEvent` plus a typed projection row.

## 10. Runner Architecture

### 10.1 Run Object

Use a keyed `object`, not a stateless service, for the run authority. It owns
the `CucumberRunDb` stream and all envelope ordering.

```ts
export const cucumberRun = object({
  name: "cucumber/run",
  handlers: {
    *start(input: RunInput) {
      const db = yield* CucumberRunDb.open(input.runId)
      const write = appendEnvelope(db)

      yield* write(metaEnvelope())

      const plan = yield* assemble(input)
      yield* Effect.forEach(plan.discoveryEnvelopes, write, { discard: true })
      yield* Effect.forEach(plan.supportEnvelopes, write, { discard: true })
      yield* write(testRunStarted(plan.testRunStartedId))
      yield* Effect.forEach(plan.testCaseEnvelopes, write, { discard: true })

      const statuses = yield* runScenarios({ input, plan, write })
      yield* write(testRunFinished({ testRunStartedId: plan.testRunStartedId, success: succeeded(statuses) }))
    },
  },
  shared: {
    *events(input: { readonly afterSeq: number; readonly limit: number }) {
      const db = yield* CucumberRunDb.open(input.runId)
      return yield* db.envelopeEvents.query((rows) =>
        rows
          .filter((row) => row.seq > input.afterSeq)
          .sort((a, b) => a.seq - b.seq)
          .slice(0, input.limit),
      )
    },
  },
})
```

The shared `events` handler is a polling/read model. The richer live stream
should use `StreamDb` table changes directly.

### 10.2 Scenario Attempt Object

Scenario state is keyed per attempt:

```text
scenarioAttemptKey = `${runId}/${testCaseId}/${attempt}`
```

The scenario attempt object owns world state and invokes support code. It should
return structured step outcomes to the run object. The run object remains the
envelope ordering authority.

This avoids cross-object write cycles:

```text
run object -> scenario attempt object -> outcome
run object -> append envelopes
```

Do not have scenario objects call back into the run object while the run object
is awaiting them. That can recreate exclusive-object deadlock patterns.

### 10.3 Step Bodies and Durable Work

Step bodies are ordinary Effect programs. Non-deterministic work inside a step
must use `run(...)`, matching Restate's durable step rule:

```ts
Then("the trace query succeeds", function*() {
  const rows = yield* run(
    ChdbClient.query(sql),
    { name: `proof:${this.testStepId}` },
  )

  assertRows(rows)
})
```

Bare external reads followed by branching are not replay-safe. The Cucumber DSL
should make the durable `run(...)` boundary visible in proof-oriented steps.

## 11. NDJSON Import Path

The same durable table model should accept an existing Cucumber NDJSON stream:

```text
Readable NDJSON
  -> @cucumber/message-streams NdjsonToMessageStream
  -> Stream<Envelope>
  -> CucumberRunDb appendEnvelope
```

This enables:

- importing CCK fixture output into durable tables;
- replaying third-party Cucumber output through Firegrid views;
- comparing our produced run tables against fixture run tables by querying typed
  projections instead of only deep-comparing JSON arrays.

## 12. Subscriptions

Once `TableFacade.changes` exists, subscriptions are just streams over table
changes:

```ts
const testStepResults = CucumberRunDb.open(runId).pipe(
  Effect.map((db) =>
    db.testStepFinished.changes({ fromSeq, live: true }).pipe(
      Stream.map((change) => change._tag === "Delete" ? undefined : change.row),
      Stream.filter((row): row is TestStepFinishedEnvelopeRow => row !== undefined),
    )
  ),
)
```

There is no separate subscription registry in the runner. S2 read sessions and
Effect streams are the subscription mechanism.

## 13. CCK Validation

CCK should validate the durable stream, not an internal array:

1. register support bundle;
2. call `sendClient(cucumberRun, runId).start(...)`;
3. tail `EnvelopeEvent` until `testRunFinished`;
4. map rows to envelopes;
5. normalize and compare with the fixture `.ndjson`.

This exercises the actual production path:

```text
object/service calls
  -> durable table writes
  -> table change stream
  -> NDJSON-compatible envelopes
```

## 14. Milestones

### M1: StreamDb Change Streams

- Add `StreamDbInstance.changes`.
- Add `TableFacade.changes`.
- Add tests that write rows while a reader tails and assert ordered changes.
- Add resume-from-seq tests.

### M2: Cucumber Durable Table Schema

- Add `CucumberRunDb`.
- Add `EnvelopeEvent`.
- Add typed projection tables for all envelope kinds used by CCK.
- Add `projectEnvelope(runId, seq, envelope)`.

### M3: NDJSON Import

- Add `ingestCucumberNdjson`.
- Use `@cucumber/message-streams` for NDJSON parsing.
- Verify a fixture NDJSON stream round-trips into `EnvelopeEvent` ordering.

### M4: Streaming Runner

- Make `cucumberRun` object the envelope authority.
- Append discovery, support, test-case, execution, and completion envelopes to
  `CucumberRunDb` as they are produced.
- Expose `runFeaturesStream` as a tail over `EnvelopeEvent`.

### M5: CCK Over Durable Tables

- Validate `minimal`, `attachments`, and `examples-tables` by tailing
  `EnvelopeEvent`.
- Keep normalization aligned with CCK guidance.

### M6: Query/Proof Views

- Build Firegrid proof views over typed tables:
  `testCase`, `testStepFinished`, `attachments`, and trace/proof attachments.
- Add dashboard/formatter readers as table subscribers.

## 15. Decisions

- **D1: The ordered stream is a table.** `EnvelopeEvent` is a durable table keyed
  by run-local sequence. It is not an in-memory array and not a queue.
- **D2: Typed tables are projections.** They exist for queries and
  subscriptions; they do not replace `EnvelopeEvent` ordering.
- **D3: Subscriptions are S2/Effect streams.** No runner-local PubSub.
- **D4: Scenario objects return outcomes.** The run object appends envelopes to
  avoid cross-object write cycles.
- **D5: StreamDb needs live changes.** This is the missing primitive needed to
  make Cucumber output fully streaming.

## 16. Open Questions

- Should `EnvelopeEvent.seq` be app-managed, S2-assigned, or transaction-assigned
  by a new StreamDb helper?
- Should `TableFacade.changes` expose raw S2 `seqNum`, app row `seq`, or both?
- Should `includeSnapshot` emit current rows before tailing live updates, or
  should snapshot reads remain `query` plus a separate `changes` stream?
- How should we encode schemas for upstream Cucumber message types without
  drifting into hand-rolled domain models?
- Do we need a generic `Stream<Row> -> Table` sink in `effect-s2-stream-db`, or
  should ingestion remain application-owned until more use cases appear?
