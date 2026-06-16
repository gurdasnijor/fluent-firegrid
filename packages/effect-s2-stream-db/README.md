# effect-s2-stream-db

A durable, materialized, compactable **state DB over a single S2 stream** — the
S2/Effect analog of [StreamDB], built on [`effect-s2`](../effect-s2). One stream =
one db = many named tables. It knows nothing about workflows; it could back a chat
room or a feature-flag store equally well.

[StreamDB]: ../../docs/reference/durable-streams/docs/stream-db.md

## Model

- An **`S2StreamDb`** is one S2 stream that aggregates named **Tables**.
- A **Table** is one typed row collection — a `Schema.Struct` that encodes its
  relative path (its State-Protocol `type` discriminator within the stream) and its
  primary key (the `primaryKey`-annotated field). Everything about a table lives in
  its schema.
- The stream **path is derived from the schema**: `open(key)` validates the key and
  derives the stream path `${basePath}/${encode(key)}` — one stream per key.

`Table` and `StreamDb` are curried + self-typed, exactly like `Schema.Class` /
`Effect.Service`:

```ts
import { Effect, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"

class Activity extends Table<Activity>("activities")({
  activityKey: Schema.String.pipe(primaryKey),
  result: Schema.Unknown,
}) {}

class ClockWakeup extends Table<ClockWakeup>("clockWakeups")({
  clockKey: Schema.String.pipe(primaryKey),
  deadlineMs: Schema.Number,
}) {}

// optional: a schema-typed instance key (defaults to Schema.String)
const ExecutionId = Schema.String.pipe(Schema.brand("ExecutionId"))

class WorkflowDb extends StreamDb<WorkflowDb>("wf")({
  activities: Activity,
  clockWakeups: ClockWakeup,
}, ExecutionId) {}

const program = Effect.gen(function*() {
  // one db per key → one S2 stream "wf/exec-1" (path derived from the schema)
  const db = yield* WorkflowDb.open(ExecutionId.make("exec-1"))

  // tables are accessed by name; rows are typed/validated by the table schema
  yield* db.activities.insert({ activityKey: "charge", result: { ok: true } })
  const charge = yield* db.activities.get("charge")

  // a transaction commits across tables atomically (one S2 batch)
  yield* db.transact((tx) => {
    tx.upsert("activities", { activityKey: "fulfill", result: null })
    tx.insert("clockWakeups", { clockKey: "cooloff", deadlineMs: 1000 })
  })
})
```

## Semantics

- **Materialization** is the [Durable Streams State Protocol] fold —
  latest-value-per-`(type, key)` over insert/update/delete change messages
  (`ChangeMessage` + `MaterializedState`). The same fold runs for live apply-on-ack
  and cold replay, so the in-memory view never diverges from the durable log.
- **Writes** are atomic: a transaction buffers its mutations and commits them as one
  conditional S2 batch, then applies them to the in-memory view *after* the ack.
- **Compaction** is S2-native: `db.compact` (in-stream snapshot + trim, bounding
  preload cost to live-key count) or `db.drop` (delete the stream).
- **Single-writer** per stream: one owner per db. Positional CAS never contends, so
  no per-write fencing.

One db per stream. For the durable-execution engine that means **one db per
execution** (one S2 stream per execution); see
[`docs/s2-stream-db-sdd.md`](../../docs/s2-stream-db-sdd.md).

[Durable Streams State Protocol]: ../../docs/reference/durable-streams/packages/state/STATE-PROTOCOL.md

## Status

In progress. Landed: the State-Protocol codec (`ChangeMessage`), the fold
(`MaterializedState`), the `Table`/`StreamDb` definition surface, and the live runtime
(`open` / table facades / `transact` / `compact` / `drop`) over `effect-s2`. Next:
acceptance tests against a real `s2 lite` server.
