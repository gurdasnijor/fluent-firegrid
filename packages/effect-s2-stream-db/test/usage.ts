import { Effect, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "../src/index.ts"

// A Table is fully described by its Schema: the row fields, its relative path
// (the `type`, here "activities"), and its primary key (the annotated field).
// An S2StreamDb aggregates tables over one stream whose path derives from the
// schema. Typecheck-only: it proves the inference, no S2 calls.

class Activity extends Table<Activity>("activities")({
  activityKey: Schema.String.pipe(primaryKey),
  result: Schema.Unknown,
}) {}

class ClockWakeup extends Table<ClockWakeup>("clockWakeups")({
  clockKey: Schema.String.pipe(primaryKey),
  deadlineMs: Schema.Number,
  status: Schema.Literals(["pending", "fired"]),
}) {}

// The instance key is itself schema-typed; `open` validates it and derives the
// path segment through the schema. (Defaults to `Schema.String` when omitted.)
const ExecutionId = Schema.String.pipe(Schema.brand("ExecutionId"))

class WorkflowDb extends StreamDb<WorkflowDb>("wf")({
  activities: Activity,
  clockWakeups: ClockWakeup,
}, ExecutionId) {}

export const program = Effect.gen(function*() {
  // key validated + encoded → one S2 stream "wf/exec-1" (path derived from the schema).
  const db = yield* WorkflowDb.open(ExecutionId.make("exec-1"))

  // tables are accessed by name; `insert` takes the schema's decoded row.
  yield* db.activities.insert({ activityKey: "charge", result: { ok: true } })
  const charge = yield* db.activities.get("charge")

  // a transaction commits across tables atomically (one S2 batch).
  yield* db.transact((tx) => {
    tx.upsert("activities", { activityKey: "fulfill", result: null })
    tx.insert("clockWakeups", { clockKey: "cooloff", deadlineMs: 1000, status: "pending" })
  })

  yield* db.compact
  return charge
})
