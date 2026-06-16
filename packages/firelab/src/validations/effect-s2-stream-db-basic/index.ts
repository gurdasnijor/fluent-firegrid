import { Effect, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import { assertEquals, assertNone, assertSome } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

class Note extends Table<Note>("notes")({
  key: Schema.String.pipe(primaryKey),
  text: Schema.String,
}) {}

class TestDb extends StreamDb<TestDb>("firelab-stream-db")({ items: Item, notes: Note }) {}

export default defineValidation({
  id: "effect-s2-stream-db-basic",
  description:
    "Runs effect-s2-stream-db against a real s2 lite process, asserting insert/get, "
    + "upsert, cross-table transaction, compact, and reopen behavior with OTel evidence.",
  feature: {
    product: "effect-s2-stream-db",
    name: "storage-primitives",
  },
  backend: S2LiteLive,
  component: ({ key }) =>
    Effect.gen(function*() {
      const db = yield* TestDb.open(key)
      return {
        db,
        reopen: () => TestDb.open(key),
      }
    }),
  requirements: [
    {
      id: "STREAM_CONFIG.2",
      description: "omitting stream config preserves create-if-absent open behaviour",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.open")) && spans.exists(s, named(s, "S2.createStream")) && spans.exists(s, named(s, "S2.checkTail"))',
      claim: ({ db }) =>
        Effect.gen(function*() {
          assertNone(yield* db.items.get("missing"))
        }),
    },
    {
      id: "ORDERED_READ.3",
      description: "latest-value materialized reads remain available for state",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.transact")) && spans.exists(s, named(s, "effect-s2-stream-db.table.get")) && spans.exists(s, named(s, "effect-s2-stream-db.table.query"))',
      claim: ({ db }) =>
        Effect.gen(function*() {
          yield* db.transact((tx) => {
            tx.insert(Item, { id: "b", value: 3 })
            tx.insert(Note, { key: "n", text: "hi" })
          })
          assertSome(yield* db.items.get("b"), { id: "b", value: 3 })
          assertSome(yield* db.notes.get("n"), { key: "n", text: "hi" })
          assertEquals(yield* db.items.query((rows) => rows.reduce((sum, row) => sum + row.value, 0)), 3)
        }),
    },
    {
      id: "CHECKPOINT.1",
      description: "compact snapshots live state and reopens from the compacted stream",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.compact")) && spans.exists(s, named(s, "S2.append")) && spans.exists(s, named(s, "S2.readBatch"))',
      claim: ({ db, reopen }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "a", value: 1 })
          yield* db.items.upsert({ id: "a", value: 2 })
          yield* db.items.insert({ id: "b", value: 3 })
          yield* db.items.delete("a")
          yield* db.compact
          const reopened = yield* reopen()
          assertNone(yield* reopened.items.get("a"))
          assertSome(yield* reopened.items.get("b"), { id: "b", value: 3 })
        }),
    },
  ],
})
