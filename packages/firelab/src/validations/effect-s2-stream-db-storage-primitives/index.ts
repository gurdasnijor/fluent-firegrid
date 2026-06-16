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

class StorageDb extends StreamDb<StorageDb>("firelab-storage-primitives")({ items: Item, notes: Note }) {}

export default defineValidation({
  id: "effect-s2-stream-db-storage-primitives",
  description:
    "Drives the currently implemented effect-s2-stream-db storage primitives "
    + "against s2 lite: create-on-open, materialized reads, compact/reopen, and drop.",
  feature: {
    product: "effect-s2-stream-db",
    name: "storage-primitives",
  },
  backend: S2LiteLive,
  component: ({ key, keyFor }) =>
    Effect.gen(function*() {
      const db = yield* StorageDb.open(key)
      return {
        db,
        open: (suffix: string) => StorageDb.open(keyFor(suffix)),
        reopen: () => StorageDb.open(key),
      }
    }),
  requirements: [
    {
      id: "STREAM_CONFIG.2",
      description: "omitting stream config preserves create-if-absent open behaviour",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.open")) && spans.exists(s, named(s, "S2.createStream")) && spans.exists(s, named(s, "S2.checkTail"))',
      claim: ({ db }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "created", value: 1 })
          assertSome(yield* db.items.get("created"), { id: "created", value: 1 })
        }),
    },
    {
      id: "ORDERED_READ.3",
      description: "latest-value materialized reads remain available for state",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.table.get")) && spans.exists(s, named(s, "effect-s2-stream-db.table.query"))',
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
    {
      id: "CHECKPOINT.3",
      description: "state represented by the compact snapshot remains readable after trim",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.compact")) && spans.exists(s, named(s, "S2.readBatch")) && spans.exists(s, named(s, "effect-s2-stream-db.table.query"))',
      claim: ({ db, reopen }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "a", value: 1 })
          yield* db.items.insert({ id: "b", value: 2 })
          yield* db.items.delete("a")
          yield* db.compact
          const reopened = yield* reopen()
          assertEquals(yield* reopened.items.query((values) => values), [{ id: "b", value: 2 }])
        }),
    },
    {
      id: "COMPAT.1",
      description: "existing drop behaviour still deletes the underlying stream",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.drop")) && spans.exists(s, named(s, "S2.deleteStream"))',
      claim: ({ db }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "temp", value: 1 })
          yield* db.drop
        }),
    },
  ],
})
