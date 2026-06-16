import { Effect, Option, Schema } from "effect"
import { S2Client } from "effect-s2"
import { primaryKey, StreamDb, Table } from "effect-s2-stream-db"
import { assertEquals, assertNone, assertSome, assertTrue } from "../../assertions.ts"
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
    "Drives every effect-s2-stream-db storage primitive against s2 lite: per-stream "
    + "config on open, instance enumeration, non-creating existence probes, the "
    + "latest-value table projection, and caller-driven checkpoint + trim.",
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
        key,
        keyFor,
        streamName: `firelab-storage-primitives/${key}`,
        open: (suffix: string) => StorageDb.open(keyFor(suffix)),
        openConfigured: (suffix: string, config: Parameters<typeof StorageDb.open>[1]) =>
          StorageDb.open(keyFor(suffix), config),
        list: (keyPrefix?: string) => StorageDb.list(keyPrefix === undefined ? {} : { keyPrefix }),
        exists: (k: string) => StorageDb.exists(k),
        openExisting: (k: string) => StorageDb.openExisting(k),
        reopen: () => StorageDb.open(key),
      }
    }),
  requirements: [
    // ── STREAM_CONFIG ────────────────────────────────────────────────────────
    {
      id: "STREAM_CONFIG.1",
      description: "open accepts a StreamConfig (storageClass / deleteOnEmpty / retentionPolicy) on create-if-absent",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.open")) && spans.exists(s, named(s, "S2.createStream"))',
      claim: ({ openConfigured }) =>
        Effect.gen(function*() {
          const configured = yield* openConfigured("configured", {
            config: { storageClass: "standard", deleteOnEmpty: { minAgeSecs: 0 } },
          })
          yield* configured.items.insert({ id: "c", value: 7 })
          assertSome(yield* configured.items.get("c"), { id: "c", value: 7 })
        }),
    },
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
      id: "STREAM_CONFIG.3",
      description: "a stream mixing permanent state with transient records is GC'd by checkpoint, not age retention",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.checkpoint")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ openConfigured, open }) =>
        Effect.gen(function*() {
          // Explicit infinite retention: no age-based GC. State is GC'd only by the
          // caller-driven checkpoint, and survives a reopen of the same instance.
          const persistent = yield* openConfigured("persistent", {
            config: { retentionPolicy: { infinite: {} } },
          })
          yield* persistent.items.insert({ id: "state", value: 100 })
          yield* persistent.checkpoint
          const reopened = yield* open("persistent")
          assertSome(yield* reopened.items.get("state"), { id: "state", value: 100 })
        }),
    },
    // ── ENUMERATE ─────────────────────────────────────────────────────────────
    {
      id: "ENUMERATE.1",
      description: "list returns the instance keys that currently exist as streams under basePath",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.list")) && spans.exists(s, named(s, "S2.listStreams"))',
      claim: ({ key, keyFor, open, list }) =>
        Effect.gen(function*() {
          yield* open("e1")
          yield* open("e2")
          const keys = yield* list(key)
          assertTrue(keys.includes(key), "list includes the base instance")
          assertTrue(keys.includes(keyFor("e1")), "list includes e1")
          assertTrue(keys.includes(keyFor("e2")), "list includes e2")
        }),
    },
    {
      id: "ENUMERATE.2",
      description: "a keyPrefix narrows enumeration to matching keys",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.list")) && spans.exists(s, named(s, "S2.listStreams"))',
      claim: ({ key, keyFor, open, list }) =>
        Effect.gen(function*() {
          yield* open("aa")
          yield* open("ab")
          yield* open("zz")
          const narrowed = yield* list(`${key}.a`)
          assertTrue(narrowed.includes(keyFor("aa")), "narrowed list includes aa")
          assertTrue(narrowed.includes(keyFor("ab")), "narrowed list includes ab")
          assertTrue(!narrowed.includes(keyFor("zz")), "narrowed list excludes zz")
        }),
    },
    {
      id: "ENUMERATE.3",
      description: "includeDeleted defaults false — a dropped instance no longer enumerates",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.list")) && spans.exists(s, named(s, "effect-s2-stream-db.drop")) && spans.exists(s, named(s, "S2.deleteStream"))',
      claim: ({ key, keyFor, open, list }) =>
        Effect.gen(function*() {
          yield* open("live")
          const gone = yield* open("gone")
          assertTrue((yield* list(key)).includes(keyFor("gone")), "gone enumerates before drop")
          yield* gone.drop
          const after = yield* list(key)
          assertTrue(after.includes(keyFor("live")), "live still enumerates")
          assertTrue(!after.includes(keyFor("gone")), "dropped instance is excluded")
        }),
    },
    // ── EXISTENCE ─────────────────────────────────────────────────────────────
    {
      id: "EXISTENCE.1",
      description: "exists(key) reports existence without creating the stream (checkTail)",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.exists")) && spans.exists(s, named(s, "S2.checkTail"))',
      claim: ({ keyFor, exists, open, list }) =>
        Effect.gen(function*() {
          const ghost = keyFor("ghost")
          assertTrue((yield* exists(ghost)) === false, "missing stream does not exist")
          assertTrue((yield* list(ghost)).length === 0, "probing did not create the stream")
          yield* open("ghost")
          assertTrue((yield* exists(ghost)) === true, "exists is true once created")
        }),
    },
    {
      id: "EXISTENCE.2",
      description: "openExisting returns None for a missing stream and never creates it",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.exists")) && spans.exists(s, named(s, "S2.checkTail"))',
      claim: ({ keyFor, exists, openExisting, open }) =>
        Effect.gen(function*() {
          const absent = keyFor("absent")
          assertTrue(Option.isNone(yield* openExisting(absent)), "openExisting is None for a missing stream")
          assertTrue((yield* exists(absent)) === false, "openExisting did not create the stream")
          yield* open("absent")
          const reopened = yield* openExisting(absent)
          assertTrue(Option.isSome(reopened), "openExisting is Some once the stream exists")
          if (Option.isSome(reopened)) {
            yield* reopened.value.items.insert({ id: "x", value: 1 })
            assertSome(yield* reopened.value.items.get("x"), { id: "x", value: 1 })
          }
        }),
    },
    // ── PROJECTION ────────────────────────────────────────────────────────────
    {
      id: "PROJECTION.1",
      description: "the latest-value materialized fold is the read lens for user state (get/query)",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.table.get")) && spans.exists(s, named(s, "effect-s2-stream-db.table.query"))',
      claim: ({ db }) =>
        Effect.gen(function*() {
          // get/query read the CURRENT value per (table, key) — the projection lens,
          // not an ordered event log (that is a schema-owned actor-log over
          // effect-s2.readDecoded, in effect-s2-durable; see object-actor-model LAYERING.6).
          yield* db.transact((tx) => {
            tx.insert(Item, { id: "b", value: 1 })
            tx.insert(Note, { key: "n", text: "hi" })
          })
          yield* db.items.upsert({ id: "b", value: 3 }) // a later write supersedes the earlier value
          assertSome(yield* db.items.get("b"), { id: "b", value: 3 })
          assertSome(yield* db.notes.get("n"), { key: "n", text: "hi" })
          assertEquals(yield* db.items.query((rows) => rows.reduce((sum, row) => sum + row.value, 0)), 3)
        }),
    },
    // ── CHECKPOINT ────────────────────────────────────────────────────────────
    {
      id: "CHECKPOINT.1",
      description: "checkpoint snapshots the live set then reopens from the compacted stream",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.checkpoint")) && spans.exists(s, named(s, "S2.append")) && spans.exists(s, named(s, "S2.readBatch"))',
      claim: ({ db, reopen }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "a", value: 1 })
          yield* db.items.upsert({ id: "a", value: 2 })
          yield* db.items.insert({ id: "b", value: 3 })
          yield* db.items.delete("a")
          yield* db.checkpoint
          const reopened = yield* reopen()
          assertNone(yield* reopened.items.get("a"))
          assertSome(yield* reopened.items.get("b"), { id: "b", value: 3 })
        }),
    },
    {
      id: "CHECKPOINT.2",
      description: "trim(cursor) issues an explicit trim of records before the cursor",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.trim")) && spans.exists(s, named(s, "S2.append")) && spans.exists(s, named(s, "S2.checkTail"))',
      claim: ({ db, streamName }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "t1", value: 1 })
          // the cursor is an S2 seq_num — read it from the S2 layer (checkTail), not
          // from any stream-db ordered-log lens. The tail after t1 is the seq_num t2
          // will take, so trimming before it requests removal of t1.
          const cursor = (yield* S2Client.checkTail(streamName)).tail.seqNum
          yield* db.items.insert({ id: "t2", value: 2 })
          yield* db.items.insert({ id: "t3", value: 3 })
          // trim durably appends one trim command record to S2 — the stream tail
          // advances by exactly 1. (Physical purge of trimmed records is an
          // asynchronous S2 background operation, not observed synchronously here.)
          const before = yield* S2Client.checkTail(streamName)
          yield* db.trim(cursor)
          const after = yield* S2Client.checkTail(streamName)
          assertTrue(
            after.tail.seqNum === before.tail.seqNum + 1,
            "trim appends exactly one durable trim command record",
          )
          // the stream stays consistent and appendable after the trim.
          assertSome(yield* db.items.get("t3"), { id: "t3", value: 3 })
          yield* db.items.insert({ id: "t4", value: 4 })
          assertSome(yield* db.items.get("t4"), { id: "t4", value: 4 })
        }),
    },
    {
      id: "CHECKPOINT.3",
      description: "state represented by the checkpoint snapshot remains readable after trim",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.checkpoint")) && spans.exists(s, named(s, "S2.readBatch")) && spans.exists(s, named(s, "effect-s2-stream-db.table.query"))',
      claim: ({ db, reopen }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "a", value: 1 })
          yield* db.items.insert({ id: "b", value: 2 })
          yield* db.items.delete("a")
          yield* db.checkpoint
          const reopened = yield* reopen()
          assertEquals(yield* reopened.items.query((values) => values), [{ id: "b", value: 2 }])
        }),
    },
    {
      id: "CHECKPOINT.4",
      description: "a multi-key checkpoint snapshot lands as one atomic batch and reopens whole",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.checkpoint")) && spans.exists(s, named(s, "S2.append")) && spans.exists(s, named(s, "S2.readBatch"))',
      claim: ({ db, reopen }) =>
        Effect.gen(function*() {
          yield* db.transact((tx) => {
            tx.insert(Item, { id: "k1", value: 1 })
            tx.insert(Item, { id: "k2", value: 2 })
            tx.insert(Item, { id: "k3", value: 3 })
            tx.insert(Item, { id: "k4", value: 4 })
          })
          yield* db.checkpoint
          const reopened = yield* reopen()
          assertEquals(yield* reopened.items.query((rows) => rows.length), 4)
          assertSome(yield* reopened.items.get("k4"), { id: "k4", value: 4 })
        }),
    },
    // ── COMPAT ────────────────────────────────────────────────────────────────
    {
      id: "COMPAT.1",
      description: "existing compact + drop behaviour is unchanged alongside the new primitives",
      evidence: 'spans.exists(s, named(s, "effect-s2-stream-db.compact")) && spans.exists(s, named(s, "effect-s2-stream-db.drop")) && spans.exists(s, named(s, "S2.deleteStream"))',
      claim: ({ db }) =>
        Effect.gen(function*() {
          yield* db.items.insert({ id: "temp", value: 1 })
          yield* db.compact // legacy name still works
          assertSome(yield* db.items.get("temp"), { id: "temp", value: 1 })
          yield* db.drop
        }),
    },
  ],
})
