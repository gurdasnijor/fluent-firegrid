import { expect, layer } from "@effect/vitest"
import { Duration, Effect, Option, Schema } from "effect"
import { primaryKey, StreamDb, Table } from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

class Note extends Table<Note>("notes")({
  key: Schema.String.pipe(primaryKey),
  text: Schema.String,
}) {}

// Declared on no StreamDb class — exercised purely through `db.table(...)`.
class Tag extends Table<Tag>("tags")({
  name: Schema.String.pipe(primaryKey),
  color: Schema.String,
}) {}

class TestDb extends StreamDb<TestDb>("testdb")({ items: Item, notes: Note }) {}

layer(S2LiteLive, { excludeTestServices: true, timeout: Duration.seconds(40) })(
  "S2StreamDb over s2 lite",
  (it) => {
    it.effect("insert + get round-trips a row", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("insert-get")
        yield* db.items.insert({ id: "a", value: 1 })
        expect(Option.getOrNull(yield* db.items.get("a"))).toStrictEqual({ id: "a", value: 1 })
        expect(Option.isNone(yield* db.items.get("missing"))).toBe(true)
      }))

    it.effect("upsert replaces the row", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("upsert")
        yield* db.items.insert({ id: "a", value: 1 })
        yield* db.items.upsert({ id: "a", value: 2 })
        expect(Option.getOrNull(yield* db.items.get("a"))).toStrictEqual({ id: "a", value: 2 })
      }))

    it.effect("insertOrGet is first-writer-wins", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("insert-or-get")
        expect((yield* db.items.insertOrGet({ id: "a", value: 1 }))._tag).toBe("Inserted")
        const second = yield* db.items.insertOrGet({ id: "a", value: 999 })
        expect(second).toStrictEqual({ _tag: "Found", row: { id: "a", value: 1 } })
      }))

    it.effect("delete removes the row", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("delete")
        yield* db.items.insert({ id: "a", value: 1 })
        yield* db.items.delete("a")
        expect(Option.isNone(yield* db.items.get("a"))).toBe(true)
      }))

    it.effect("query sees all live rows", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("query")
        yield* db.items.insert({ id: "a", value: 1 })
        yield* db.items.insert({ id: "b", value: 2 })
        const total = yield* db.items.query((rows) => rows.reduce((n, r) => n + r.value, 0))
        expect(total).toBe(3)
      }))

    it.effect("transact commits across tables atomically", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("transact")
        yield* db.transact((tx) => {
          tx.insert(Item, { id: "a", value: 1 })
          tx.insert(Note, { key: "n", text: "hi" })
        })
        expect(Option.getOrNull(yield* db.items.get("a"))).toStrictEqual({ id: "a", value: 1 })
        expect(Option.getOrNull(yield* db.notes.get("n"))).toStrictEqual({ key: "n", text: "hi" })
      }))

    it.effect("state survives reopen (durable preload fold)", () =>
      Effect.gen(function*() {
        const a = yield* TestDb.open("reopen")
        yield* a.items.insert({ id: "a", value: 1 })
        yield* a.items.upsert({ id: "a", value: 7 })
        // a fresh db over the same stream re-folds from the durable log
        const b = yield* TestDb.open("reopen")
        expect(Option.getOrNull(yield* b.items.get("a"))).toStrictEqual({ id: "a", value: 7 })
      }))

    it.effect("db.table addresses an undeclared table, durably", () =>
      Effect.gen(function*() {
        const a = yield* TestDb.open("dyn-table")
        yield* a.table(Tag).insert({ name: "urgent", color: "red" })
        expect(Option.getOrNull(yield* a.table(Tag).get("urgent"))).toStrictEqual({ name: "urgent", color: "red" })
        // a fresh open re-folds the stream — the dynamic table's rows survive
        const b = yield* TestDb.open("dyn-table")
        expect(Option.getOrNull(yield* b.table(Tag).get("urgent"))).toStrictEqual({ name: "urgent", color: "red" })
      }))

    it.effect("transact mixes a declared and an undeclared table atomically", () =>
      Effect.gen(function*() {
        const db = yield* TestDb.open("dyn-transact")
        yield* db.transact((tx) => {
          tx.insert(Item, { id: "a", value: 1 })
          tx.upsert(Tag, { name: "t", color: "blue" })
        })
        expect(Option.getOrNull(yield* db.items.get("a"))).toStrictEqual({ id: "a", value: 1 })
        expect(Option.getOrNull(yield* db.table(Tag).get("t"))).toStrictEqual({ name: "t", color: "blue" })
      }))

    it.effect("compact preserves state (snapshot + trim, then reopen)", () =>
      Effect.gen(function*() {
        const a = yield* TestDb.open("compact")
        yield* a.items.insert({ id: "a", value: 1 })
        yield* a.items.insert({ id: "b", value: 2 })
        yield* a.items.delete("a")
        yield* a.compact
        const b = yield* TestDb.open("compact")
        expect(Option.isNone(yield* b.items.get("a"))).toBe(true)
        expect(Option.getOrNull(yield* b.items.get("b"))).toStrictEqual({ id: "b", value: 2 })
      }))
  },
)
