import { describe, expect, it } from "@effect/vitest"
import { Effect, Option, Schema } from "effect"
import { ChangeMessage, MaterializedState, primaryKey, Table } from "../src/index.ts"
import { dbChangeOf, tableChangeOf } from "../src/StreamDb.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

describe("change projection (the pure core of TableFacade.changes / db.changes)", () => {
  const insert = {
    type: "items",
    key: "a",
    value: { id: "a", value: 1 },
    headers: { operation: "insert" },
  } satisfies ChangeMessage.Message
  const update = {
    type: "items",
    key: "a",
    value: { id: "a", value: 2 },
    headers: { operation: "update" },
  } satisfies ChangeMessage.Message
  const remove = {
    type: "items",
    key: "a",
    headers: { operation: "delete" },
  } satisfies ChangeMessage.Message

  it.effect("projects a typed TableChange, decoding the row (insert/update/delete)", () =>
    Effect.gen(function*() {
      expect(yield* tableChangeOf(Item.schema, 7, insert)).toEqual({ _tag: "Insert", seq: 7, key: "a", row: { id: "a", value: 1 } })
      expect(yield* tableChangeOf(Item.schema, 8, update)).toEqual({ _tag: "Update", seq: 8, key: "a", row: { id: "a", value: 2 } })
      expect(yield* tableChangeOf(Item.schema, 9, remove)).toEqual({ _tag: "Delete", seq: 9, key: "a" })
    }))

  it("projects an untyped DbChange (no row decode), preserving type/key/seq", () => {
    expect(dbChangeOf(7, insert)).toEqual({ _tag: "Insert", seq: 7, type: "items", key: "a", value: { id: "a", value: 1 } })
    expect(dbChangeOf(9, remove)).toEqual({ _tag: "Delete", seq: 9, type: "items", key: "a" })
  })
})

describe("Table definition", () => {
  it("derives metadata from the schema-owned primary key", () => {
    expect(Item.tableName).toBe("items")
    expect(Item.pkField).toBe("id")
  })

  it("rejects tables without a primary key", () => {
    expect(() =>
      Table("broken")({
        id: Schema.String,
        value: Schema.Number,
      }),
    ).toThrow(/no primaryKey/)
  })
})

describe("ChangeMessage projection", () => {
  it.effect("encodes, decodes, and folds latest-value rows", () =>
    Effect.gen(function*() {
      const state = MaterializedState.empty()
      const insert = {
        type: "items",
        key: "a",
        value: { id: "a", value: 1 },
        headers: { operation: "insert" },
      } satisfies ChangeMessage.Message
      const update = {
        type: "items",
        key: "a",
        value: { id: "a", value: 2 },
        headers: { operation: "update" },
      } satisfies ChangeMessage.Message
      const remove = {
        type: "items",
        key: "a",
        headers: { operation: "delete" },
      } satisfies ChangeMessage.Message

      state.apply(yield* ChangeMessage.decode(yield* ChangeMessage.encode(insert)))
      expect(state.get("items", "a")).toEqual(Option.some({ id: "a", value: 1 }))

      state.apply(update)
      expect(state.values("items")).toEqual([{ id: "a", value: 2 }])

      state.apply(remove)
      expect(Option.isNone(state.get("items", "a"))).toBe(true)
    }))
})
