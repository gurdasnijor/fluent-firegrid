import { describe, expect, it } from "@effect/vitest"
import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import * as Schema from "effect/Schema"
import { ChangeMessage, MaterializedState, primaryKey, Table } from "../src/index.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number
}) {}

describe("Table definition", () => {
  it("derives metadata from the schema-owned primary key", () => {
    expect(Item.tableName).toBe("items")
    expect(Item.pkField).toBe("id")
  })

  it("rejects tables without a primary key", () => {
    expect(() =>
      Table("broken")({
        id: Schema.String,
        value: Schema.Number
      })
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
        headers: { operation: "insert" }
      } satisfies ChangeMessage.Message
      const update = {
        type: "items",
        key: "a",
        value: { id: "a", value: 2 },
        headers: { operation: "update" }
      } satisfies ChangeMessage.Message
      const remove = {
        type: "items",
        key: "a",
        headers: { operation: "delete" }
      } satisfies ChangeMessage.Message

      state.apply(yield* ChangeMessage.decode(yield* ChangeMessage.encode(insert)))
      expect(state.get("items", "a")).toEqual(Option.some({ id: "a", value: 1 }))

      state.apply(update)
      expect(state.values("items")).toEqual([{ id: "a", value: 2 }])

      state.apply(remove)
      expect(Option.isNone(state.get("items", "a"))).toBe(true)
    }))
})
