import { describe, expect, it } from "vitest"
import { Effect, Option, Schema } from "effect"

import { FluentDurableContext, type ObjectStateBackend, type RunAction } from "../src/context.ts"
import type { FluentFiregridError } from "../src/error.ts"
import { ChangeMessage, MaterializedState, primaryKey, state, Table } from "../src/state.ts"

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
  it("encodes, decodes, and folds latest-value rows", async () => {
    const program = Effect.gen(function*() {
      const materialized = MaterializedState.empty()
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

      materialized.apply(yield* ChangeMessage.decode(yield* ChangeMessage.encode(insert)))
      expect(materialized.get("items", "a")).toEqual(Option.some({ id: "a", value: 1 }))

      materialized.apply(update)
      expect(materialized.values("items")).toEqual([{ id: "a", value: 2 }])

      materialized.apply(remove)
      expect(Option.isNone(materialized.get("items", "a"))).toBeTruthy()
    })

    await Effect.runPromise(program)
  })
})

describe("state(Table)", () => {
  it("uses the ambient object state backend with schema decoding", async () => {
    const rows = new Map<string, unknown>()
    const backend: ObjectStateBackend = {
      get: (table, key) => Effect.succeed(Option.fromNullishOr(rows.get(`${table}:${key}`))),
      set: (table, key, value) =>
        Effect.sync(() => {
          rows.set(`${table}:${key}`, value)
        }),
      delete: (table, key) =>
        Effect.sync(() => {
          rows.delete(`${table}:${key}`)
        })
    }
    const items = state(Item)

    const program = Effect.gen(function*() {
      yield* items.set({ id: "a", value: 3 })
      const current = yield* items.get("a")
      yield* items.delete("a")
      const removed = yield* items.get("a")
      return { current, removed }
    }).pipe(
      Effect.provideService(
        FluentDurableContext,
        FluentDurableContext.of({
          key: "object-1",
          state: backend,
          sleep: () => Effect.void,
          sleepUntil: () => Effect.void,
          step: <A>(_name: string, action: RunAction<A>) => {
            const value = action({ attempt: 1, id: "step", signal: new AbortController().signal })
            return (Effect.isEffect(value)
              ? value
              : Effect.promise(() => Promise.resolve(value))) as Effect.Effect<A, FluentFiregridError>
          },
          waitForSignal: () => Effect.die("not used")
        })
      )
    )

    const result = await Effect.runPromise(program)
    expect(result.current).toEqual(Option.some({ id: "a", value: 3 }))
    expect(Option.isNone(result.removed)).toBeTruthy()
  })
})
