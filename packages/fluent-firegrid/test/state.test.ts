import { describe, expect, it } from "vitest"
import { Effect, Option, Schema } from "effect"

import { FluentDurableContext, type ObjectStateBackend } from "../src/context.ts"
import { FluentFiregridError } from "../src/error.ts"
import {
  cel,
  ChangeMessage,
  evaluateStatePredicate,
  MaterializedState,
  primaryKey,
  state,
  statePredicateEnvironment,
  Table
} from "../src/state.ts"

class Item extends Table<Item>("items")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number
}) {}

const testContext = (
  state: ObjectStateBackend,
  stateOperationId: NonNullable<Parameters<typeof FluentDurableContext.of>[0]["stateOperationId"]>
) =>
  FluentDurableContext.of({
    key: "object-1",
    state,
    stateOperationId,
    sleep: () => Effect.void,
    sleepUntil: () => Effect.void,
    step: () => Effect.fail(new FluentFiregridError({ message: "step not used" })),
    waitForSignal: () => Effect.fail(new FluentFiregridError({ message: "waitForSignal not used" }))
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

describe("state wait predicates", () => {
  it("derives a serializable predicate environment from the table schema", () => {
    expect(statePredicateEnvironment(Item)).toEqual({
      change: {
        key: { name: "key", type: "string" },
        operation: { name: "operation", type: "string" },
        table: { name: "table", type: "string" }
      },
      environmentVersion: "table:items:id:string,value:number",
      old: {
        id: { name: "id", type: "string" },
        value: { name: "value", type: "number" }
      },
      row: {
        id: { name: "id", type: "string" },
        value: { name: "value", type: "number" }
      },
      table: "items"
    })
  })

  it("evaluates serializable CEL predicates against the row context", async () => {
    const result = await Effect.runPromise(
      evaluateStatePredicate(cel("row.value >= 3 && row.id == 'a'"), {
        row: { id: "a", value: 3 }
      })
    )
    expect(result).toBeTruthy()
  })

  it("builds CEL predicates with serializable expression helpers", async () => {
    const predicate = cel.expr((t) =>
      t.row.value!.greaterThanOrEqual(3)
        .and(t.row.id!.eq("a"))
        .and(t.change.operation.in(["insert", "update"]))
    )

    expect(predicate).toEqual({
      expression: "((row.value >= 3) && (row.id == \"a\")) && (change.operation in [\"insert\", \"update\"])",
      language: "cel"
    })
    await expect(
      Effect.runPromise(
        evaluateStatePredicate(predicate, {
          change: {
            key: "a",
            operation: "insert",
            table: "items"
          },
          row: { id: "a", value: 3 }
        })
      )
    ).resolves.toBeTruthy()
  })

  it("rejects CEL predicates that do not return bool", async () => {
    await expect(
      Effect.runPromise(
        evaluateStatePredicate(cel("row.value"), {
          row: { id: "a", value: 3 }
        })
      )
    ).rejects.toMatchObject({
      _tag: "FluentFiregridError"
    })
  })
})

describe("state(Table)", () => {
  it("uses the ambient object state backend with schema decoding", async () => {
    const rows = new Map<string, unknown>()
    const operations = new Array<string>()
    let nextOperation = 0
    const backend: ObjectStateBackend = {
      get: (table, key, options) =>
        Effect.sync(() => {
          operations.push(`get:${options?.readId ?? "none"}`)
          return Option.fromNullishOr(rows.get(`${table}:${key}`))
        }),
      set: (table, key, value, options) =>
        Effect.sync(() => {
          operations.push(`set:${options?.opId ?? "none"}`)
          rows.set(`${table}:${key}`, value)
        }),
      delete: (table, key, options) =>
        Effect.sync(() => {
          operations.push(`delete:${options?.opId ?? "none"}`)
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
        testContext(backend, ({ kind, table, key }) => `state-op:${nextOperation++}:${kind}:${table}:${key}`)
      )
    )

    const result = await Effect.runPromise(program)
    expect(result.current).toEqual(Option.some({ id: "a", value: 3 }))
    expect(Option.isNone(result.removed)).toBeTruthy()
    expect(operations).toEqual([
      "set:state-op:0:set:items:a",
      "get:state-op:1:get:items:a",
      "delete:state-op:2:delete:items:a",
      "get:state-op:3:get:items:a"
    ])
  })

  it("delegates keyed waits to the ambient backend with a serializable predicate", async () => {
    let captured:
      | {
        readonly key: string
        readonly name: string
        readonly environmentVersion?: string
        readonly signalName: string
        readonly table: string
        readonly timeoutAt?: number
        readonly timeoutMs?: number
        readonly waitId?: string
        readonly expression: string
      }
      | undefined
    let nextOperation = 0
    const backend: ObjectStateBackend = {
      get: () => Effect.succeed(Option.none()),
      set: () => Effect.void,
      delete: () => Effect.void,
      waitFor: (table, key, predicate, options) =>
        Effect.sync(() => {
          captured = {
            ...(options.environmentVersion === undefined ? {} : { environmentVersion: options.environmentVersion }),
            expression: predicate.expression,
            key,
            name: options.name,
            signalName: options.signalName,
            table,
            ...(options.timeoutAt === undefined ? {} : { timeoutAt: options.timeoutAt }),
            ...(options.timeoutMs === undefined ? {} : { timeoutMs: options.timeoutMs }),
            ...(options.waitId === undefined ? {} : { waitId: options.waitId })
          }
          return Option.some({ id: key, value: 7 })
        })
    }

    const program = state(Item).waitFor("a", {
      name: "value-ready",
      timeoutMs: 1_000,
      when: cel("row.value >= 7")
    }).pipe(
      Effect.provideService(
        FluentDurableContext,
        {
          ...testContext(backend, ({ kind, table, key }) => `state-op:${nextOperation++}:${kind}:${table}:${key}`),
          now: () => Effect.succeed(1_000)
        }
      )
    )

    await expect(Effect.runPromise(program)).resolves.toEqual({ id: "a", value: 7 })
    expect(captured).toEqual({
      environmentVersion: "table:items:id:string,value:number",
      expression: "row.value >= 7",
      key: "a",
      name: "value-ready",
      signalName: "__firegrid_state_wait:items:a:value-ready",
      table: "items",
      timeoutAt: 2_000,
      timeoutMs: 1_000,
      waitId: "state-op:0:waitFor:items:a"
    })
  })

  it("rejects state waits that reference fields outside the table schema", async () => {
    let backendCalled = false
    const backend: ObjectStateBackend = {
      get: () => Effect.succeed(Option.none()),
      set: () => Effect.void,
      delete: () => Effect.void,
      waitFor: () =>
        Effect.sync(() => {
          backendCalled = true
          return Option.none()
        })
    }

    const program = state(Item).waitFor("a", {
      name: "bad-reference",
      when: cel("row.missing == 'ready'")
    }).pipe(
      Effect.provideService(
        FluentDurableContext,
        testContext(backend, ({ kind, table, key }) => `state-op:0:${kind}:${table}:${key}`)
      )
    )

    await expect(Effect.runPromise(program)).rejects.toMatchObject({
      _tag: "FluentFiregridError",
      message: "invalid state wait predicate for table items: unknown field reference row.missing"
    })
    expect(backendCalled).toBeFalsy()
  })

  it("parks on the ambient signal when the backend registers a wait", async () => {
    let capturedSignal:
      | {
        readonly id?: string
        readonly name: string
      }
      | undefined
    let nextOperation = 0
    const backend: ObjectStateBackend = {
      get: () => Effect.succeed(Option.none()),
      set: () => Effect.void,
      delete: () => Effect.void,
      waitFor: () => Effect.succeed(Option.none())
    }

    const program = state(Item).waitFor("a", {
      name: "value-ready",
      timeoutMs: 5_000,
      when: cel("row.value >= 7")
    }).pipe(
      Effect.provideService(
        FluentDurableContext,
        FluentDurableContext.of({
          key: "object-1",
          state: backend,
          now: () => Effect.succeed(1_000),
          stateOperationId: ({ kind, table, key }) => `state-op:${nextOperation++}:${kind}:${table}:${key}`,
          sleep: () => Effect.void,
          sleepUntil: () => Effect.void,
          step: () => Effect.fail(new FluentFiregridError({ message: "step not used" })),
          waitForSignal: <Payload>(name: string, options?: { readonly deadline?: number; readonly id?: string }) =>
            Effect.sync(() => {
              capturedSignal = {
                ...(options?.id === undefined ? {} : { id: options.id }),
                name
              }
              return { id: "a", value: 9 } as Payload
            })
        })
      )
    )

    await expect(Effect.runPromise(program)).resolves.toEqual({ id: "a", value: 9 })
    expect(capturedSignal).toEqual({
      id: "state-op:0:waitFor:items:a",
      name: "__firegrid_state_wait:items:a:value-ready"
    })
  })

  it("fails with a typed timeout error when the timer wins a state wait", async () => {
    const backend: ObjectStateBackend = {
      get: () => Effect.succeed(Option.none()),
      set: () => Effect.void,
      delete: () => Effect.void,
      waitFor: () => Effect.succeed(Option.none())
    }

    const program = state(Item).waitFor("a", {
      name: "value-ready",
      timeoutMs: 5_000,
      when: cel("row.value >= 7")
    }).pipe(
      Effect.provideService(
        FluentDurableContext,
        FluentDurableContext.of({
          key: "object-1",
          state: backend,
          now: () => Effect.succeed(1_000),
          stateOperationId: ({ kind, table, key }) => `state-op:0:${kind}:${table}:${key}`,
          sleep: () => Effect.void,
          sleepUntil: () => Effect.void,
          step: () => Effect.fail(new FluentFiregridError({ message: "step not used" })),
          waitForSignal: <Payload>() => Effect.succeed({ _tag: "StateWaitTimedOut", name: "value-ready" } as Payload)
        })
      )
    )

    await expect(Effect.runPromise(program)).rejects.toMatchObject({
      _tag: "FluentFiregridError",
      message: "state.waitFor value-ready timed out"
    })
  })
})
