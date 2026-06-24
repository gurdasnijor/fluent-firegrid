import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Option from "effect/Option"
import * as Queue from "effect/Queue"
import * as Schema from "effect/Schema"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { describe, expect, it } from "vitest"

import { EventStream, FlowStreamCodec, TableView } from "../src/index.ts"
import type { AppendBatch, EventRecord, FlowRecord, StreamStore } from "../src/index.ts"
import { InMemoryStreamStore } from "../src/test-support/index.ts"

interface Todo {
  readonly id: string
  readonly text: string
}

const TodoSchema = Schema.fromJsonString(Schema.Struct({
  id: Schema.String,
  text: Schema.String
}))

const Todos = EventStream.make("todos", {
  key: Schema.String,
  value: TodoSchema
})

const runScoped = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>): Promise<A> =>
  effect.pipe(Effect.scoped, Effect.runPromise)

const todo = (id: string, text: string): Todo => ({ id, text })

const record = (seqNum: number, key: string, value: Todo): EventRecord<string, Todo> => ({
  stream: EventStream.physicalName(Todos.name, key),
  key,
  value,
  cursor: {
    stream: EventStream.physicalName(Todos.name, key),
    seqNum
  },
  headers: new Map()
})

const flowRecord = (seqNum: number, key: string, value: Todo): FlowRecord<EventRecord<string, Todo>> => ({
  seqNum,
  value: record(seqNum, key, value)
})

const tableDefinition = TableView.define({
  name: "todos-by-id",
  source: Todos,
  key: (event: EventRecord<string, Todo>) => event.key,
  reduce: (state: ReadonlyMap<string, Todo>, event: EventRecord<string, Todo>) =>
    new Map(state).set(event.key, event.value)
})

const makeTable = (store: StreamStore<EventRecord<string, Todo>>) =>
  TableView.make({
    store,
    definition: tableDefinition,
    config: { readTimeout: "500 millis" }
  })

describe("Flow stream codecs", () => {
  it("surfaces encode failures as typed FlowError values", () =>
    Effect.gen(function*() {
      const reason = yield* FlowStreamCodec.encodeValue(Todos, { id: "1" } as Todo).pipe(
        Effect.match({
          onFailure: (error) => error.reason,
          onSuccess: () => "success"
        })
      )

      expect(reason).toBe("encode")
    }).pipe(Effect.runPromise))

  it("surfaces decode failures as typed FlowError values", () =>
    Effect.gen(function*() {
      const reason = yield* FlowStreamCodec.decodeRecord(Todos, {
        seqNum: 0,
        key: "a",
        body: `{"id":"1"}`
      }).pipe(
        Effect.match({
          onFailure: (error) => error.reason,
          onSuccess: () => "success"
        })
      )

      expect(reason).toBe("decode")
    }).pipe(Effect.runPromise))
})

describe("TableView", () => {
  it("folds empty and missing streams to the initial state", () =>
    runScoped(
      Effect.gen(function*() {
        const emptyStore = yield* InMemoryStreamStore.make<EventRecord<string, Todo>>()
        const emptyTable = yield* makeTable(emptyStore)
        const missingStore: StreamStore<EventRecord<string, Todo>> = {
          append: () => Effect.die("unused"),
          checkTail: Effect.succeed(0),
          readSession: () => Stream.empty
        }
        const missingTable = yield* makeTable(missingStore)

        expect(yield* emptyTable.refresh).toBe(0)
        expect(yield* missingTable.refresh).toBe(0)
        expect(Option.isNone(yield* emptyTable.getStrong("a"))).toBeTruthy()
        expect(Option.isNone(yield* missingTable.getStrong("a"))).toBeTruthy()
      })
    ))

  it("getStrong reflects a concurrent write once the tail is applied", () =>
    runScoped(
      Effect.gen(function*() {
        const tail = yield* Queue.unbounded<FlowRecord<EventRecord<string, Todo>>>()
        const store: StreamStore<EventRecord<string, Todo>> = {
          append: (batch: AppendBatch<EventRecord<string, Todo>>) =>
            Effect.succeed({
              startSeqNum: 0,
              endSeqNum: batch.values.length,
              records: batch.values.map((value, seqNum) => ({ seqNum, value }))
            }),
          checkTail: Effect.succeed(1),
          readSession: () => Stream.fromQueue(tail)
        }
        const table = yield* makeTable(store)
        const result = yield* Deferred.make<Option.Option<Todo>>()

        yield* table.getStrong("a").pipe(
          Effect.flatMap((value) => Deferred.succeed(result, value)),
          Effect.forkScoped
        )
        yield* Effect.sleep("20 millis")

        expect((yield* Deferred.poll(result))._tag).toBe("None")

        yield* Queue.offer(tail, flowRecord(0, "a", todo("a", "write")))
        const value = yield* Deferred.await(result)

        expect(Option.getOrUndefined(value)).toEqual(todo("a", "write"))
      })
    ))

  it("delivers each applied mutation once through changes", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<EventRecord<string, Todo>>()
        const table = yield* makeTable(store)
        const changesFiber = yield* table.changes.pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)

        yield* Effect.sleep("20 millis")
        yield* store.externalAppend([
          record(0, "a", todo("a", "first")),
          record(1, "b", todo("b", "second"))
        ])
        const changes = yield* Fiber.join(changesFiber)

        expect([...changes]).toEqual([
          ["a", todo("a", "first")],
          ["b", todo("b", "second")]
        ])
      })
    ))

  it("delivers same-key changes with the value from each applied mutation", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<EventRecord<string, Todo>>()
        const table = yield* makeTable(store)
        const changesFiber = yield* table.changes.pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)

        yield* Effect.sleep("20 millis")
        yield* store.externalAppend([
          record(0, "a", todo("a", "first")),
          record(1, "a", todo("a", "second"))
        ])
        const changes = yield* Fiber.join(changesFiber)

        expect([...changes]).toEqual([
          ["a", todo("a", "first")],
          ["a", todo("a", "second")]
        ])
      })
    ))

  it("cold-starts from a snapshot to the same state as full replay", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<EventRecord<string, Todo>>()
        yield* store.externalAppend([
          record(0, "a", todo("a", "one")),
          record(1, "b", todo("b", "two")),
          record(2, "a", todo("a", "three"))
        ])
        const fullReplay = yield* makeTable(store)
        const fromSnapshot = yield* TableView.make({
          store,
          definition: TableView.define({
            ...tableDefinition,
            snapshot: {
              fromSeqNum: 2,
              entries: [
                ["a", todo("a", "one")],
                ["b", todo("b", "two")]
              ]
            }
          }),
          config: { readTimeout: "500 millis" }
        })

        yield* fullReplay.refresh
        yield* fromSnapshot.refresh

        expect(yield* fullReplay.entries).toEqual(yield* fromSnapshot.entries)
      })
    ))

  it("emits a snapshot followed by future changes", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<EventRecord<string, Todo>>()
        yield* store.externalAppend([record(0, "a", todo("a", "one"))])
        const table = yield* makeTable(store)

        yield* table.refresh
        const valuesFiber = yield* table.snapshotAndChanges.pipe(Stream.take(2), Stream.runCollect, Effect.forkScoped)
        yield* store.externalAppend([record(1, "b", todo("b", "two"))])
        const values = yield* Fiber.join(valuesFiber)

        expect([...values]).toEqual([
          ["a", todo("a", "one")],
          ["b", todo("b", "two")]
        ])
      })
    ))

  it("does not drop a mutation that races snapshotAndChanges startup", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<EventRecord<string, Todo>>()
        yield* store.externalAppend([record(0, "a", todo("a", "one"))])
        const table = yield* makeTable(store)

        yield* table.refresh
        const valuesFiber = yield* table.snapshotAndChanges.pipe(Stream.take(3), Stream.runCollect, Effect.forkScoped)
        yield* Effect.yieldNow
        yield* store.externalAppend([record(1, "b", todo("b", "two"))])
        yield* store.externalAppend([record(2, "c", todo("c", "three"))])
        const values = [...yield* Fiber.join(valuesFiber)]

        expect(values).toEqual([
          ["a", todo("a", "one")],
          ["b", todo("b", "two")],
          ["c", todo("c", "three")]
        ])
        expect(values.filter(([key]) => key === "b")).toHaveLength(1)
      })
    ))
})
