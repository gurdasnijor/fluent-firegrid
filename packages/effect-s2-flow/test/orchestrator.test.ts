import * as Deferred from "effect/Deferred"
import * as Effect from "effect/Effect"
import * as Queue from "effect/Queue"
import type * as Scope from "effect/Scope"
import * as Stream from "effect/Stream"
import { describe, expect, it } from "vitest"

import { OwnedOrchestrator, ViewOrchestrator } from "../src/index.ts"
import type { AppendAck, AppendBatch, FlowRecord, StreamStore } from "../src/index.ts"
import { InMemoryStreamStore } from "../src/test-support/index.ts"

const runScoped = <A, E>(effect: Effect.Effect<A, E, Scope.Scope>): Promise<A> =>
  effect.pipe(Effect.scoped, Effect.runPromise)

const reduceNumbers = (state: ReadonlyArray<number>, record: { readonly value: number }) => [
  ...state,
  record.value
]

const ackAt = (batch: AppendBatch<number>, seqNum: number): AppendAck<number> => ({
  startSeqNum: seqNum,
  endSeqNum: seqNum + 1,
  records: [{
    seqNum,
    value: batch.values[0]!,
    ...(batch.ownerId === undefined ? {} : { ownerId: batch.ownerId }),
    ...(batch.writeId === undefined ? {} : { writeId: batch.writeId })
  }]
})

describe("ViewOrchestrator", () => {
  it("serves eventual reads from the applied tail", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<number>()
        yield* store.externalAppend([1, 2])
        const view = yield* ViewOrchestrator.make({
          store,
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers
        })

        yield* view.readStrong((s) => s)
        const state = yield* view.read((s) => s)

        expect(state).toEqual([1, 2])
        expect(yield* view.applied).toBe(2)
      })
    ))

  it("blocks strong reads until the checked tail has been applied", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<number>()
        yield* store.externalAppend([1])
        const view = yield* ViewOrchestrator.make({
          store,
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers
        })

        const state = yield* view.readStrong((s) => s)

        expect(state).toEqual([1])
      })
    ))

  it("recovers from a cursor by folding only records at or after it", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<number>()
        yield* store.externalAppend([1, 2, 3])
        const view = yield* ViewOrchestrator.make({
          store,
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers,
          fromSeqNum: 2
        })

        const state = yield* view.readStrong((s) => s)

        expect(state).toEqual([3])
        expect(yield* view.applied).toBe(3)
      })
    ))

  it("times out a strong read when the tail reader never reaches the checked tail", () =>
    runScoped(
      Effect.gen(function*() {
        const view = yield* ViewOrchestrator.make({
          store: {
            append: () => Effect.die("unused"),
            checkTail: Effect.succeed(1),
            readSession: () => Stream.never
          },
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers,
          config: { readTimeout: 10 }
        })

        const reason = yield* view.readStrong((s) => s).pipe(
          Effect.match({
            onFailure: (error) => error.reason,
            onSuccess: () => "success"
          })
        )

        expect(reason).toBe("read-timeout")
      })
    ))

  it("keeps generated write/read-strong histories linearizable against an array model", () =>
    runScoped(
      Effect.gen(function*() {
        for (let mask = 0; mask < 16; mask++) {
          const store = yield* InMemoryStreamStore.make<number>()
          const view = yield* ViewOrchestrator.make({
            store,
            initial: [] as ReadonlyArray<number>,
            reduce: reduceNumbers
          })
          const model: Array<number> = []

          for (let step = 0; step < 4; step++) {
            if ((mask & (1 << step)) === 0) {
              const value = model.length + 1
              model.push(value)
              yield* store.externalAppend([value])
            } else {
              const observed = yield* view.readStrong((s) => s)
              expect(observed).toEqual(model)
            }
          }

          const observed = yield* view.readStrong((s) => s)
          expect(observed).toEqual(model)
        }
      })
    ))
})

describe("OwnedOrchestrator", () => {
  it("completes writes only after ordered local apply, giving read-your-writes", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<number>()
        const owned = yield* OwnedOrchestrator.make({
          store,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers
        })

        const ack = yield* owned.write([1])
        const state = yield* owned.read((s) => s)

        expect(ack.startSeqNum).toBe(0)
        expect(state).toEqual([1])
        expect(yield* owned.applied).toBe(1)
      })
    ))

  it("does not double-apply its own records when the tail reader observes them", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<number>()
        const owned = yield* OwnedOrchestrator.make({
          store,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers
        })

        yield* owned.write([1])
        yield* owned.write([2])
        const state = yield* owned.read((s) => s)

        expect(state).toEqual([1, 2])
      })
    ))

  it("does not reorder an own ack after an earlier foreign record", () =>
    runScoped(
      Effect.gen(function*() {
        const store = yield* InMemoryStreamStore.make<number>()
        const owned = yield* OwnedOrchestrator.make({
          store,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers
        })

        yield* store.externalAppend([10])
        yield* owned.write([20])
        const state = yield* owned.read((s) => s)

        expect(state).toEqual([10, 20])
      })
    ))

  it("holds an acked own write until an earlier foreign record is applied", () =>
    runScoped(
      Effect.gen(function*() {
        const tail = yield* Queue.unbounded<FlowRecord<number>>()
        const store: StreamStore<number> = {
          append: (batch: AppendBatch<number>) => Effect.succeed(ackAt(batch, 1)),
          checkTail: Effect.succeed(2),
          readSession: () => Stream.fromQueue(tail)
        }
        const owned = yield* OwnedOrchestrator.make({
          store,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers,
          config: { writeTimeout: "500 millis" }
        })
        const result = yield* Deferred.make<unknown>()

        yield* owned.write([20]).pipe(
          Effect.matchEffect({
            onFailure: (error) => Deferred.succeed(result, error.reason),
            onSuccess: (ack) => Deferred.succeed(result, ack)
          }),
          Effect.forkScoped
        )
        yield* Effect.sleep("20 millis")
        const beforeForeignRecord = yield* Deferred.poll(result)

        expect(beforeForeignRecord._tag).toBe("None")

        yield* Queue.offer(tail, { seqNum: 0, value: 10 })
        const ack = yield* Deferred.await(result)
        const state = yield* owned.read((s) => s)

        expect(ack).toMatchObject({ startSeqNum: 1, endSeqNum: 2 })
        expect(state).toEqual([10, 20])
        expect(yield* owned.applied).toBe(2)
      })
    ))

  it("fails an owned write when its ack cannot be ordered-applied before the deadline", () =>
    runScoped(
      Effect.gen(function*() {
        const store: StreamStore<number> = {
          append: (batch: AppendBatch<number>) => Effect.succeed(ackAt(batch, 1)),
          checkTail: Effect.succeed(2),
          readSession: () => Stream.never
        }
        const owned = yield* OwnedOrchestrator.make({
          store,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers,
          config: { writeTimeout: 10 }
        })
        const reason = yield* owned.write([20]).pipe(
          Effect.match({
            onFailure: (error) => error.reason,
            onSuccess: () => "success"
          })
        )

        expect(reason).toBe("write-timeout")
      })
    ))

  it("passes the configured fencing token through every owned append", () =>
    runScoped(
      Effect.gen(function*() {
        const store: StreamStore<number> = {
          append: (batch: AppendBatch<number>) =>
            batch.fencingToken === "token-a"
              ? Effect.succeed(ackAt(batch, 0))
              : Effect.die("missing fencing token"),
          checkTail: Effect.succeed(1),
          readSession: () => Stream.never
        }
        const owned = yield* OwnedOrchestrator.make({
          store,
          ownerId: "owner-a",
          fencingToken: "token-a",
          initial: [] as ReadonlyArray<number>,
          reduce: reduceNumbers
        })

        yield* owned.write([1])
        const state = yield* owned.read((s) => s)

        expect(state).toEqual([1])
      })
    ))
})
