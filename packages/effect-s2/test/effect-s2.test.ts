import {
  AppendRecord,
  S2Conflict,
  conditionalAppend,
  publish,
  readDecoded,
  S2Client,
} from "effect-s2"
import * as TestS2 from "effect-s2/testing"
import { Effect, Fiber, Schema, Stream } from "effect"
import { describe, expect, it } from "@effect/vitest"

const provideTestS2 = <A, E>(effect: Effect.Effect<A, E, S2Client>) =>
  effect.pipe(Effect.provide(TestS2.layer), Effect.runPromise)

const textDecoder = new TextDecoder()
const bodyBytes = (record: { readonly body: Uint8Array }): string => textDecoder.decode(record.body)
const headerBytes = (record: {
  readonly headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>
}): ReadonlyArray<readonly [string, string]> =>
  record.headers.map(([key, value]) => [textDecoder.decode(key), textDecoder.decode(value)])

describe("effect-s2", () => {
  it("reads appended records by seqNum", async () => {
    const records = await provideTestS2(
      Effect.gen(function*() {
        yield* S2Client.createStream("events")
        yield* S2Client.append("events", [
          AppendRecord.string({ body: "a" }),
          AppendRecord.string({ body: "b" }),
          AppendRecord.string({ body: "c" }),
        ])
        return yield* S2Client.read("events", {
          start: { from: { seqNum: 0 } },
          stop: { limits: { count: 3 } },
        }).pipe(Stream.runCollect)
      }),
    )

    expect(records.map((record) => record.body)).toEqual(["a", "b", "c"])
    expect(records.map((record) => record.seqNum)).toEqual([0, 1, 2])
  })

  it("supports scoped append sessions for ordered batch production", async () => {
    const result = await provideTestS2(
      Effect.scoped(
        Effect.gen(function*() {
          yield* S2Client.createStream("session")
          const session = yield* S2Client.appendSession("session", {
            maxInflightBytes: 1024 * 1024,
            maxInflightBatches: 2,
          })
          const first = yield* session.submit([
            AppendRecord.string({ body: "a" }),
            AppendRecord.string({ body: "b" }),
          ])
          const second = yield* session.submit([AppendRecord.string({ body: "c" })])
          const records = yield* S2Client.read("session", {
            start: { from: { seqNum: 0 } },
            stop: { limits: { count: 3 } },
          }).pipe(Stream.runCollect)
          return { first, second, records }
        }),
      ),
    )

    expect([result.first.start.seqNum, result.first.end.seqNum]).toEqual([0, 2])
    expect([result.second.start.seqNum, result.second.end.seqNum]).toEqual([2, 3])
    expect(result.records.map((record) => record.body)).toEqual(["a", "b", "c"])
  })

  it("supports byte-oriented consumption for mixed string and bytes records", async () => {
    const records = await provideTestS2(
      Effect.gen(function*() {
        yield* S2Client.createStream("bytes")
        yield* S2Client.append("bytes", [
          AppendRecord.string({ body: "text", headers: [["kind", "string"]] }),
          AppendRecord.bytes({
            body: new Uint8Array([0, 1, 2, 255]),
            headers: [[new Uint8Array([1, 2]), new Uint8Array([3, 4])]],
          }),
        ])
        return yield* S2Client.readBytes("bytes", {
          start: { from: { seqNum: 0 } },
          stop: { limits: { count: 2 } },
        }).pipe(Stream.runCollect)
      }),
    )

    expect(records.length).toBe(2)
    const [first, second] = records
    if (first === undefined || second === undefined) {
      throw new Error("expected two byte records")
    }
    expect(bodyBytes(first)).toBe("text")
    expect(headerBytes(first)).toEqual([["kind", "string"]])
    expect(Array.from(second.body)).toEqual([0, 1, 2, 255])
    expect(second.headers.map(([key, value]) => [Array.from(key), Array.from(value)])).toEqual([
      [[1, 2], [3, 4]],
    ])
  })

  it("applies conditional append checks through append sessions", async () => {
    const error = await Effect.scoped(
      Effect.gen(function*() {
        yield* S2Client.createStream("session-cas")
        const session = yield* S2Client.appendSession("session-cas")
        yield* session.submit([AppendRecord.string({ body: "first" })], { matchSeqNum: 0 })
        return yield* session.submit([AppendRecord.string({ body: "replay" })], { matchSeqNum: 0 }).pipe(
          Effect.match({
            onFailure: (failure) => failure,
            onSuccess: () => undefined,
          }),
        )
      }),
    ).pipe(Effect.provide(TestS2.layer), Effect.runPromise)

    expect(error).toBeInstanceOf(S2Conflict)
    if (error instanceof S2Conflict) {
      expect(error.expectedSeqNum).toBe(0)
      expect(error.observedSeqNum).toBe(1)
    }
  })

  it("submits producer records in stream order", async () => {
    const records = await provideTestS2(
      Effect.scoped(
        Effect.gen(function*() {
          yield* S2Client.createStream("producer")
          const producer = yield* S2Client.producer("producer", {
            lingerDurationMillis: 1,
            maxBatchRecords: 2,
            maxInflightBytes: 1024 * 1024,
          })
          yield* Effect.forEach(["p0", "p1", "p2", "p3"], (body) =>
            producer.submit(AppendRecord.string({ body })),
          )
          return yield* S2Client.read("producer", {
            start: { from: { seqNum: 0 } },
            stop: { limits: { count: 4 } },
          }).pipe(Stream.runCollect)
        }),
      ),
    )

    expect(records.map((record) => record.body)).toEqual(["p0", "p1", "p2", "p3"])
    expect(records.map((record) => record.seqNum)).toEqual([0, 1, 2, 3])
  })

  it("observes live records appended after the reader starts", async () => {
    const records = await provideTestS2(
      Effect.gen(function*() {
        yield* S2Client.createStream("live")
        const fiber = yield* S2Client.read("live", {
          start: { from: { tailOffset: 0 } },
        }).pipe(Stream.take(2), Stream.runCollect, Effect.forkChild)
        yield* Effect.sleep("10 millis")
        yield* S2Client.append("live", [AppendRecord.string({ body: "x" })])
        yield* S2Client.append("live", [AppendRecord.string({ body: "y" })])
        return yield* Fiber.join(fiber)
      }),
    )

    expect(records.map((record) => record.body)).toEqual(["x", "y"])
  })

  it("fails replayed conditional append with S2Conflict", async () => {
    const error = await Effect.gen(function*() {
      yield* S2Client.createStream("journal")
      const tail = yield* S2Client.checkTail("journal")
      yield* conditionalAppend("journal", Schema.String, "V", tail.tail.seqNum)
      return yield* conditionalAppend("journal", Schema.String, "V", tail.tail.seqNum).pipe(
        Effect.match({
          onFailure: (failure) => failure,
          onSuccess: () => undefined,
        }),
      )
    }).pipe(Effect.provide(TestS2.layer), Effect.runPromise)

    expect(error).toBeInstanceOf(S2Conflict)
    if (error instanceof S2Conflict) {
      expect(error.expectedSeqNum).toBe(0)
      expect(error.observedSeqNum).toBe(1)
    }

    const records = await provideTestS2(
      Effect.gen(function*() {
        yield* S2Client.createStream("journal")
        const tail = yield* S2Client.checkTail("journal")
        yield* conditionalAppend("journal", Schema.String, "V", tail.tail.seqNum)
        return yield* readDecoded("journal", Schema.String, {
          start: { from: { seqNum: 0 } },
          stop: { limits: { count: 1 } },
        }).pipe(Stream.runCollect)
      }),
    )

    expect(records).toEqual(["V"])
  })

  it("roundtrips typed channel values", async () => {
    class Order extends Schema.Class<Order>("Order")({
      id: Schema.String,
      total: Schema.Number,
    }) {}

    const values = await provideTestS2(
      Effect.gen(function*() {
        yield* S2Client.createStream("orders")
        yield* publish("orders", Order, Order.make({ id: "o-1", total: 42 }))
        return yield* readDecoded("orders", Order, {
          start: { from: { seqNum: 0 } },
          stop: { limits: { count: 1 } },
        }).pipe(Stream.runCollect)
      }),
    )

    expect(values).toEqual([Order.make({ id: "o-1", total: 42 })])
  })

  it("surfaces schema failures without corrupting the stream", async () => {
    class Order extends Schema.Class<Order>("Order")({
      id: Schema.String,
      total: Schema.Number,
    }) {}

    const result = await provideTestS2(
      Effect.gen(function*() {
        yield* S2Client.createStream("poison")
        yield* S2Client.append("poison", [
          AppendRecord.string({ body: "{\"id\":\"bad\",\"total\":\"oops\"}" }),
          AppendRecord.string({ body: "{\"id\":\"good\",\"total\":12}" }),
        ])
        const exit = yield* readDecoded("poison", Order, {
          start: { from: { seqNum: 0 } },
          stop: { limits: { count: 1 } },
        }).pipe(Stream.runCollect, Effect.exit)
        const valid = yield* readDecoded("poison", Order, {
          start: { from: { seqNum: 1 } },
          stop: { limits: { count: 1 } },
        }).pipe(Stream.runCollect)
        return { exit, valid }
      }),
    )

    expect(result.exit._tag).toBe("Failure")
    expect(result.valid).toEqual([Order.make({ id: "good", total: 12 })])
  })
})
