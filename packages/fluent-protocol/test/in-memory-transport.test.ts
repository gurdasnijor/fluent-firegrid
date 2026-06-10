import { Effect, Queue } from "effect"
import { describe, expect, it } from "vitest"
import { decodeStreamPath } from "@firegrid/fluent-store"
import * as InMemoryStreamLog from "@firegrid/fluent-store-inmemory"
import {
  Append,
  Close,
  Create,
  ProducerFence,
  Read,
  ReadLive,
  makeLocalTransport,
} from "../src/index.ts"

const enc = new TextEncoder()

describe("DurableTransport in-memory", () => {
  it("maps store operations to typed protocol outcomes", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("protocol/orders")
        const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
        const transport = yield* makeLocalTransport(log)

        const created = yield* transport.call(new Create({ path, contentType: "text/plain" }))
        const appended = yield* transport.call(
          new Append({ path, contentType: "text/plain", bytes: enc.encode("hello") }),
        )
        const read = yield* transport.call(new Read({ path, offset: "-1" }))
        const mismatch = yield* transport.call(
          new Append({ path, contentType: "application/json", bytes: enc.encode("{}") }),
        )

        return { created, appended, read, mismatch }
      }),
    )

    expect(result.created).toMatchObject({ _tag: "Created", contentType: "text/plain" })
    expect(result.appended).toMatchObject({ _tag: "Appended", closed: false })
    expect(result.read).toMatchObject({
      _tag: "ReadResult",
      records: [{ bytes: enc.encode("hello") }],
      upToDate: true,
    })
    expect(result.mismatch).toMatchObject({
      _tag: "ContentMismatch",
      code: "content-mismatch",
      expected: "text/plain",
      actual: "application/json",
    })
  })

  it("returns typed offset conflicts and idempotent close responses", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("protocol/close")
        const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
        const transport = yield* makeLocalTransport(log)
        yield* transport.call(new Create({ path, contentType: "text/plain" }))
        yield* transport.call(new Append({ path, contentType: "text/plain", bytes: enc.encode("one") }))

        const conflict = yield* transport.call(
          new Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("stale"),
            expectedTailOffset: "00000000000000000000" as Append["expectedTailOffset"],
          }),
        )
        const firstClose = yield* transport.call(new Close({ path }))
        const secondClose = yield* transport.call(new Close({ path }))

        return { conflict, firstClose, secondClose }
      }),
    )

    expect(result.conflict).toMatchObject({
      _tag: "OffsetConflict",
      code: "offset-conflict",
      expectedTailOffset: "00000000000000000000",
      actualTailOffset: "00000000000000000001",
    })
    expect(result.firstClose).toMatchObject({
      _tag: "Appended",
      nextOffset: "00000000000000000002",
      closed: true,
    })
    expect(result.secondClose).toMatchObject({
      _tag: "Closed",
      finalOffset: "00000000000000000002",
    })
  })

  it("maps producer fencing to typed append outcomes", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("protocol/producers")
        const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
        const transport = yield* makeLocalTransport(log)
        yield* transport.call(new Create({ path, contentType: "text/plain" }))

        const first = yield* transport.call(
          new Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("one"),
            producer: new ProducerFence({ producerId: "p1", epoch: 0, seq: 0 }),
          }),
        )
        const duplicate = yield* transport.call(
          new Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("one-again"),
            producer: new ProducerFence({ producerId: "p1", epoch: 0, seq: 0 }),
          }),
        )
        const gap = yield* transport.call(
          new Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("gap"),
            producer: new ProducerFence({ producerId: "p1", epoch: 0, seq: 2 }),
          }),
        )
        yield* transport.call(
          new Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("claim"),
            producer: new ProducerFence({ producerId: "p1", epoch: 1, seq: 0 }),
          }),
        )
        const fenced = yield* transport.call(
          new Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("old"),
            producer: new ProducerFence({ producerId: "p1", epoch: 0, seq: 1 }),
          }),
        )

        return { first, duplicate, gap, fenced }
      }),
    )

    expect(result.first).toMatchObject({ _tag: "Appended" })
    expect(result.duplicate).toMatchObject({ _tag: "AppendDuplicate" })
    expect(result.gap).toMatchObject({ _tag: "SequenceGap", expectedSeq: 1, receivedSeq: 2 })
    expect(result.fenced).toMatchObject({ _tag: "EpochFenced", currentEpoch: 1 })
  })

  it("streams live reads through a Queue", async () => {
    const event = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const path = yield* decodeStreamPath("protocol/live")
          const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
          const transport = yield* makeLocalTransport(log)
          yield* transport.call(new Create({ path, contentType: "text/plain" }))

          const queue = yield* transport.stream(new ReadLive({ path, offset: "-1" }))
          yield* transport.call(new Append({ path, contentType: "text/plain", bytes: enc.encode("live") }))
          return yield* Queue.take(queue).pipe(Effect.orDie)
        }),
      ),
    )

    expect(event._tag).toBe("RecordBatch")
  })
})
