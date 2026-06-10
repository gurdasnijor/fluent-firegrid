import { Chunk, Effect } from "effect"
import { describe, expect, it } from "vitest"
import { decodeStreamPath } from "@firegrid/fluent-store"
import * as InMemoryStreamLog from "@firegrid/fluent-store-inmemory"
import {
  Append,
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
      expected: "text/plain",
      actual: "application/json",
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

  it("streams live reads through a Mailbox", async () => {
    const events = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const path = yield* decodeStreamPath("protocol/live")
          const log = yield* InMemoryStreamLog.makeInMemoryStreamLog()
          const transport = yield* makeLocalTransport(log)
          yield* transport.call(new Create({ path, contentType: "text/plain" }))

          const mailbox = yield* transport.stream(new ReadLive({ path, offset: "-1" }))
          yield* transport.call(new Append({ path, contentType: "text/plain", bytes: enc.encode("live") }))
          const [items] = yield* mailbox.takeAll
          return Chunk.toReadonlyArray(items)
        }),
      ),
    )

    expect(events.map((event) => event._tag)).toContain("RecordBatch")
  })
})
