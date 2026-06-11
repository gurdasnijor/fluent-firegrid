import { Cause, Effect, Queue } from "effect"
import { describe, expect, it } from "vitest"
import { decodeStreamPath, initialOffset } from "@firegrid/fluent-stream-log"
import * as InMemoryStreamLog from "@firegrid/fluent-stream-log-inmemory"
import * as Protocol from "@firegrid/fluent-protocol"
import * as DurableStreamsClient from "../../src/client/DurableStreamsClient.ts"
import { makeProducer } from "../../src/client/Producer.ts"

const enc = new TextEncoder()
const dec = new TextDecoder()

describe("DurableStreamsClient", () => {
  it("round-trips create, append, read, head, and delete over DurableTransport", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const path = yield* decodeStreamPath("client/orders")
          const log = yield* InMemoryStreamLog.make()
          const transport = yield* Protocol.makeLocalTransport(log)
          const client = DurableStreamsClient.make(transport)

          yield* client.create(path, "text/plain")
          const append = yield* client.append(path, "text/plain", enc.encode("hello"))
          yield* Effect.all([
            client.append(path, "text/plain", enc.encode(" concurrent-a")),
            client.append(path, "text/plain", enc.encode(" concurrent-b")),
          ], { concurrency: "unbounded" })
          const read = yield* client.read(path)
          if (read._tag !== "ReadResult") {
            return yield* Effect.fail(new Error(`Expected ReadResult, got ${read._tag}`))
          }
          const head = yield* client.head(path)
          const deleted = yield* client.delete(path)

          return {
            append,
            read,
            head,
            deleted,
            body: read.records.map((record) => dec.decode(record.bytes)).join(""),
          }
        }),
      ),
    )

    expect(result.append).toMatchObject({ _tag: "Appended", closed: false })
    expect(result.head).toMatchObject({ _tag: "HeadResult", contentType: "text/plain" })
    expect(result.deleted).toMatchObject({ _tag: "Deleted" })
    expect(result.read).toMatchObject({ _tag: "ReadResult", upToDate: true })
    expect(result.body.startsWith("hello")).toBe(true)
    expect(result.body).toContain("concurrent-a")
    expect(result.body).toContain("concurrent-b")
  })

  it("surfaces append outcomes as typed protocol values", async () => {
    const outcome = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("client/mismatch")
        const log = yield* InMemoryStreamLog.make()
        const transport = yield* Protocol.makeLocalTransport(log)
        const client = DurableStreamsClient.make(transport)

        yield* client.create(path, "text/plain")
        return yield* client.append(path, "application/json", enc.encode("{}"))
      }),
    )

    expect(outcome).toMatchObject({
      _tag: "ContentMismatch",
      code: "content-mismatch",
      expected: "text/plain",
      actual: "application/json",
    })
  })

  it("tail emits caught-up, closed control, then completes", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const path = yield* decodeStreamPath("client/tail-close")
          const log = yield* InMemoryStreamLog.make()
          const transport = yield* Protocol.makeLocalTransport(log)
          const client = DurableStreamsClient.make(transport)

          yield* client.create(path, "text/plain")
          const tail = yield* client.tail(path, "-1")
          const caughtUp = yield* Queue.take(tail)
          yield* client.close(path)
          const closed = yield* Queue.take(tail)
          const done = yield* Queue.take(tail).pipe(Effect.flip)
          return { caughtUp, closed, done }
        }),
      ),
    )

    expect(result.caughtUp).toMatchObject({
      _tag: "Control",
      upToDate: true,
      closed: false,
    })
    expect(result.closed).toMatchObject({
      _tag: "Control",
      upToDate: true,
      closed: true,
    })
    expect(Cause.isDone(result.done)).toBe(true)
  })

  it("producer retries transport failures with the same fence tuple", async () => {
    const seenSeqs: number[] = []
    const transport: Protocol.DurableTransportService = {
      call: ((request: Protocol.Request) => {
        if (request._tag !== "Append") {
          return Effect.die("unexpected request")
        }
        seenSeqs.push(request.producer?.seq ?? -1)
        return seenSeqs.length === 1
          ? Effect.fail(new Protocol.TransportError({ message: "connection died" }))
          : Effect.succeed(new Protocol.Appended({ nextOffset: initialOffset, closed: false }))
      }) as Protocol.DurableTransportService["call"],
      stream: () => Effect.die("unused"),
    }

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("client/producer-retry")
        const producer = yield* makeProducer(transport, {
          path,
          contentType: "text/plain",
          producerId: "p1",
          maxTransportRetries: 1,
        })
        const append = yield* producer.append(enc.encode("hello"))
        const state = yield* producer.state
        return { append, state }
      }),
    )

    expect(result.append).toMatchObject({ _tag: "Appended" })
    expect(result.state).toEqual({ epoch: 0, seq: 1 })
    expect(seenSeqs).toEqual([0, 0])
  })

  it("producer can auto-claim a newer epoch after being fenced", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const path = yield* decodeStreamPath("client/producer-autoclaim")
        const log = yield* InMemoryStreamLog.make()
        const transport = yield* Protocol.makeLocalTransport(log)
        yield* transport.call(new Protocol.Create({ path, contentType: "text/plain" }))
        yield* transport.call(
          new Protocol.Append({
            path,
            contentType: "text/plain",
            bytes: enc.encode("claimed"),
            producer: new Protocol.ProducerFence({ producerId: "p1", epoch: 2, seq: 0 }),
          }),
        )

        const producer = yield* makeProducer(transport, {
          path,
          contentType: "text/plain",
          producerId: "p1",
          autoClaim: true,
        })
        const append = yield* producer.append(enc.encode("after-fence"))
        const state = yield* producer.state
        return { append, state }
      }),
    )

    expect(result.append).toMatchObject({ _tag: "Appended" })
    expect(result.state).toEqual({ epoch: 3, seq: 1 })
  })
})
