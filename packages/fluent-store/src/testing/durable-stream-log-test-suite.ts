/* eslint-disable effect/no-runPromise -- Reusable Vitest contract suite runs Effects at test boundaries. */
import { Chunk, Effect, Fiber, Stream, pipe } from "effect"
import { describe, expect, it } from "vitest"
import { appendBytes, appendEmpty, beginning, readCollect } from "../streamLog.ts"
import { ContentTypeMismatchError, StreamNotFoundError } from "../errors.ts"
import { decodeStreamPath } from "../domainTypes.ts"
import type { DurableStreamLog } from "../services.ts"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const bytes = (text: string) => encoder.encode(text)
const text = (input: Uint8Array) => decoder.decode(input)
const toArray = Chunk.toReadonlyArray

export interface DurableStreamLogTestOptions {
  readonly supportsMissingHistoricalRead?: boolean
}

export const runDurableStreamLogTestSuite = (
  name: string,
  makeLog: () => Effect.Effect<DurableStreamLog, never>,
  options: DurableStreamLogTestOptions = {},
) => {
  const { supportsMissingHistoricalRead = true } = options

  describe(`${name} DurableStreamLog`, () => {
    it("creates a stream and exposes metadata through head", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/create")
          yield* log.create({ path, contentType: "text/plain" })
          return yield* log.head(path)
        }),
      )

      expect(result.contentType).toBe("text/plain")
      expect(result.closed).toBe(false)
    })

    it("appends bytes and reads historical records from the beginning", async () => {
      const records = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/read")
          yield* log.create({ path, contentType: "text/plain" })
          yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("a"))
          yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("b"))
          return yield* readCollect(log, beginning(path))
        }),
      )

      expect(toArray(records).map((record) => text(record.bytes))).toEqual(["a", "b"])
    })

    it("rejects appends with a mismatched content type", async () => {
      const error = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/content-type")
          yield* log.create({ path, contentType: "text/plain" })
          return yield* pipe(
            appendBytes(log, { path, contentType: "application/json" }, bytes("{}")),
            Effect.flip,
          )
        }),
      )

      expect(error).toBeInstanceOf(ContentTypeMismatchError)
    })

    it("subscribes with historical replay followed by live records", async () => {
      const records = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/subscribe")
          yield* log.create({ path, contentType: "text/plain" })
          yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("historical"))
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* log.subscribe(beginning(path))
              const fiber = yield* pipe(stream, Stream.take(2), Stream.runCollect, Effect.fork)
              yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("live"))
              return yield* Fiber.join(fiber)
            }),
          )
        }),
      )

      expect(toArray(records).map((record) => text(record.bytes))).toEqual(["historical", "live"])
    })

    it("does not create missing streams through subscribe", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/missing-subscribe")
          const error = yield* Effect.scoped(log.subscribe(beginning(path))).pipe(Effect.flip)
          const created = yield* log.create({ path, contentType: "text/plain" })
          const append = yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("created"))
          return { error, created, append }
        }),
      )

      expect(result.error).toBeInstanceOf(StreamNotFoundError)
      expect(result.created._tag).toBe("Created")
      expect(result.created.metadata.contentType).toBe("text/plain")
      expect(result.append._tag).toBe("Appended")
    })

    it("subscribeAll is live-only and emits tail advancement", async () => {
      const tail = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/all")
          yield* log.create({ path, contentType: "text/plain" })
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* log.subscribeAll()
              const fiber = yield* pipe(stream, Stream.take(1), Stream.runCollect, Effect.fork)
              yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("x"))
              const tails = yield* Fiber.join(fiber)
              return toArray(tails)[0]
            }),
          )
        }),
      )

      expect(tail?.closed).toBe(false)
      expect(tail?.path).toBe("contract/all")
    })

    it("close-only append advances the tail and is observable in read and head", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/close")
          yield* log.create({ path, contentType: "text/plain" })
          yield* appendEmpty(log, { path, contentType: "text/plain", close: true })
          const metadata = yield* log.head(path)
          const records = yield* readCollect(log, beginning(path))
          return { metadata, records }
        }),
      )

      expect(result.metadata.closed).toBe(true)
      expect(toArray(result.records)[0]?.closed).toBe(true)
    })

    if (supportsMissingHistoricalRead) {
      it("reads a missing stream as an empty historical stream", async () => {
        const records = await Effect.runPromise(
          Effect.gen(function* () {
            const log = yield* makeLog()
            const path = yield* decodeStreamPath("contract/missing")
            return yield* readCollect(log, beginning(path))
          }),
        )

        expect(toArray(records)).toEqual([])
      })
    }
  })
}
