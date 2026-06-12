/* eslint-disable effect/no-runPromise -- Reusable Vitest contract suite runs Effects at test boundaries. */
import { Effect, Fiber, Stream, pipe } from "effect"
import { describe, expect, it } from "vitest"
import { appendBytes, appendEmpty, beginning, readCollect } from "../operations.ts"
import { ContentTypeMismatchError, StreamNotFoundError } from "../errors.ts"
import { decodeStreamPath } from "../domainTypes.ts"
import type { DurableStreamLog } from "../durableStreamLog.ts"
import type { ChangeEvent } from "../streamTypes.ts"

const encoder = new TextEncoder()
const decoder = new TextDecoder()

const bytes = (text: string) => encoder.encode(text)
const text = (input: Uint8Array) => decoder.decode(input)

export interface DurableStreamLogTestOptions {
  readonly supportsMissingHistoricalRead?: boolean
}

export const runDurableStreamLogTestSuite = (
  name: string,
  makeLog: () => Effect.Effect<DurableStreamLog, never>,
  options: DurableStreamLogTestOptions = {},
) => {
  const { supportsMissingHistoricalRead = false } = options

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

      expect(records.map((record) => text(record.bytes))).toEqual(["a", "b"])
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

    it("changes emits historical replay, caught-up control, then live records", async () => {
      const events = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/changes")
          yield* log.create({ path, contentType: "text/plain" })
          yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("historical"))
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* log.changes(beginning(path))
              const fiber = yield* pipe(stream, Stream.take(3), Stream.runCollect, Effect.forkChild)
              yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("live"))
              return yield* Fiber.join(fiber)
            }),
          )
        }),
      )

      const output = events.map((event: ChangeEvent) =>
        event._tag === "Chunk" ? text(event.record.bytes) : event._tag,
      )
      expect(output).toEqual(["historical", "CaughtUp", "live"])
    })

    it("does not create missing streams through changes", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/missing-changes")
          const error = yield* Effect.scoped(log.changes(beginning(path))).pipe(Effect.flip)
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

    it("multiple changes subscribers receive the same live records", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/multiple-subscribers")
          yield* log.create({ path, contentType: "text/plain" })
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const left = yield* log.changes(beginning(path))
              const right = yield* log.changes(beginning(path))
              const leftFiber = yield* pipe(left, Stream.take(2), Stream.runCollect, Effect.forkChild)
              const rightFiber = yield* pipe(right, Stream.take(2), Stream.runCollect, Effect.forkChild)
              yield* appendBytes(log, { path, contentType: "text/plain" }, bytes("x"))
              const leftEvents = yield* Fiber.join(leftFiber)
              const rightEvents = yield* Fiber.join(rightFiber)
              return { leftEvents, rightEvents }
            }),
          )
        }),
      )

      expect(result.leftEvents.map((event: ChangeEvent) => event._tag)).toEqual(["CaughtUp", "Chunk"])
      expect(result.rightEvents.map((event: ChangeEvent) => event._tag)).toEqual(["CaughtUp", "Chunk"])
    })

    it("close-only append is observable through read, head, and changes EOF", async () => {
      const result = await Effect.runPromise(
        Effect.gen(function* () {
          const log = yield* makeLog()
          const path = yield* decodeStreamPath("contract/close")
          yield* log.create({ path, contentType: "text/plain" })
          yield* appendEmpty(log, { path, contentType: "text/plain", close: true })
          const metadata = yield* log.head(path)
          const window = yield* log.read(beginning(path))
          const events = yield* Effect.scoped(
            Effect.gen(function* () {
              const stream = yield* log.changes(beginning(path))
              return yield* pipe(stream, Stream.runCollect)
            }),
          )
          return { metadata, window, events }
        }),
      )

      expect(result.metadata.closed).toBe(true)
      expect(result.window.closed).toBe(true)
      expect(result.window.records).toEqual([])
      expect(result.events.map((event: ChangeEvent) => event._tag)).toEqual(["CaughtUp", "Closed"])
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

        expect(records).toEqual([])
      })
    }
  })
}
