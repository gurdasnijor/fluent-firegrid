import { describe, expect, it } from "@effect/vitest"
import { Effect, Schema } from "effect"
import { decodeEventRecord, encodeEventRecord, EventStream } from "../src/index.ts"

// A schema with a brand + a transform-ish field, to prove the codec boundary
// (decoded value !== encoded storage shape only at the JSON level here).
const EventId = Schema.String.pipe(Schema.brand("EventId"))
const TestEvent = Schema.Struct({
  id: EventId,
  count: Schema.Number,
})
type TestEvent = typeof TestEvent.Type

const StreamName = "test/events"

describe("EventStream declaration", () => {
  class Events extends EventStream<Events>("test/events")(TestEvent) {}

  it("derives its base path and carries the value + key schemas", () => {
    expect(Events.basePath).toBe("test/events")
    expect(Events.value).toBe(TestEvent)
    // default key schema is Schema.String
    expect(Schema.encodeUnknownSync(Events.key)("run-1")).toBe("run-1")
  })

  it("derives the path segment through a branded key schema", () => {
    const RunId = Schema.String.pipe(Schema.brand("RunId"))
    class Keyed extends EventStream<Keyed>("test/keyed")(TestEvent, RunId) {}
    expect(Schema.encodeUnknownSync(Keyed.key)(RunId.make("abc"))).toBe("abc")
  })
})

describe("EventStream record codec (the pure core of append/read)", () => {
  const value: TestEvent = { id: EventId.make("a"), count: 7 }

  it.effect("encodes then decodes a typed value round-trip, carrying the seq cursor", () =>
    Effect.gen(function*() {
      const record = yield* encodeEventRecord(TestEvent, StreamName, value)
      const decoded = yield* decodeEventRecord(TestEvent, StreamName, { seqNum: 42, timestamp: 0, body: record.body })
      expect(decoded).toEqual({ seqNum: 42, timestamp: 0, value })
    }))

  it.effect("a malformed body fails with a typed decode error naming the stream and seq", () =>
    Effect.gen(function*() {
      const error = yield* decodeEventRecord(TestEvent, StreamName, { seqNum: 99, timestamp: 0, body: "{not json" }).pipe(
        Effect.flip,
      )
      expect(error._tag).toBe("S2StreamDbError")
      expect(error.operation).toBe("EventStream.read")
      expect(error.message).toContain(`${StreamName}#99`)
    }))

  it.effect("a well-formed body that violates the schema also fails with seq context", () =>
    Effect.gen(function*() {
      // valid JSON, wrong shape (count missing) → schema decode failure
      const error = yield* decodeEventRecord(TestEvent, StreamName, { seqNum: 3, timestamp: 0, body: JSON.stringify({ id: "a" }) }).pipe(
        Effect.flip,
      )
      expect(error._tag).toBe("S2StreamDbError")
      expect(error.message).toContain(`${StreamName}#3`)
    }))
})
