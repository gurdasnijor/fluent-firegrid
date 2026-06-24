import { type AppendInput, type AppendOptions, S2Client, type S2ClientApi, S2Error, type StreamApi } from "effect-s2"
import * as DateTime from "effect/DateTime"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { describe, expect, it } from "vitest"

import { EventStream, FlowStreamCodec, S2Stream } from "../src/index.ts"

const Todo = Schema.fromJsonString(Schema.Struct({
  id: Schema.String,
  text: Schema.String
}))

const Todos = EventStream.make("todos", {
  key: Schema.String,
  value: Todo
})

const epoch = DateTime.toDateUtc(DateTime.makeUnsafe("1970-01-01T00:00:00Z"))
const invalidTodoBody = `{"id":"1"}`
const guardedTodoBody = `{"id":"1","text":"guarded"}`

const appendOnlyStream = (capture: {
  input?: AppendInput
  options?: AppendOptions
}): StreamApi =>
  ({
    raw: {},
    name: "todos/k-1",
    append: (input: AppendInput) =>
      Effect.sync(() => {
        capture.input = input
        capture.options = {
          ...(input.matchSeqNum === undefined ? {} : { matchSeqNum: input.matchSeqNum }),
          ...(input.fencingToken === undefined ? {} : { fencingToken: input.fencingToken })
        }
        return {
          start: { seqNum: 0, timestamp: epoch },
          end: { seqNum: 1, timestamp: epoch },
          tail: { seqNum: 1, timestamp: epoch }
        }
      }),
    readSession: () => Stream.never
  }) as unknown as StreamApi

const streamClient = (
  capture: {
    basin?: string
    stream?: string
    input?: AppendInput
    options?: AppendOptions
  },
  stream: StreamApi = appendOnlyStream(capture)
) =>
  ({
    stream: (basin: string, streamName: string) =>
      Effect.sync(() => {
        capture.basin = basin
        capture.stream = streamName
        return stream
      })
  }) as unknown as S2ClientApi

describe("EventStream", () => {
  it("derives physical stream names from encoded keys", () =>
    Effect.gen(function*() {
      const key = yield* FlowStreamCodec.encodeKey(Todos, "k-1")
      expect(EventStream.streamName(Todos, key)).toBe("todos/k-1")
    }).pipe(Effect.runPromise))

  it("encodes and decodes string S2 records through Effect Schema", () =>
    Effect.gen(function*() {
      const appendRecord = yield* FlowStreamCodec.appendRecord(Todos, { id: "1", text: "ship" })
      const event = yield* FlowStreamCodec.decodeRecord(Todos, "k-1", {
        seqNum: 7,
        timestamp: epoch,
        body: appendRecord.body,
        headers: [["content-type", "application/json"]]
      })

      expect(event).toEqual({
        stream: "todos/k-1",
        key: "k-1",
        value: { id: "1", text: "ship" },
        cursor: { stream: "todos/k-1", seqNum: 7 },
        headers: new Map([["content-type", "application/json"]])
      })
    }).pipe(Effect.runPromise))

  it("surfaces schema failures as typed decode errors", () =>
    Effect.gen(function*() {
      const reason = yield* FlowStreamCodec.decodeRecord(Todos, "k-1", {
        seqNum: 0,
        timestamp: epoch,
        body: invalidTodoBody,
        headers: []
      }).pipe(
        Effect.match({
          onFailure: (error) => error.reason,
          onSuccess: () => "success"
        })
      )

      expect(reason).toBe("decode")
    }).pipe(Effect.runPromise))

  it("opens the encoded physical stream and passes guarded append options through", () => {
    const capture: {
      basin?: string
      stream?: string
      input?: AppendInput
      options?: AppendOptions
    } = {}

    return Effect.gen(function*() {
      yield* S2Stream.append("basin-a", Todos, "k-1", { id: "1", text: "guarded" }, {
        matchSeqNum: 42,
        fencingToken: "token-a"
      })

      expect(capture.basin).toBe("basin-a")
      expect(capture.stream).toBe("todos/k-1")
      expect(capture.options).toEqual({ matchSeqNum: 42, fencingToken: "token-a" })
      expect(capture.input?.records).toHaveLength(1)
      expect(capture.input?.records[0]?.body).toBe(guardedTodoBody)
    }).pipe(Effect.provideService(S2Client, streamClient(capture)), Effect.runPromise)
  })

  it("normalizes missing and empty reads to an empty session", () =>
    Effect.gen(function*() {
      const emptyStream = {
        raw: {},
        name: "todos/k-1",
        readSession: () =>
          Stream.fail(new S2Error({ message: "range not satisfiable", status: 416, origin: "server" })),
        append: () => Effect.die("not used")
      } as unknown as StreamApi
      const records = yield* S2Stream.readSessionFromStream(emptyStream, Todos, "k-1").pipe(Stream.runCollect)

      expect([...records]).toEqual([])
    }).pipe(Effect.runPromise))
})
