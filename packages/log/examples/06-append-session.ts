import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { AppendInput, AppendRecord, basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-session-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  const session = yield* stream.appendSession({
    maxInflightBytes: 1024 * 1024,
    maxInflightBatches: 2
  })

  const firstTicket = yield* session.submit(AppendInput.create([
    AppendRecord.string({ body: "session-a" }),
    AppendRecord.string({ body: "session-b" })
  ]))
  const secondTicket = yield* session.submit(AppendInput.create([
    AppendRecord.string({ body: "session-c" })
  ]))

  const first = yield* firstTicket.ack
  const second = yield* secondTicket.ack

  const records = yield* stream.readSession({
    start: { from: { seqNum: first.start.seqNum } },
    stop: { limits: { count: second.end.seqNum - first.start.seqNum } }
  }).pipe(Stream.runCollect)

  yield* Console.log({
    first: [first.start.seqNum, first.end.seqNum],
    second: [second.start.seqNum, second.end.seqNum],
    bodies: records.map((record) => record.body)
  })
}).pipe(Effect.scoped, Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
