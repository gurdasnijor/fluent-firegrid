import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect, Stream } from "effect"
import { AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-session-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream(streamName)
  const session = yield* S2Client.appendSession(streamName, {
    maxInflightBytes: 1024 * 1024,
    maxInflightBatches: 2,
  })

  const first = yield* session.submit([
    AppendRecord.string({ body: "session-a" }),
    AppendRecord.string({ body: "session-b" }),
  ])
  const second = yield* session.submit([
    AppendRecord.string({ body: "session-c" }),
  ])

  const records = yield* S2Client.read(streamName, {
    start: { from: { seqNum: first.start.seqNum } },
    stop: { limits: { count: second.end.seqNum - first.start.seqNum } },
  }).pipe(Stream.runCollect)

  yield* Console.log({
    first: [first.start.seqNum, first.end.seqNum],
    second: [second.start.seqNum, second.end.seqNum],
    bodies: records.map((record) => record.body),
  })
}).pipe(Effect.scoped, Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
