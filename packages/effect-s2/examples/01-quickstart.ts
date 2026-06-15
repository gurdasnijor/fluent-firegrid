import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect, Stream } from "effect"
import { AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-quickstart-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream(streamName)
  yield* S2Client.append(streamName, [
    AppendRecord.string({ body: "hello" }),
    AppendRecord.string({ body: "s2" }),
  ])
  const records = yield* S2Client.read(streamName, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 2 } },
  }).pipe(Stream.runCollect)
  yield* Console.log(records.map((record) => record.body))
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
