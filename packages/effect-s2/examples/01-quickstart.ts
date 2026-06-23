import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { AppendInput, AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-quickstart-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  yield* S2Client.append(
    streamName,
    AppendInput.create([
      AppendRecord.string({ body: "hello" }),
      AppendRecord.string({ body: "s2" })
    ])
  )
  const records = yield* S2Client.read(streamName, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 2 } }
  }).pipe(Stream.runCollect)
  yield* Console.log(records.map((record) => record.body))
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
