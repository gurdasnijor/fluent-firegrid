import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-producer-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  const p = yield* S2Client.producer(streamName, {
    lingerDurationMillis: 5,
    maxBatchRecords: 100,
    maxInflightBytes: 1024 * 1024
  })
  yield* Stream.fromIterable([
    AppendRecord.string({ body: "batched" }),
    AppendRecord.string({ body: "durable" })
  ]).pipe(Stream.run(S2Client.sink(p)))
  yield* Console.log("producer records acknowledged")
}).pipe(Effect.scoped, Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
