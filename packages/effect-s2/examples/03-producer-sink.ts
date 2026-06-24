import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import { AppendRecord, basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-producer-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  const producer = yield* stream.producer({ maxInflightBytes: 1024 * 1024 })

  const first = yield* producer.submit(AppendRecord.string({ body: "batched" }))
  const second = yield* producer.submit(AppendRecord.string({ body: "durable" }))
  yield* first.ack
  yield* second.ack

  yield* Console.log("producer records acknowledged")
}).pipe(Effect.scoped, Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
