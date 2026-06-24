import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import { basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-tail-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  const tail = yield* stream.checkTail()
  yield* Console.log(`tail seqNum: ${tail.tail.seqNum}`)
}).pipe(Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
