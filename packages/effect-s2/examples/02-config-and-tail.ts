import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect } from "effect"
import { S2Client } from "../src/index.ts"

const streamName = `effect-s2-tail-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  const tail = yield* S2Client.checkTail(streamName)
  yield* Console.log(`tail seqNum: ${tail.tail.seqNum}`)
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
