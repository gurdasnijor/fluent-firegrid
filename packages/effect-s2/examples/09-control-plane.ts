import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Console, Effect } from "effect"
import { S2Client } from "../src/index.ts"

const streamName = `effect-s2-control-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({
    stream: streamName,
    config: {
    retentionPolicy: { infinite: {} },
    },
  })
  const config = yield* S2Client.getStreamConfig({ stream: streamName })
  const streams = yield* S2Client.listStreams({ prefix: "effect-s2-control-" })
  yield* S2Client.deleteStream({ stream: streamName })

  yield* Console.log({
    storageClass: config.storageClass,
    streamNames: streams.streams.map((stream) => stream.name),
  })
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
