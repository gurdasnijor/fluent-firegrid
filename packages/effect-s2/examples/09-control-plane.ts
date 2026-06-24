import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import { basin, layer } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-control-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({
    stream: streamName,
    config: {
      retentionPolicy: { infinite: {} }
    }
  })

  const config = yield* basinApi.streams.getConfig({ stream: streamName })
  const streams = yield* basinApi.streams.list({ prefix: "effect-s2-control-" })
  yield* basinApi.streams.delete({ stream: streamName })

  yield* Console.log({
    storageClass: config.storageClass,
    streamNames: streams.streams.map((stream) => stream.name)
  })
}).pipe(Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
