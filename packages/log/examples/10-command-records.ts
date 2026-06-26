import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import { AppendInput, AppendRecord, basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-commands-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  yield* stream.append(AppendInput.create([AppendRecord.fence("writer-a")]))
  yield* stream.append(AppendInput.create(
    [AppendRecord.string({ body: "guarded write" })],
    { fencingToken: "writer-a" }
  ))
  yield* stream.append(AppendInput.create([AppendRecord.trim(1)]))

  const batch = yield* stream.read({
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 10 } },
    ignoreCommandRecords: true
  })

  yield* basinApi.streams.delete({ stream: streamName })
  yield* Console.log(batch.records.map((record) => record.body))
}).pipe(Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
