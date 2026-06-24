import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import * as Stream from "effect/Stream"
import { AppendInput, AppendRecord, basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-live-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  const reader = yield* stream.readSession({
    start: { from: { tailOffset: 0 } }
  }).pipe(
    Stream.take(2),
    Stream.runCollect,
    Effect.forkChild
  )

  yield* stream.append(AppendInput.create([AppendRecord.string({ body: "x" })]))
  yield* stream.append(AppendInput.create([AppendRecord.string({ body: "y" })]))

  const records = yield* Fiber.join(reader)
  yield* Console.log(records.map((record) => record.body))
}).pipe(Effect.scoped, Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
