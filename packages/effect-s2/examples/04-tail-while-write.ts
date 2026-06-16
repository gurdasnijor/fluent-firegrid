import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Console, Effect, Fiber, Stream } from "effect"
import { AppendInput, AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-live-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  const reader = yield* S2Client.read(streamName, {
    start: { from: { tailOffset: 0 } },
  }).pipe(
    Stream.take(2),
    Stream.runCollect,
    Effect.forkChild,
  )
  yield* S2Client.append(streamName, AppendInput.create([AppendRecord.string({ body: "x" })]))
  yield* S2Client.append(streamName, AppendInput.create([AppendRecord.string({ body: "y" })]))
  const records = yield* Fiber.join(reader)
  yield* Console.log(records.map((record) => record.body))
}).pipe(Effect.scoped, Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
