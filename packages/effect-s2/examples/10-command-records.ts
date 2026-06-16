import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Console, Effect } from "effect"
import { AppendInput, AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-commands-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  yield* S2Client.append(streamName, AppendInput.create([AppendRecord.fence("writer-a")]))
  yield* S2Client.append(
    streamName,
    AppendInput.create(
      [AppendRecord.string({ body: "guarded write" })],
      { fencingToken: "writer-a" },
    ),
  )
  yield* S2Client.append(streamName, AppendInput.create([AppendRecord.trim(1)]))

  const batch = yield* S2Client.readBatch(streamName, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 10 } },
    ignoreCommandRecords: true,
  })

  yield* S2Client.deleteStream({ stream: streamName })
  yield* Console.log(batch.records.map((record) => record.body))
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
