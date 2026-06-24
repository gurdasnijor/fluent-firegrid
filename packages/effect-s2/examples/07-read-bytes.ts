import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { AppendInput, AppendRecord, basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-bytes-${Date.now()}`
const decoder = new TextDecoder()

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  yield* stream.append(AppendInput.create([
    AppendRecord.string({
      body: "utf8 text",
      headers: [["content-type", "text/plain"]]
    }),
    AppendRecord.bytes({
      body: new Uint8Array([0, 1, 2, 255]),
      headers: [[new Uint8Array([1, 2]), new Uint8Array([3, 4])]]
    })
  ]))

  const records = yield* stream.readSession({
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 2 } }
  }, { as: "bytes" }).pipe(Stream.runCollect)

  yield* Console.log({
    textRecord: decoder.decode(records[0]?.body),
    byteRecord: Array.from(records[1]?.body ?? new Uint8Array())
  })
}).pipe(Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
