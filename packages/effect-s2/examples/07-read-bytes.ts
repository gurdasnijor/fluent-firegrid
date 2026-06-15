import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect, Stream } from "effect"
import { AppendInput, AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-bytes-${Date.now()}`
const decoder = new TextDecoder()

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  yield* S2Client.append(streamName, AppendInput.create([
    AppendRecord.string({
      body: "utf8 text",
      headers: [["content-type", "text/plain"]],
    }),
    AppendRecord.bytes({
      body: new Uint8Array([0, 1, 2, 255]),
      headers: [[new Uint8Array([1, 2]), new Uint8Array([3, 4])]],
    }),
  ]))

  const records = yield* S2Client.readBytes(streamName, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 2 } },
  }).pipe(Stream.runCollect)

  yield* Console.log({
    textRecord: decoder.decode(records[0]?.body),
    byteRecord: Array.from(records[1]?.body ?? new Uint8Array()),
  })
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
