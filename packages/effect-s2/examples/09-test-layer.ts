import { Console, Effect, Stream } from "effect"
import { AppendRecord, S2Client } from "../src/index.ts"
import * as TestS2 from "../src/TestS2.ts"

const program = Effect.gen(function*() {
  yield* S2Client.createStream("offline")
  yield* S2Client.append("offline", [
    AppendRecord.string({ body: "runs without S2 credentials" }),
  ])

  const records = yield* S2Client.read("offline", {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 1 } },
  }).pipe(Stream.runCollect)

  yield* Console.log(records.map((record) => record.body))
}).pipe(Effect.provide(TestS2.layer), Effect.runPromise)

await program
