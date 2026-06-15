import { NodeRuntime } from "@effect/platform-node"
import { Console, Effect } from "effect"
import { AppendInput, AppendRecord, S2Client } from "../src/index.ts"

const streamName = `effect-s2-session-cas-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  const tail = yield* S2Client.checkTail(streamName)
  const session = yield* S2Client.appendSession(streamName)

  const ack = yield* session.submit(AppendInput.create(
    [AppendRecord.string({ body: "exactly-once" })],
    { matchSeqNum: tail.tail.seqNum },
  ))

  yield* Console.log(`committed seqNum range ${ack.start.seqNum}-${ack.end.seqNum}`)
}).pipe(Effect.scoped, Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
