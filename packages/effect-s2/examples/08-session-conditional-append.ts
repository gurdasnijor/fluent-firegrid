import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import { AppendInput, AppendRecord, basin, layer, stream as s2Stream } from "../src/index.ts"

const basinName = "my-basin"
const streamName = `effect-s2-session-cas-${Date.now()}`

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  const tail = yield* stream.checkTail()
  const session = yield* stream.appendSession()

  const ticket = yield* session.submit(AppendInput.create(
    [AppendRecord.string({ body: "exactly-once" })],
    { matchSeqNum: tail.tail.seqNum }
  ))
  const ack = yield* ticket.ack

  yield* Console.log(`committed seqNum range ${ack.start.seqNum}-${ack.end.seqNum}`)
}).pipe(Effect.scoped, Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
