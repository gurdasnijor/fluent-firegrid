import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Schema from "effect/Schema"
import * as Stream from "effect/Stream"
import { publish, readDecoded, S2Client } from "../src/index.ts"

class Order extends Schema.Class<Order>("Order")({
  id: Schema.String,
  total: Schema.Number
}) {}

const streamName = `effect-s2-orders-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  yield* publish(streamName, Order, Order.make({ id: "o-1", total: 42 }))
  const orders = yield* readDecoded(streamName, Order, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 1 } }
  }).pipe(Stream.runCollect)
  yield* Console.log(orders.map((record) => [record.seqNum, record.value]))
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
