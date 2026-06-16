import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import { Console, Effect, Schema, Stream } from "effect"
import { S2Client, publish, readDecoded } from "../src/index.ts"

class Order extends Schema.Class<Order>("Order")({
  id: Schema.String,
  total: Schema.Number,
}) {}

const streamName = `effect-s2-orders-${Date.now()}`

const program = Effect.gen(function*() {
  yield* S2Client.createStream({ stream: streamName })
  yield* publish(streamName, Order, Order.make({ id: "o-1", total: 42 }))
  const orders = yield* readDecoded(streamName, Order, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 1 } },
  }).pipe(Stream.runCollect)
  yield* Console.log(orders)
}).pipe(Effect.provide(S2Client.layerConfig))

NodeRuntime.runMain(program)
