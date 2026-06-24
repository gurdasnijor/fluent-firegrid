import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Console from "effect/Console"
import * as Effect from "effect/Effect"
import * as Stream from "effect/Stream"
import { basin, layer, stream as s2Stream } from "../src/index.ts"

class Order {
  constructor(
    readonly id: string,
    readonly total: number
  ) {}
}

const basinName = "my-basin"
const streamName = `effect-s2-orders-${Date.now()}`
const encoder = new TextEncoder()
const decoder = new TextDecoder()

const encodeOrder = (order: Order) => encoder.encode(JSON.stringify(order))
const decodeOrder = (bytes: Uint8Array) => {
  const json = JSON.parse(decoder.decode(bytes)) as { readonly id: string; readonly total: number }
  return new Order(json.id, json.total)
}

const program = Effect.gen(function*() {
  const basinApi = yield* basin(basinName)
  yield* basinApi.streams.create({ stream: streamName })

  const stream = yield* s2Stream(basinName, streamName)
  const writer = yield* stream.serialization.appendSession(encodeOrder)
  yield* writer.submit(new Order("o-1", 42))

  const orders = yield* stream.serialization.readSession(decodeOrder, {
    start: { from: { seqNum: 0 } },
    stop: { limits: { count: 1 } }
  }).pipe(Stream.runCollect)

  yield* Console.log(orders.map((order) => [order.id, order.total]))
}).pipe(Effect.scoped, Effect.provide(layer({ accessToken: "s2_access_token" })))

NodeRuntime.runMain(program)
