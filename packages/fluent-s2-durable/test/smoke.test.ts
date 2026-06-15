import { Duration, Effect, Fiber, Layer } from "effect"
import { expect, layer } from "@effect/vitest"
import { DispatchLayer, TimerHeapLayer, makeWorker, type WorkerConfig } from "../src/index.ts"
import { S2LiteLive } from "./s2lite.ts"
import {
  makeChargeBook,
  makeOrderHandler,
  type OrderInput,
  type Receipt,
} from "./demo.ts"

const infra = Layer.provideMerge(TimerHeapLayer, DispatchLayer)

const config = (
  book: ReturnType<typeof makeChargeBook>,
): WorkerConfig<OrderInput, Receipt, never> => ({
  handler: makeOrderHandler(book, Duration.millis(30)),
  handlerLayer: Layer.empty,
})

// excludeTestServices: durable timers + s2-lite I/O need the *real* clock, not
// @effect/vitest's frozen TestClock.
layer(S2LiteLive, { excludeTestServices: true, timeout: Duration.seconds(30) })(
  "smoke — real s2-lite",
  (it) => {
  it.effect("drives the demo order workflow to completion", () =>
    Effect.gen(function* () {
      const book = makeChargeBook()
      const worker = yield* makeWorker(config(book)).pipe(Effect.provide(infra))
      const fiber = yield* Effect.forkChild(worker.runLoop)
      yield* worker.start("ord-1", { orderId: "ord-1", amount: 100 })
      yield* worker.resolveEvent("ord-1", "approval", true)
      const receipt = yield* worker.awaitResult("ord-1")
      yield* Fiber.interrupt(fiber)
      expect(receipt.status).toBe("fulfilled")
      expect(book.charged).toBe(1)
    }))
  },
)
