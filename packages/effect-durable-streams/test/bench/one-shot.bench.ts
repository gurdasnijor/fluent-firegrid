import { DurableStream as RefDurableStream } from "@durable-streams/client"
import { Effect, Schema } from "effect"
import { afterAll, beforeAll, bench, describe } from "vitest"
import { DurableStream } from "../../src/index.ts"
import {
  makeEffectRuntime,
  runScoped,
  startBenchServer,
  type EffectRuntime,
} from "./harness.ts"

let server: Awaited<ReturnType<typeof startBenchServer>>
let runtime: EffectRuntime
let url: string

const Msg = Schema.Struct({ payload: Schema.String })

beforeAll(async () => {
  server = await startBenchServer()
  runtime = makeEffectRuntime()
  url = server.streamUrl("one-shot")
  const setup = new RefDurableStream({ url })
  await setup.create({ contentType: "application/json" })
}, 30000)

afterAll(async () => {
  await runtime.dispose()
  await server.stop()
})

describe("one-shot append - no producer batching", () => {
  bench("reference @durable-streams/client", async () => {
    const fresh = server.streamUrl("oneshot-ref")
    const ref = new RefDurableStream({ url: fresh })
    await ref.create({ contentType: "application/json" })
    await ref.append(JSON.stringify({ payload: "hello" }))
  })

  bench("effect-durable-streams", async () => {
    const fresh = server.streamUrl("oneshot-eff")
    await runScoped(
      runtime,
      Effect.gen(function* () {
        const s = DurableStream.define({ endpoint: { url: fresh }, schema: Msg })
        yield* s.create({ contentType: "application/json" })
        yield* s.append({ payload: "hello" })
      }),
    )
  })
})
