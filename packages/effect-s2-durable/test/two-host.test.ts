import { describe, expect, it } from "@effect/vitest"
import { Context, Effect, Layer, Schema } from "effect"
import { encodeObjectCallId } from "../src/object/machine/index.ts"
import { DurableEngine } from "../src/engine/api.ts"
import { serviceLayer } from "../src/service.ts"
import { Counter, hasS2 } from "./ingress-support.ts"
import { S2LiteLive } from "./s2lite.ts"

// Two independent engines (distinct ObjectOwnerDriver → distinct host tokens) over
// ONE shared `s2 lite` basin = two hosts on the same durable state. Proves the
// S2-native fenced-ownership drive (host SDD §7 / build-step-4): cross-host single
// writer + fence handoff, with NO in-process coordination shared between them.

const engineLayer = serviceLayer(Counter)

const addHandler = Counter.compiled.add!.handler
const callId = (key: string, nonce: string) =>
  encodeObjectCallId({ object: Counter.name, key, method: "add", nonce })

/** Build two engines sharing one s2-lite S2Client, run `program(e1, e2)`. */
const runTwoHosts = <A, E>(
  program: (
    e1: typeof DurableEngine.Service,
    e2: typeof DurableEngine.Service,
  ) => Effect.Effect<A, E>,
): Promise<A> =>
  Effect.gen(function*() {
    const ctx1 = yield* Layer.build(engineLayer)
    const ctx2 = yield* Layer.build(engineLayer)
    return yield* program(Context.get(ctx1, DurableEngine), Context.get(ctx2, DurableEngine))
  }).pipe(Effect.scoped, Effect.provide(S2LiteLive), Effect.runPromise)

describe.skipIf(!hasS2())("two hosts over shared S2 (fenced ownership)", () => {
  it("a peer host takes over an owner stream and reads state the first host wrote", async () => {
    // sequential cross-host accumulation on one key: host 1 writes, host 2 claims
    // the same owner stream and reads host 1's durable state (5 → 8). If the fence
    // handoff or cross-host state continuity were broken, host 2 would see 0 → 3.
    const result = await runTwoHosts((e1, e2) =>
      Effect.gen(function*() {
        const id1 = yield* callId("acc", "n1")
        yield* e1.submit(addHandler, id1, 5)
        const first = yield* e1.attach(id1, Schema.Number)

        const id2 = yield* callId("acc", "n2")
        yield* e2.submit(addHandler, id2, 3)
        const second = yield* e2.attach(id2, Schema.Number)
        return { first, second }
      }),
    )
    expect(result).toEqual({ first: 5, second: 8 })
  }, 60_000)

  it("both hosts driving the SAME admitted call settle to one consistent result", async () => {
    // concurrent contention on one call id: both hosts admit (idempotent) + drive.
    // The fence ensures only one host's appends are durable; both attaches observe
    // the single Completed (no double-applied / corrupted fold), and neither errors.
    const result = await runTwoHosts((e1, e2) =>
      Effect.gen(function*() {
        const id = yield* callId("race", "n1")
        yield* Effect.all([e1.submit(addHandler, id, 5), e2.submit(addHandler, id, 5)], {
          concurrency: "unbounded",
        })
        const [r1, r2] = yield* Effect.all([
          e1.attach(id, Schema.Number),
          e2.attach(id, Schema.Number),
        ], { concurrency: "unbounded" })
        return { r1, r2 }
      }),
    )
    expect(result).toEqual({ r1: 5, r2: 5 })
  }, 60_000)
})
