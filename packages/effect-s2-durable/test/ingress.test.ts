import * as Effect from "effect/Effect"
import * as Option from "effect/Option"
import { describe, expect, it } from "vitest"
import { Calculator, Counter, hasS2, runIngress } from "./ingress-support.ts"

// Proves the durable HTTP ingress end to end: a real Node HTTP server serves the
// durable definitions; an out-of-process-style client connects over HTTP and
// drives a service AND a keyed object, each invocation journaled to (s2 lite) S2.
// The edge provide-stack lives once in `runIngress` (see ingress-support.ts).

describe.skipIf(!hasS2())("durable ingress over HTTP (S2 + node http)", () => {
  it("connect({ url }) drives a service and a keyed object over real HTTP", async () => {
    const result = await runIngress((ingress) =>
      Effect.gen(function*() {
        // request-response over HTTP, for a service and a keyed object
        const doubled = yield* ingress.serviceClient(Calculator).double(21)
        const added = yield* ingress.objectClient(Counter, "cart").add(5)
        // fire-and-forget: send returns an awaitable handle (Restate's Send → attach)
        const handle = yield* ingress.serviceSendClient(Calculator).double(10)
        const attached = yield* handle.attach
        // non-blocking output: once attached, the handle's output is ready
        const polled = yield* handle.output
        return {
          doubled,
          added,
          invocationIdPresent: handle.invocationId.length > 0,
          attached,
          polled: Option.getOrNull(polled)
        }
      })
    )

    expect(result).toEqual({ doubled: 42, added: 5, invocationIdPresent: true, attached: 20, polled: 20 })
  }, 60_000)

  it("attaches and polls an existing invocation by idempotency key (no original handle)", async () => {
    const result = await runIngress((ingress) =>
      Effect.gen(function*() {
        // a "first caller" sends a keyed-object invocation pinned to an idempotency key
        const idempotencyKey = "ingress-idem-1"
        yield* ingress.objectSendClient(Counter, "wishlist").add(7, { idempotencyKey })
        // a SECOND caller — holding only (def, key, method, idempotencyKey), not the
        // server-minted id — re-attaches to the same invocation and reads its result
        const attached = yield* ingress.objectAttachClient(Counter, "wishlist").add({ idempotencyKey })
        const polled = yield* ingress.objectOutputClient(Counter, "wishlist").add({ idempotencyKey })
        return { attached, polled: Option.getOrNull(polled) }
      })
    )

    expect(result).toEqual({ attached: 7, polled: 7 })
  }, 60_000)

  it("attaches/polls a SERVICE invocation by idempotency key, and reports not-ready as None", async () => {
    const result = await runIngress((ingress) =>
      Effect.gen(function*() {
        const idempotencyKey = "ingress-svc-idem-1"
        // first caller sends a stateless-service invocation pinned to an idempotency key
        yield* ingress.serviceSendClient(Calculator).double(9, { idempotencyKey })
        // second caller re-attaches by key over the SERVICE route (no :key segment)
        const attached = yield* ingress.serviceAttachClient(Calculator).double({ idempotencyKey })
        // and the non-blocking output is ready once the invocation has completed
        const ready = yield* ingress.serviceOutputClient(Calculator).double({ idempotencyKey })
        // an invocation that was never sent reads back as not-ready (Option.none / wire "notReady")
        const unknown = yield* ingress.serviceOutputClient(Calculator).double({ idempotencyKey: "ingress-never-sent" })
        return { attached, ready: Option.getOrNull(ready), unknown: Option.getOrNull(unknown) }
      })
    )

    expect(result).toEqual({ attached: 18, ready: 18, unknown: null })
  }, 60_000)
})
