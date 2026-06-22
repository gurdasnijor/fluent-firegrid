import { Effect } from "effect"
import { describe, expect, it } from "vitest"
import { DurableFailure } from "../src/index.ts"
import { Footgun, hasS2, Proxy, runIngress } from "./ingress-support.ts"

// The "footgun-free client split" claim, defended over the real ingress: the
// WRONG surface (top-level `client(...)` inside a handler) must be rejected, and
// the RIGHT surface (`objectClient(...)`) must keep working — and a rejected
// handler must surface as a TYPED `DurableFailure`, not a 500 defect.

describe.skipIf(!hasS2())("durable ingress — client-surface guard", () => {
  it("rejects the top-level client() surface used inside a handler (footgun → typed DurableFailure)", async () => {
    // `Effect.flip` turns the expected failure into the success channel; if the
    // call unexpectedly SUCCEEDED, flip would fail the test instead.
    const failure = await runIngress((ingress) => ingress.serviceClient(Footgun).callTopLevel(5).pipe(Effect.flip))
    expect(failure).toBeInstanceOf(DurableFailure)
    expect(failure.message).toContain("not replay-safe inside a handler")
  }, 60_000)

  it("allows the in-handler objectClient() surface — the guard does not misfire", async () => {
    const result = await runIngress((ingress) => ingress.serviceClient(Proxy).bump(5))
    expect(result).toBe(5)
  }, 60_000)
})
