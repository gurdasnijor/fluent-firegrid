import { execSync } from "node:child_process"
import { fileURLToPath } from "node:url"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import type { Envelope } from "@cucumber/messages"
import { Effect, Layer, Option, Schema, Stream } from "effect"
import { state } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { describe, expect, it } from "vitest"
import { defineSteps } from "../src/durable/support.ts"
import { runFeaturesDurable } from "../src/durable/runtime.ts"
import { S2LiteLive } from "../src/s2lite.ts"

// Proves the foundational capability for running firegrid-durable specs on the
// new runner: a step body is a *durable* Effect program (uses state(...)), and
// scenario state persists across the separate world `invoke` calls. If state did
// not persist, the final assertion step would FAIL — so a green run is the proof.

class CounterRow extends Table<CounterRow>("durable-step-counter")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

const valueOf = (row: Option.Option<CounterRow>): number =>
  Option.match(row, { onNone: () => 0, onSome: (r) => r.value })

const durableState = defineSteps(({ Given, Then, When }) => {
  Given("the counter starts at {int}", (n: number) =>
    Effect.gen(function*() {
      yield* state(CounterRow).set({ id: "v", value: n })
    }))
  When("I add {int}", (n: number) =>
    Effect.gen(function*() {
      const current = valueOf(yield* state(CounterRow).get("v"))
      yield* state(CounterRow).set({ id: "v", value: current + n })
    }))
  Then("the counter is {int}", (n: number) =>
    Effect.gen(function*() {
      const current = valueOf(yield* state(CounterRow).get("v"))
      if (current !== n) {
        return yield* Effect.fail(new Error(`counter was ${current}, expected ${n}`))
      }
    }))
})

const featurePath = fileURLToPath(new URL("./fixtures/durable-state.feature", import.meta.url))

const hasS2 = (): boolean => {
  try {
    execSync("command -v s2", { stdio: "ignore" })
    return true
  } catch {
    return false
  }
}

const run = (): Promise<ReadonlyArray<Envelope>> =>
  runFeaturesDurable([featurePath], { runId: `durable-state-${Date.now()}`, support: durableState }).pipe(
    Stream.runCollect,
    Effect.map((chunk) => Array.from(chunk) as ReadonlyArray<Envelope>),
    Effect.provide(Layer.mergeAll(S2LiteLive, NodeFileSystem.layer)),
    Effect.scoped,
    Effect.runPromise,
  )

describe.skipIf(!hasS2())("durable step bodies (S2-backed)", () => {
  it("state(...) in a step body persists across steps and the run passes", async () => {
    const envelopes = await run()
    const statuses = envelopes.flatMap((e) => (e.testStepFinished ? [e.testStepFinished.testStepResult.status] : []))
    const success = envelopes.find((e) => e.testRunFinished)?.testRunFinished?.success
    expect(statuses).toEqual(["PASSED", "PASSED", "PASSED"])
    expect(success).toBe(true)
  })
})
