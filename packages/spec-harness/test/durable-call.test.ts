import { execSync } from "node:child_process"
import { fileURLToPath } from "node:url"
import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import type { Envelope } from "@cucumber/messages"
import { Effect, Layer, Option, Schema, Stream } from "effect"
import { object, objectClient, state } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { describe, expect, it } from "vitest"
import { runFeaturesDurable } from "../src/durable/runtime.ts"
import { defineSteps } from "../src/durable/support.ts"
import { S2LiteLive } from "../src/s2lite.ts"

// Proves the next firegrid-durable requirement: a step body drives a *product*
// durable object (registered via durableDefs) through objectClient and reads its
// result — the world(object) -> Counter(object) child-call pattern the
// durable-executions specs rely on.

class CounterState extends Table<CounterState>("probe-counter-state")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

class ResultRow extends Table<ResultRow>("probe-call-result")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

const valueOf = (row: Option.Option<{ readonly value: number }>): number =>
  Option.match(row, { onNone: () => 0, onSome: (r) => r.value })

const Counter = object({
  name: "probe/counter",
  handlers: {
    *set(value: number) {
      yield* state(CounterState).set({ id: "v", value })
      return value
    },
    *add(amount: number) {
      const total = valueOf(yield* state(CounterState).get("v")) + amount
      yield* state(CounterState).set({ id: "v", value: total })
      return total
    },
  },
  schemas: {
    set: { input: Schema.Number, output: Schema.Number },
    add: { input: Schema.Number, output: Schema.Number },
  },
})

const COUNTER_KEY = "probe"

const objCall = defineSteps(({ Given, Then, When }) => {
  Given("a fresh durable counter", () => objectClient(Counter, COUNTER_KEY).set(0))
  When("the step adds {int} via the durable counter", (n: number) =>
    Effect.gen(function*() {
      const total = yield* objectClient(Counter, COUNTER_KEY).add(n)
      yield* state(ResultRow).set({ id: "r", value: total })
    }))
  Then("the durable counter total is {int}", (n: number) =>
    Effect.gen(function*() {
      const total = valueOf(yield* state(ResultRow).get("r"))
      if (total !== n) return yield* Effect.fail(new Error(`durable counter total was ${total}, expected ${n}`))
    }))
})

const featurePath = fileURLToPath(new URL("./fixtures/durable-call.feature", import.meta.url))

const hasS2 = (): boolean => {
  try {
    execSync("command -v s2", { stdio: "ignore" })
    return true
  } catch {
    return false
  }
}

const run = (): Promise<ReadonlyArray<Envelope>> =>
  runFeaturesDurable([featurePath], {
    runId: `durable-call-${Date.now()}`,
    support: objCall,
    durableDefs: [Counter],
  }).pipe(
    Stream.runCollect,
    Effect.map((chunk) => Array.from(chunk) as ReadonlyArray<Envelope>),
    Effect.provide(Layer.mergeAll(S2LiteLive, NodeFileSystem.layer)),
    Effect.scoped,
    Effect.runPromise,
  )

describe.skipIf(!hasS2())("durable object call from a step (S2-backed)", () => {
  it("a step drives a product durable object via objectClient and the run passes", async () => {
    const envelopes = await run()
    const statuses = envelopes.flatMap((e) => (e.testStepFinished ? [e.testStepFinished.testStepResult.status] : []))
    const success = envelopes.find((e) => e.testRunFinished)?.testRunFinished?.success
    expect(statuses).toEqual(["PASSED", "PASSED", "PASSED"])
    expect(success).toBe(true)
  })
})
