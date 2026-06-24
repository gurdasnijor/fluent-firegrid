import { layer as ChdbLayer } from "@firegrid/observability"
import { Effect, Layer } from "effect"
import { describe, expect, it } from "vitest"

import { expectWorkloadResult, property, runProperty, traceSql, VerificationRuntime } from "../src/index.ts"

const TestLayer = Layer.mergeAll(ChdbLayer({}), VerificationRuntime.layer)

describe("property", () => {
  it("runs an Effect workload and post-run checks", () =>
    Effect.gen(function*() {
      const spec = property("durable.step-replay")
        .host("worker", { opaque: "host" })
        .workload(() => Effect.succeed({ greeting: "Hello, Ada!" }))
        .verify(
          expectWorkloadResult({ greeting: "Hello, Ada!" }),
          traceSql("constant-trace-proof", "SELECT 1 AS ok")
        )

      const trial = yield* runProperty(spec, { trialId: "trial-1" })

      expect(trial.trialId).toBe("trial-1")
      expect(trial.result._tag).toBe("Success")
    }).pipe(
      Effect.provide(TestLayer),
      Effect.scoped,
      Effect.runPromise
    ))

  it("fails when the workload result does not match", () =>
    Effect.gen(function*() {
      const spec = property("result-mismatch")
        .workload(() => Effect.succeed(1))
        .verify(expectWorkloadResult(2))

      const exit = yield* Effect.exit(runProperty(spec))

      expect(exit._tag).toBe("Failure")
    }).pipe(
      Effect.provide(TestLayer),
      Effect.scoped,
      Effect.runPromise
    ))
})
