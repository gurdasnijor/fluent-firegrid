import { layer as ChdbLayer } from "@firegrid/observability"
import { Effect, Layer } from "effect"
import { describe, expect, it } from "vitest"

import { expectWorkloadResult, property, runProperty, traceSql, VerificationRuntime } from "../src/index.ts"
import { layer as TraceRuntimeLayer } from "../src/TraceRuntime.ts"

const TestRuntimeLayer = Layer.succeed(VerificationRuntime, {
  flush: Effect.void,
  waitForSpan: Effect.fn("TestVerificationRuntime.waitForSpan")(() => Effect.void)
})
const TestLayer = Layer.mergeAll(ChdbLayer({}), TestRuntimeLayer)
const LiveTraceLayer = TraceRuntimeLayer({ serviceName: "firegrid-verification-test" })

describe("property", () => {
  it("fails loudly when no trace runtime is installed", () =>
    Effect.gen(function*() {
      const spec = property("missing-trace-runtime")
        .workload(() => Effect.succeed("ok"))
        .verify(expectWorkloadResult("ok"))

      const exit = yield* Effect.exit(runProperty(spec))

      expect(exit._tag).toBe("Failure")
    }).pipe(
      Effect.provide(Layer.mergeAll(ChdbLayer({}), VerificationRuntime.layer)),
      Effect.scoped,
      Effect.runPromise
    ))

  it("exports workload and operation spans to chDB", () =>
    Effect.gen(function*() {
      const spec = property("durable.step-replay")
        .host("worker", { opaque: "host" })
        .workload(({ operation }) =>
          operation(
            "greeter.process",
            { name: "Ada" },
            Effect.succeed({ greeting: "Hello, Ada!" }),
            { clientId: 1, operationId: 1, key: "Ada" }
          )
        )
        .verify(
          expectWorkloadResult({ greeting: "Hello, Ada!" }),
          traceSql(
            "operation-span-exported",
            `
            SELECT countIf(SpanName = 'verification.operation') = 1 AS ok
            FROM trial_spans
          `
          ),
          traceSql(
            "operation-view-projects-boundary",
            `
            SELECT count() = 1
              AND any(operation) = 'greeter.process'
              AND any(operation_key) = 'Ada'
              AND any(status) = 'ok' AS ok
            FROM verification_operations
          `
          )
        )

      const trial = yield* runProperty(spec, { trialId: "trial-1" })

      expect(trial.trialId).toBe("trial-1")
      expect(trial.result._tag).toBe("Success")
    }).pipe(
      Effect.provide(LiveTraceLayer),
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
