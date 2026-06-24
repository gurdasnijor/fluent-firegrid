import * as NodeFileSystem from "@effect/platform-node/NodeFileSystem"
import { layer as ChdbLayer } from "@firegrid/observability"
import { Effect, Layer } from "effect"
import { FileSystem } from "effect/FileSystem"
import { describe, expect, it } from "vitest"

import {
  expectWorkloadResult,
  processHost,
  property,
  runProperty,
  traceSql,
  VerificationRuntime
} from "../src/index.ts"
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
        .workload(({ operation, runtime }) =>
          Effect.gen(function*() {
            const result = yield* operation(
              "greeter.process",
              { name: "Ada" },
              Effect.succeed({ greeting: "Hello, Ada!" }),
              { clientId: 1, operationId: 1, key: "Ada" }
            )
            yield* runtime.waitForSpan("verification.operation", {
              attributes: { "firegrid.operation.name": "greeter.process" }
            })
            return result
          })
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

  it("does not let waitForSpan match another trial", () =>
    Effect.gen(function*() {
      const first = property("first-trial")
        .workload(({ operation }) =>
          operation(
            "greeter.process",
            { name: "Ada" },
            Effect.succeed({ greeting: "Hello, Ada!" }),
            { clientId: 1, operationId: 1, key: "Ada" }
          )
        )
        .verify()

      yield* runProperty(first, { trialId: "first-trial" })

      const second = property("second-trial")
        .workload(({ runtime }) =>
          runtime.waitForSpan("verification.operation", {
            attempts: 0,
            attributes: { "firegrid.operation.name": "greeter.process" }
          }).pipe(Effect.as("ok"))
        )
        .verify(expectWorkloadResult("ok"))

      const exit = yield* Effect.exit(runProperty(second, { trialId: "second-trial" }))

      expect(exit._tag).toBe("Failure")
    }).pipe(
      Effect.provide(LiveTraceLayer),
      Effect.scoped,
      Effect.runPromise
    ))

  it("kills and restarts a supervised process host after a trial-scoped span", () =>
    Effect.gen(function*() {
      const spec = property("supervised-host-fault")
        .host(
          "worker",
          processHost({
            command: "node",
            args: ["-e", "setInterval(() => undefined, 1000)"]
          })
        )
        .workload(({ faults, operation }) =>
          Effect.gen(function*() {
            const result = yield* operation(
              "fault.probe",
              { host: "worker" },
              Effect.succeed("ok"),
              { operationId: 1 }
            )
            yield* faults.killHostAfterSpan("worker", {
              span: "verification.operation",
              attributes: { "firegrid.operation.name": "fault.probe" }
            })
            yield* faults.restartHost("worker")
            yield* faults.killHost("worker")
            return result
          })
        )
        .verify(
          expectWorkloadResult("ok"),
          traceSql(
            "host-killed-after-span",
            `
            SELECT countIf(
              SpanName = 'verification.host.kill'
              AND SpanAttributes['firegrid.host.id'] = 'worker'
              AND SpanAttributes['verification.signal'] = 'SIGKILL'
            ) = 2 AS ok
            FROM trial_spans
          `
          ),
          traceSql(
            "host-restarted",
            `
            SELECT countIf(SpanName = 'verification.host.start') = 2
              AND countIf(SpanName = 'verification.host.restart') = 1 AS ok
            FROM trial_spans
          `
          )
        )

      const trial = yield* runProperty(spec, { trialId: "supervised-host-fault" })

      expect(trial.result._tag).toBe("Success")
    }).pipe(
      Effect.provide(LiveTraceLayer),
      Effect.scoped,
      Effect.runPromise
    ))

  it("starts configured s2 lite and injects trial host environment into process hosts", () =>
    Effect.gen(function*() {
      const s2Port = 32201
      const hostPort = 32202
      const spec = property("s2-env-injection")
        .s2Lite({ persistence: "local-root" })
        .host(
          "worker",
          processHost({
            command: "node",
            args: [
              "-e",
              `
              if (process.env.FIREGRID_TRIAL_ID !== "s2-env-injection") process.exit(1)
              if (process.env.FIREGRID_HOST_ID !== "worker") process.exit(1)
              if (process.env.S2_ENDPOINT !== "http://127.0.0.1:${s2Port}") process.exit(1)
              require("node:http").createServer((_, res) => res.end("ok")).listen(${hostPort})
            `
            ],
            readiness: {
              url: `http://127.0.0.1:${hostPort}`,
              attempts: 80,
              interval: "25 millis"
            }
          })
        )
        .workload(({ faults }) =>
          Effect.gen(function*() {
            yield* faults.killHost("worker")
            return "ok"
          })
        )
        .verify(
          expectWorkloadResult("ok"),
          traceSql(
            "host-started-with-s2-env",
            `
            SELECT countIf(SpanName = 'verification.host.start') = 1
              AND countIf(SpanName = 'verification.host.kill') = 1 AS ok
            FROM trial_spans
          `
          )
        )

      const trial = yield* runProperty(spec, {
        trialId: "s2-env-injection",
        s2Lite: {
          bin: "node",
          args: (cfg) => [
            "-e",
            `require("node:http").createServer((_, res) => res.end("ok")).listen(${cfg.port})`
          ],
          port: s2Port,
          localRoot: "/tmp/firegrid-verification-s2-env-injection"
        }
      })

      expect(trial.result._tag).toBe("Success")
    }).pipe(
      Effect.provide(LiveTraceLayer),
      Effect.scoped,
      Effect.runPromise
    ))

  it("writes a trial report artifact for passing checks", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem
      const reportDir = yield* fs.makeTempDirectoryScoped({ prefix: "firegrid-verification-report-pass-" })
      const spec = property("report-pass")
        .workload(({ operation }) => operation("report.probe", {}, Effect.succeed("ok"), { operationId: 1 }))
        .verify(expectWorkloadResult("ok"))

      const trial = yield* runProperty(spec, { trialId: "report-pass", reportDir })

      expect(trial.report?.status).toBe("passed")
      expect(trial.report?.path).toBe(`${reportDir}/report-pass.json`)
      const reportJson = yield* fs.readFileString(trial.report!.path!)
      expect(reportJson).toContain("\"status\": \"passed\"")
      expect(reportJson).toContain("verification.operation")
    }).pipe(
      Effect.provide(Layer.mergeAll(LiveTraceLayer, NodeFileSystem.layer)),
      Effect.scoped,
      Effect.runPromise
    ))

  it("writes a counterexample report when a check fails", () =>
    Effect.gen(function*() {
      const fs = yield* FileSystem
      const reportDir = yield* fs.makeTempDirectoryScoped({ prefix: "firegrid-verification-report-fail-" })
      const spec = property("report-fail")
        .workload(({ operation }) => operation("report.probe", {}, Effect.succeed("ok"), { operationId: 1 }))
        .verify(traceSql("intentional-failure", "SELECT 0 AS ok"))

      const exit = yield* Effect.exit(runProperty(spec, { trialId: "report-fail", reportDir }))

      expect(exit._tag).toBe("Failure")
      const reportJson = yield* fs.readFileString(`${reportDir}/report-fail.json`)
      expect(reportJson).toContain("\"status\": \"failed\"")
      expect(reportJson).toContain("\"failedCheck\": \"intentional-failure\"")
      expect(reportJson).toContain("verification.operation")
    }).pipe(
      Effect.provide(Layer.mergeAll(LiveTraceLayer, NodeFileSystem.layer)),
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

  it("compares workload results structurally", () =>
    Effect.gen(function*() {
      const spec = property("structural-result")
        .workload(() => Effect.succeed({ a: 1, b: 2 }))
        .verify(expectWorkloadResult({ b: 2, a: 1 }))

      const trial = yield* runProperty(spec)

      expect(trial.result._tag).toBe("Success")
    }).pipe(
      Effect.provide(TestLayer),
      Effect.scoped,
      Effect.runPromise
    ))
})
