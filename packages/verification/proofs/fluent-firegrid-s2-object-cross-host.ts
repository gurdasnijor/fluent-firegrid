import * as Effect from "effect/Effect"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = "packages/verification/fixtures/fluent-firegrid-s2-object-worker.ts"

const portFromTrialId = (trialId: string, salt: string): number => {
  const hash = Array.from(`fluent-object-cross-host-${trialId}-${salt}`).reduce(
    (current, char) => (current * 31 + char.charCodeAt(0)) % 10_000,
    0
  )
  return 45_000 + hash
}

const requestJson = <A>(url: string, init?: RequestInit): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: async () => {
      const response = await fetch(url, init)
      if (!response.ok) {
        throw new Error(`request ${url} failed with ${response.status}: ${await response.text()}`)
      }
      return await response.json() as A
    },
    catch: (cause) => new VerificationError({ cause, message: `fluent object cross-host request failed: ${url}` })
  })

export default proof("fluent-firegrid-s2.object-cross-host")
  .describedAs(
    "Proves S2-backed objectClient cross-host serialization: two worker processes race same-key object calls and final table state does not lose updates."
  )
  .spec(({ property, trialId }) => {
    const portA = portFromTrialId(trialId, "a")
    const portB = portFromTrialId(trialId, "b")
    const hostA = `http://127.0.0.1:${portA}`
    const hostB = `http://127.0.0.1:${portB}`

    return property("fluent-firegrid-s2.object-cross-host-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({
        a: processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: { HOST_PORT: String(portA) },
          readiness: {
            attempts: 120,
            interval: "25 millis",
            url: `${hostA}/ready`
          }
        }),
        b: processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: { HOST_PORT: String(portB) },
          readiness: {
            attempts: 120,
            interval: "25 millis",
            url: `${hostB}/ready`
          }
        })
      })
      .workload(() =>
        Effect.gen(function*() {
          const results = yield* Effect.all([
            requestJson<{ readonly hostId: string; readonly value: number }>(`${hostA}/add?by=5`, { method: "POST" }),
            requestJson<{ readonly hostId: string; readonly value: number }>(`${hostB}/add?by=7`, { method: "POST" })
          ], { concurrency: "unbounded" })
          const loaded = yield* requestJson<{ readonly value: number }>(`${hostA}/value`)
          return {
            completedCalls: results.length,
            maxResult: Math.max(...results.map((result) => result.value)),
            participatingHosts: [...new Set(results.map((result) => result.hostId))].sort(),
            value: loaded.value
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          completedCalls: 2,
          maxResult: 12,
          participatingHosts: ["a", "b"],
          value: 12
        }),
        traceSql(
          "fluent-object-cross-host-started-two-workers",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-cross-host-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
