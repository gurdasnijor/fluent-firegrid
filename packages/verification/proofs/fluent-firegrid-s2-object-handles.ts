import * as Effect from "effect/Effect"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = new URL("../fixtures/fluent-firegrid-s2-object-worker.ts", import.meta.url).pathname

const portFromTrialId = (trialId: string, salt: string): number => {
  const hash = Array.from(`fluent-object-handles-${trialId}-${salt}`).reduce(
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
    catch: (cause) => new VerificationError({ cause, message: `fluent object handle request failed: ${url}` })
  })

const handleRunId = "object:cross-host-counter:counter-1:crashAfterSet"

export default proof("fluent-firegrid-s2.object-handles")
  .describedAs(
    "Proves S2-backed object send handles: sendObjectClient admits a durable same-key call and returns before execution; another host attaches by invocation id and obtains the output."
  )
  .spec(({ property, trialId }) => {
    const portA = portFromTrialId(trialId, "a")
    const portB = portFromTrialId(trialId, "b")
    const hostA = `http://127.0.0.1:${portA}`
    const hostB = `http://127.0.0.1:${portB}`

    return property("fluent-firegrid-s2.object-handles-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({
        a: processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: { HOST_PORT: String(portA) },
          readiness: {
            attempts: 400,
            interval: "50 millis",
            url: `${hostA}/ready`
          }
        }),
        b: processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: { HOST_PORT: String(portB) },
          readiness: {
            attempts: 400,
            interval: "50 millis",
            url: `${hostB}/ready`
          }
        })
      })
      .workload(({ faults }) =>
        Effect.gen(function*() {
          const sent = yield* requestJson<{
            readonly hostId: string
            readonly reference: {
              readonly handler?: string
              readonly invocationId: string
              readonly key?: string
              readonly kind?: string
              readonly name?: string
              readonly output?: unknown
            }
          }>(`${hostA}/send-crash-after-set?by=5&runId=${encodeURIComponent(handleRunId)}`, { method: "POST" })

          yield* faults.killHost("a")

          const attached = yield* requestJson<{ readonly hostId: string; readonly value: number }>(
            `${hostB}/attach-crash-after-set?by=5&runId=${encodeURIComponent(sent.reference.invocationId)}`,
            { method: "POST" }
          )
          const afterAttach = yield* requestJson<{ readonly hostId: string; readonly value: number }>(
            `${hostB}/add?by=7`,
            { method: "POST" }
          )
          const loaded = yield* requestJson<{ readonly hostId: string; readonly value: number }>(`${hostB}/value`)

          return {
            afterAttach,
            attached,
            reference: sent.reference,
            sendHost: sent.hostId,
            value: loaded.value
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          afterAttach: {
            hostId: "b",
            value: 12
          },
          attached: {
            hostId: "b",
            value: 5
          },
          reference: {
            handler: "crashAfterSet",
            invocationId: "object:cross-host-counter:counter-1:crashAfterSet",
            key: "counter-1",
            kind: "object",
            name: "cross-host-counter"
          },
          sendHost: "a",
          value: 12
        }),
        traceSql(
          "fluent-object-handles-killed-sender",
          `
          SELECT countIf(
            SpanName = 'verification.host.kill'
            AND SpanAttributes['firegrid.host.id'] = 'a'
            AND SpanAttributes['verification.signal'] = 'SIGKILL'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-handles-started-two-workers",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-handles-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
