import * as Effect from "effect/Effect"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = "packages/verification/fixtures/fluent-firegrid-signal-worker.ts"

const portFromTrialId = (trialId: string): number => {
  const hash = Array.from(`fluent-signal-${trialId}`).reduce(
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
    catch: (cause) => new VerificationError({ cause, message: `fluent signal worker request failed: ${url}` })
  })

export default proof("fluent-firegrid.signal-restart")
  .describedAs(
    "Proves fluent waitForSignal lowers to TanStack/S2 signal waits: a service handler pauses, the worker restarts, a signal is delivered, and the run completes from S2."
  )
  .spec(({ property, trialId }) => {
    const hostPort = portFromTrialId(trialId)
    const baseUrl = `http://127.0.0.1:${hostPort}`

    return property("fluent-firegrid.signal-restart-proof")
      .s2Lite({ persistence: "local-root" })
      .host(
        "worker",
        processHost({
          args: ["exec", "tsx", workerPath],
          command: "pnpm",
          env: {
            HOST_PORT: String(hostPort)
          },
          readiness: {
            attempts: 120,
            interval: "25 millis",
            url: `${baseUrl}/ready`
          }
        })
      )
      .workload(({ faults }) =>
        Effect.gen(function*() {
          const reference = yield* requestJson<{
            readonly invocationId: string
          }>(`${baseUrl}/send`, { method: "POST" })

          yield* faults.killHost("worker")
          yield* faults.restartHost("worker")

          const delivered = yield* requestJson<{
            readonly kind: string
          }>(`${baseUrl}/signal`, { method: "POST" })

          const loaded = yield* requestJson<{
            readonly workflowId: string
            readonly execution?: {
              readonly run: { readonly output?: unknown; readonly status: string }
              readonly events: ReadonlyArray<{ readonly eventType: string; readonly stepId?: string }>
            }
          }>(`${baseUrl}/execution`)

          return {
            deliveredKind: delivered.kind,
            eventStepIds: loaded.execution?.events.map((event) => event.stepId ?? ""),
            eventTypes: loaded.execution?.events.map((event) => event.eventType),
            invocationId: reference.invocationId,
            output: loaded.execution?.run.output,
            runStatus: loaded.execution?.run.status,
            workflowId: loaded.workflowId
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          deliveredKind: "completed",
          eventStepIds: ["reserve", "payment", "payment", "ship", ""],
          eventTypes: [
            "STEP_FINISHED",
            "SIGNAL_AWAITED",
            "SIGNAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          invocationId: "fluent-signals:run-1",
          output: {
            orderId: "order-1",
            paymentId: "pay-1",
            shippedBy: "worker"
          },
          runStatus: "finished",
          workflowId: "service:signal-orders:submit"
        }),
        traceSql(
          "fluent-signal-restart-killed-worker",
          `
          SELECT countIf(
            SpanName = 'verification.host.kill'
            AND SpanAttributes['firegrid.host.id'] = 'worker'
            AND SpanAttributes['verification.signal'] = 'SIGKILL'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-signal-restart-started-twice",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2
            AND countIf(SpanName = 'verification.host.restart') = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-signal-restart-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
