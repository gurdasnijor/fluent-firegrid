import * as Effect from "effect/Effect"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = "packages/verification/fixtures/fluent-firegrid-object-worker.ts"

const portFromTrialId = (trialId: string): number => {
  const hash = Array.from(`fluent-object-${trialId}`).reduce(
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
    catch: (cause) => new VerificationError({ cause, message: `fluent object worker request failed: ${url}` })
  })

export default proof("fluent-firegrid.object-key-restart")
  .describedAs(
    "Proves keyed fluent object invocations carry the object key through the TanStack/S2 runtime: objectClient starts a keyed handler, the worker dies while sleeping, restarts, ticks the timer, and the handler completes with the same key from S2."
  )
  .spec(({ property, trialId }) => {
    const hostPort = portFromTrialId(trialId)
    const baseUrl = `http://127.0.0.1:${hostPort}`

    return property("fluent-firegrid.object-key-restart-proof")
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

          const tick = yield* requestJson<{
            readonly sweep: { readonly timers: ReadonlyArray<{ readonly kind: string }> }
          }>(`${baseUrl}/tick?now=5000`, { method: "POST" })

          const loaded = yield* requestJson<{
            readonly workflowId: string
            readonly execution?: {
              readonly run: { readonly output?: unknown; readonly status: string }
              readonly events: ReadonlyArray<{ readonly eventType: string; readonly stepId?: string }>
            }
          }>(`${baseUrl}/execution`)

          return {
            eventStepIds: loaded.execution?.events.map((event) => event.stepId ?? ""),
            eventTypes: loaded.execution?.events.map((event) => event.eventType),
            invocationId: reference.invocationId,
            output: loaded.execution?.run.output,
            runStatus: loaded.execution?.run.status,
            timerKinds: tick.sweep.timers.map((timer) => timer.kind),
            workflowId: loaded.workflowId
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          eventStepIds: ["record", "__sleep-0", "__sleep-0", "finish", ""],
          eventTypes: [
            "STEP_FINISHED",
            "SIGNAL_AWAITED",
            "SIGNAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          invocationId: "fluent-object:run-1",
          output: {
            amount: 5,
            finishedBy: "worker",
            key: "counter-1",
            recordedBy: "worker"
          },
          runStatus: "finished",
          timerKinds: ["completed"],
          workflowId: "object:counters:add"
        }),
        traceSql(
          "fluent-object-restart-killed-worker",
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
          "fluent-object-restart-started-twice",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2
            AND countIf(SpanName = 'verification.host.restart') = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-object-restart-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
