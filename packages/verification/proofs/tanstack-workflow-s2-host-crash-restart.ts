import * as Effect from "effect/Effect"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const workerPath = new URL("../fixtures/tanstack-workflow-s2-host-worker.ts", import.meta.url).pathname

const portFromTrialId = (trialId: string): number => {
  const hash = Array.from(trialId).reduce((current, char) => (current * 31 + char.charCodeAt(0)) % 10_000, 0)
  return 35_000 + hash
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
    catch: (cause) => new VerificationError({ cause, message: `host worker request failed: ${url}` })
  })

export default proof("tanstack-workflow-s2.host-crash-restart")
  .describedAs(
    "Proves the exported S2 workflow host survives process death: a worker starts and durably pauses a sleeping run, is killed, restarts, then ticks the timer and completes the run from S2."
  )
  .spec(({ property, trialId }) => {
    const hostPort = portFromTrialId(trialId)
    const baseUrl = `http://127.0.0.1:${hostPort}`

    return property("tanstack-workflow-s2.host-crash-restart-proof")
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
            attempts: 400,
            interval: "50 millis",
            url: `${baseUrl}/ready`
          }
        })
      )
      .workload(({ faults }) =>
        Effect.gen(function*() {
          const started = yield* requestJson<{
            readonly kind: string
            readonly run?: { readonly waitingFor?: { readonly signalName?: string }; readonly wakeAt?: number }
          }>(`${baseUrl}/start`, { method: "POST" })

          yield* faults.killHost("worker")
          yield* faults.restartHost("worker")

          const tick = yield* requestJson<{
            readonly sweep: { readonly timers: ReadonlyArray<{ readonly kind: string }> }
          }>(`${baseUrl}/tick?now=5000`, { method: "POST" })

          const execution = yield* requestJson<{
            readonly run: { readonly output?: unknown; readonly status: string }
            readonly events: ReadonlyArray<{ readonly eventType: string }>
          }>(`${baseUrl}/execution`)

          return {
            eventTypes: execution.events.map((event) => event.eventType),
            output: execution.run.output,
            runStatus: execution.run.status,
            startKind: started.kind,
            startWakeAt: started.run?.wakeAt,
            startWaitingFor: started.run?.waitingFor?.signalName,
            timerKinds: tick.sweep.timers.map((timer) => timer.kind)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          eventTypes: [
            "STEP_FINISHED",
            "SIGNAL_AWAITED",
            "SIGNAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          output: {
            completed: true,
            runId: "crash-sleep:run-1"
          },
          runStatus: "finished",
          startKind: "paused",
          startWakeAt: 5_000,
          startWaitingFor: "__timer",
          timerKinds: ["completed"]
        }),
        traceSql(
          "host-crash-restart-killed-worker",
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
          "host-crash-restart-started-twice",
          `
          SELECT countIf(SpanName = 'verification.host.start') = 2
            AND countIf(SpanName = 'verification.host.restart') = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "host-crash-restart-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
