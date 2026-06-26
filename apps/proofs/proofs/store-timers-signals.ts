import { s2WorkflowExecutionStore } from "@firegrid/store"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow S2 store promise failed" })
  })

export default proof("store.timers-signals")
  .describedAs(
    "Proves the TanStack Workflow S2 store wakeup path: a paused run schedules a timer, a sweeper claims it, and signal delivery queues the run exactly once."
  )
  .spec(({ property, trialId }) =>
    property("store.timers-signals-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "store timer proof requires s2Lite" })
          }
          const store = s2WorkflowExecutionStore({
            namespace: `timers-${trialId}`,
            s2Endpoint
          })
          const runId = "run-timer"
          yield* promise(() =>
            store.createRun({
              input: {},
              now: 1_000,
              runId,
              workflowId: "timer-workflow",
              workflowVersion: "v1"
            })
          )
          yield* promise(() =>
            store.markRunPaused({
              now: 1_100,
              runId,
              waitingFor: {
                deadline: 1_500,
                signalName: "__timer",
                stepId: "sleep"
              },
              wakeAt: 1_500
            })
          )
          yield* promise(() =>
            store.scheduleTimer({
              now: 1_100,
              runId,
              signalId: "timer-sleep",
              wakeAt: 1_500,
              workflowId: "timer-workflow",
              workflowVersion: "v1"
            })
          )
          const early = yield* promise(() =>
            store.claimDueTimers({ leaseMs: 500, leaseOwner: "sweeper", limit: 10, now: 1_400 })
          )
          const due = yield* promise(() =>
            store.claimDueTimers({ leaseMs: 500, leaseOwner: "sweeper", limit: 10, now: 1_600 })
          )
          const delivered = yield* promise(() =>
            store.deliverSignal({
              delivery: {
                name: "__timer",
                signalId: "timer-sleep",
                stepId: "sleep",
                payload: undefined
              },
              now: 1_600,
              runId
            })
          )
          const duplicate = yield* promise(() =>
            store.deliverSignal({
              delivery: {
                name: "__timer",
                signalId: "timer-sleep",
                stepId: "sleep",
                payload: undefined
              },
              now: 1_700,
              runId
            })
          )
          return {
            deliveredKind: delivered.kind,
            duplicateKind: duplicate.kind,
            dueSignals: due.map((timer) => timer.signalId),
            earlyCount: early.length,
            runStatus: delivered.kind === "delivered" ? delivered.run.status : undefined
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          deliveredKind: "delivered",
          duplicateKind: "duplicate",
          dueSignals: ["timer-sleep"],
          earlyCount: 0,
          runStatus: "queued"
        }),
        traceSql(
          "timer-signal-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
