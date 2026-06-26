import { LogConflictError, s2WorkflowExecutionStore, type SerializedError, type WorkflowEvent } from "@firegrid/store"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const error = (message: string): SerializedError => ({ message, name: "Error" })

const runStarted = (runId: string): WorkflowEvent => ({ runId, ts: 1, type: "RUN_STARTED" })
const stepStarted = (stepId: string): WorkflowEvent => ({ stepId, ts: 2, type: "STEP_STARTED" })
const stepFailed = (stepId: string): WorkflowEvent => ({
  error: error("failed"),
  stepId,
  ts: 3,
  type: "STEP_FAILED"
})
const stepFinished = (stepId: string): WorkflowEvent => ({
  result: { ok: true },
  stepId,
  ts: 4,
  type: "STEP_FINISHED"
})

const eventSummary = (
  stored: { readonly eventIndex: number; readonly eventType: string; readonly stepId?: string }
) => ({
  eventIndex: stored.eventIndex,
  eventType: stored.eventType,
  ...(stored.stepId === undefined ? {} : { stepId: stored.stepId })
})

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError | LogConflictError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) =>
      cause instanceof LogConflictError
        ? cause
        : new VerificationError({ cause, message: "TanStack Workflow S2 store promise failed" })
  })

export default proof("store.event-log-cas")
  .describedAs(
    "Proves the TanStack Workflow S2 store event log: appendEvents uses S2 matchSeqNum CAS, stale writers map to LogConflictError, and readEvents returns ordered committed events."
  )
  .spec(({ property, trialId }) =>
    property("store.event-log-cas-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "store event log proof requires s2Lite" })
          }
          const store = s2WorkflowExecutionStore({
            namespace: `event-log-${trialId}`,
            s2Endpoint
          })
          const runId = "run-event-log"
          const first = yield* promise(() =>
            store.appendEvents({
              events: [runStarted(runId), stepStarted("step-1")],
              expectedNextIndex: 0,
              runId
            })
          )
          const stale = yield* promise(() =>
            store.appendEvents({
              events: [stepFailed("step-1")],
              expectedNextIndex: 0,
              runId
            })
          ).pipe(
            Effect.flip,
            Effect.filterOrFail(
              (cause): cause is LogConflictError => cause instanceof LogConflictError,
              (cause) => cause
            )
          )
          const second = yield* promise(() =>
            store.appendEvents({
              events: [stepFinished("step-1")],
              expectedNextIndex: first.nextIndex,
              runId
            })
          )
          const readBack = yield* promise(() => store.readEvents({ runId }))
          return {
            conflictAttemptedIndex: stale.attemptedIndex,
            firstNextIndex: first.nextIndex,
            readBack: readBack.map(eventSummary),
            secondNextIndex: second.nextIndex
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          conflictAttemptedIndex: 0,
          firstNextIndex: 2,
          readBack: [
            { eventIndex: 0, eventType: "RUN_STARTED" },
            { eventIndex: 1, eventType: "STEP_STARTED", stepId: "step-1" },
            { eventIndex: 2, eventType: "STEP_FINISHED", stepId: "step-1" }
          ],
          secondNextIndex: 3
        }),
        traceSql(
          "event-log-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "event-log-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
