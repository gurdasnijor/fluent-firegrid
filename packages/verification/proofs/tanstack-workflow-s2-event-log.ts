import { LogConflictError, s2WorkflowExecutionStore, type WorkflowEvent } from "@firegrid/tanstack-workflow-s2"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const event = (type: string, ts: number, stepId?: string): WorkflowEvent => ({
  type,
  ts,
  ...(stepId === undefined ? {} : { stepId })
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

export default proof("tanstack-workflow-s2.event-log-cas")
  .describedAs(
    "Proves the TanStack Workflow S2 store event log: appendEvents uses S2 matchSeqNum CAS, stale writers map to LogConflictError, and readEvents returns ordered committed events."
  )
  .spec(({ property, trialId }) =>
    property("tanstack-workflow-s2.event-log-cas-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "tanstack-workflow-s2 event log proof requires s2Lite" })
          }
          const store = s2WorkflowExecutionStore({
            namespace: `event-log-${trialId}`,
            s2Endpoint
          })
          const runId = "run-event-log"
          const first = yield* promise(() =>
            store.appendEvents({
              events: [event("RUN_STARTED", 1), event("STEP_STARTED", 2, "step-1")],
              expectedNextIndex: 0,
              runId
            })
          )
          const stale = yield* promise(() =>
            store.appendEvents({
              events: [event("STEP_FAILED", 3, "step-1")],
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
              events: [event("STEP_FINISHED", 4, "step-1")],
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
