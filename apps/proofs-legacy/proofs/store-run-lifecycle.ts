import { type RunState, s2WorkflowExecutionStore, type WorkflowEvent } from "@firegrid/fluent/s2"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow S2 store promise failed" })
  })

const runStarted = (runId: string): WorkflowEvent => ({ runId, ts: 10, type: "RUN_STARTED" })

export default proof("store.run-lifecycle")
  .describedAs(
    "Proves the TanStack Workflow S2 store run lifecycle: createRun, saveRunState, loadExecution, terminal state, and timeline survive a fresh adapter instance over real S2."
  )
  .spec(({ property, trialId }) =>
    property("store.run-lifecycle-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "store lifecycle proof requires s2Lite" })
          }
          const config = { namespace: `lifecycle-${trialId}`, s2Endpoint }
          const runId = "run-lifecycle"
          const store = s2WorkflowExecutionStore(config)
          const created = yield* promise(() =>
            store.createRun({
              input: { name: "Ada" },
              now: 100,
              runId,
              workflowId: "greeter",
              workflowVersion: "v1"
            })
          )
          yield* promise(() => store.appendEvents({ events: [runStarted(runId)], expectedNextIndex: 0, runId }))
          const state: RunState = {
            createdAt: 100,
            input: { name: "Ada" },
            runId,
            status: "paused",
            updatedAt: 110,
            waitingFor: {
              signalName: "approval",
              stepId: "wait-approval"
            },
            workflowId: "greeter",
            workflowVersion: "v1"
          }
          yield* promise(() => store.saveRunState({ state }))

          const freshStore = s2WorkflowExecutionStore(config)
          const loaded = yield* promise(() => freshStore.loadExecution(runId))
          yield* promise(() => freshStore.markRunFinished({ now: 200, output: { ok: true }, runId }))
          const timeline = yield* promise(() => freshStore.getRunTimeline(runId))
          return {
            createdKind: created.kind,
            loadedEventCount: loaded?.events.length,
            loadedStatus: loaded?.run.status,
            loadedWaitingFor: loaded?.run.waitingFor?.signalName,
            terminalOutput: timeline?.run.output,
            terminalStatus: timeline?.run.status
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          createdKind: "created",
          loadedEventCount: 1,
          loadedStatus: "paused",
          loadedWaitingFor: "approval",
          terminalOutput: { ok: true },
          terminalStatus: "finished"
        }),
        traceSql(
          "lifecycle-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
