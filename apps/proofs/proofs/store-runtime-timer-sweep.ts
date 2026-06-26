import { createWorkflow, defineWorkflowRuntime } from "@firegrid/runtime"
import { s2WorkflowExecutionStore } from "@firegrid/store"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

interface StepCounters {
  beforeSleep: number
  afterSleep: number
}

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow runtime timer promise failed" })
  })

const makeTimerWorkflow = (counters: StepCounters) =>
  createWorkflow({
    id: "timer-workflow"
  }).handler(async (ctx) => {
    await ctx.step("before-sleep", async () => {
      counters.beforeSleep += 1
      return { ok: true }
    })

    const wakeAt = (ctx.input as { readonly wakeAt: number }).wakeAt
    await ctx.sleepUntil(wakeAt)

    await ctx.step("after-sleep", async () => {
      counters.afterSleep += 1
      return { wokeAt: wakeAt }
    })

    return {
      completed: true,
      wokeAt: wakeAt
    }
  })

export default proof("store.runtime-timer-sweep")
  .describedAs(
    "Proves the S2 store backs TanStack Workflow runtime timer execution: sleepUntil schedules a durable timer, sweep ignores it before the deadline, and a recreated runtime resumes the run after the timer is due."
  )
  .spec(({ property, trialId }) =>
    property("store.runtime-timer-sweep-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "store runtime timer proof requires s2Lite" })
          }

          const counters: StepCounters = { beforeSleep: 0, afterSleep: 0 }
          const workflow = makeTimerWorkflow(counters)
          const config = {
            namespace: `runtime-timer-${trialId}`,
            s2Endpoint
          }
          const runId = "timer:run-1"
          const wakeAt = 5_000

          const runtime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows: {
              "timer-workflow": {
                load: async () => workflow
              }
            }
          })

          const started = yield* promise(() =>
            runtime.startRun({
              input: { wakeAt },
              now: 1_000,
              runId,
              workflowId: "timer-workflow"
            })
          )

          const restartedRuntime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows: {
              "timer-workflow": {
                load: async () => workflow
              }
            }
          })

          const early = yield* promise(() =>
            restartedRuntime.sweep({
              leaseMs: 1_000,
              leaseOwner: "timer-sweep",
              maxScheduledRuns: 0,
              maxTimers: 10,
              now: wakeAt - 1
            })
          )

          const swept = yield* promise(() =>
            restartedRuntime.sweep({
              leaseMs: 1_000,
              leaseOwner: "timer-sweep",
              maxScheduledRuns: 0,
              maxTimers: 10,
              now: wakeAt
            })
          )

          const loaded = yield* promise(() => s2WorkflowExecutionStore(config).loadExecution(runId))

          return {
            afterSleepCalls: counters.afterSleep,
            beforeSleepCalls: counters.beforeSleep,
            earlyTimerCount: early.timers.length,
            eventTypes: loaded?.events.map((event) => event.eventType),
            output: swept.timers[0]?.run?.output,
            startKind: started.kind,
            startWaitingFor: started.run?.waitingFor?.signalName,
            startWakeAt: started.run?.wakeAt,
            sweepEventCount: swept.summary.eventCount,
            sweepTimerKinds: swept.timers.map((result) => result.kind)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          afterSleepCalls: 1,
          beforeSleepCalls: 1,
          earlyTimerCount: 0,
          eventTypes: [
            "STEP_FINISHED",
            "SIGNAL_AWAITED",
            "SIGNAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          output: {
            completed: true,
            wokeAt: 5_000
          },
          startKind: "paused",
          startWaitingFor: "__timer",
          startWakeAt: 5_000,
          sweepEventCount: 5,
          sweepTimerKinds: ["completed"]
        }),
        traceSql(
          "runtime-timer-sweep-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "runtime-timer-sweep-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
