import { createWorkflow } from "@tanstack/workflow-core"
import { defineWorkflowRuntime, every, materializeWorkflowSchedules } from "@tanstack/workflow-runtime"
import { s2WorkflowExecutionStore } from "@firegrid/tanstack-workflow-s2"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

interface StepCounters {
  scheduled: number
}

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow runtime schedule promise failed" })
  })

const makeScheduledWorkflow = (counters: StepCounters) =>
  createWorkflow({
    id: "scheduled-workflow"
  }).handler(async (ctx) => {
    const input = ctx.input as { readonly source: string }
    await ctx.step("scheduled-step", async () => {
      counters.scheduled += 1
      return { source: input.source }
    })

    return {
      completed: true,
      source: input.source
    }
  })

export default proof("tanstack-workflow-s2.runtime-schedule-sweep")
  .describedAs(
    "Proves the S2 store backs TanStack Workflow runtime schedules: a due schedule materializes to one durable bucket, two recreated sweepers race it, and only one scheduled run starts and completes."
  )
  .spec(({ property, trialId }) =>
    property("tanstack-workflow-s2.runtime-schedule-sweep-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "tanstack-workflow-s2 runtime schedule proof requires s2Lite"
            })
          }

          const counters: StepCounters = { scheduled: 0 }
          const workflow = makeScheduledWorkflow(counters)
          const config = {
            namespace: `runtime-schedule-${trialId}`,
            s2Endpoint
          }
          const scheduleId = "every-second"
          const workflowId = "scheduled-workflow"
          const fireAt = 5_000
          const runId = `${workflowId}:${scheduleId}:${fireAt}`
          const workflows = {
            [workflowId]: {
              load: async () => workflow,
              schedules: [
                {
                  enabled: true,
                  id: scheduleId,
                  input: { source: "schedule" },
                  overlapPolicy: "skip" as const,
                  schedule: every.seconds(1)
                }
              ]
            }
          }

          const materializerRuntime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows
          })

          const materialized = yield* promise(() => materializeWorkflowSchedules(materializerRuntime, { now: fireAt }))

          const runtimeA = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows
          })
          const runtimeB = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows
          })

          const [sweepA, sweepB] = yield* promise(() =>
            Promise.all([
              runtimeA.sweep({
                leaseMs: 1_000,
                leaseOwner: "schedule-sweep-a",
                maxScheduledRuns: 1,
                maxTimers: 0,
                now: fireAt
              }),
              runtimeB.sweep({
                leaseMs: 1_000,
                leaseOwner: "schedule-sweep-b",
                maxScheduledRuns: 1,
                maxTimers: 0,
                now: fireAt
              })
            ])
          )

          const store = s2WorkflowExecutionStore(config)
          const loaded = yield* promise(() => store.loadExecution(runId))
          const runs = yield* promise(() => store.listRuns({ limit: 10, workflowId }))
          const scheduledResults = [...sweepA.scheduled, ...sweepB.scheduled]

          return {
            completedCount: scheduledResults.filter((result) => result.kind === "completed").length,
            eventTypes: loaded?.events.map((event) => event.eventType),
            materializedKinds: materialized.map((item) => item.kind),
            output: loaded?.run.output,
            runCount: runs.length,
            runStatus: loaded?.run.status,
            scheduledCalls: counters.scheduled,
            scheduledKinds: scheduledResults.map((result) => result.kind)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          completedCount: 1,
          eventTypes: ["STEP_FINISHED", "RUN_FINISHED"],
          materializedKinds: ["materialized"],
          output: {
            completed: true,
            source: "schedule"
          },
          runCount: 1,
          runStatus: "finished",
          scheduledCalls: 1,
          scheduledKinds: ["completed"]
        }),
        traceSql(
          "runtime-schedule-sweep-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "runtime-schedule-sweep-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
