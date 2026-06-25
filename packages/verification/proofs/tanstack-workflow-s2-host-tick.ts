import { createWorkflow } from "@tanstack/workflow-core"
import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import { every } from "@tanstack/workflow-runtime"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

interface StepCounters {
  scheduled: number
  recovered: number
}

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow S2 host promise failed" })
  })

const makeScheduledWorkflow = (counters: StepCounters) =>
  createWorkflow({
    id: "host-scheduled-workflow"
  }).handler(async (ctx) => {
    await ctx.step("scheduled-step", async () => {
      counters.scheduled += 1
      return { scheduled: true }
    })
    return { source: "schedule" }
  })

const makeRecoveredWorkflow = (counters: StepCounters) =>
  createWorkflow({
    id: "host-recovered-workflow"
  }).handler(async (ctx) => {
    await ctx.step("recovered-step", async () => {
      counters.recovered += 1
      return { recovered: true }
    })
    return { source: (ctx.input as { readonly source: string }).source }
  })

export default proof("tanstack-workflow-s2.host-tick")
  .describedAs(
    "Proves the exported S2 workflow host surface materializes schedules, sweeps due work, and recovers stale S2-leased runs through the TanStack runtime."
  )
  .spec(({ property, trialId }) =>
    property("tanstack-workflow-s2.host-tick-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "tanstack-workflow-s2 host proof requires s2Lite" })
          }

          const counters: StepCounters = { recovered: 0, scheduled: 0 }
          const scheduleId = "host-schedule"
          const scheduledWorkflow = makeScheduledWorkflow(counters)
          const recoveredWorkflow = makeRecoveredWorkflow(counters)
          const host = createS2WorkflowRuntimeHost({
            namespace: `host-tick-${trialId}`,
            s2Endpoint,
            workflows: {
              "host-recovered-workflow": {
                load: async () => recoveredWorkflow
              },
              "host-scheduled-workflow": {
                load: async () => scheduledWorkflow,
                schedules: [
                  {
                    enabled: true,
                    id: scheduleId,
                    input: {},
                    overlapPolicy: "skip" as const,
                    schedule: every.seconds(1)
                  }
                ]
              }
            }
          })

          yield* promise(() =>
            host.store.createRun({
              input: { source: "stale" },
              now: 1_000,
              runId: "host-recovered:run-1",
              workflowId: "host-recovered-workflow"
            })
          )
          yield* promise(() =>
            host.store.claimRun({
              leaseMs: 100,
              leaseOwner: "crashed-owner",
              now: 1_000,
              runId: "host-recovered:run-1"
            })
          )

          const tick = yield* promise(() =>
            host.tick({
              leaseMs: 1_000,
              leaseOwner: "host-tick",
              maxScheduledRuns: 10,
              maxTimers: 10,
              now: 5_000,
              staleRunLimit: 10
            })
          )

          const scheduledRunId = `host-scheduled-workflow:${scheduleId}:5000`
          const scheduledExecution = yield* promise(() => host.store.loadExecution(scheduledRunId))
          const recoveredExecution = yield* promise(() => host.store.loadExecution("host-recovered:run-1"))

          return {
            materializedKinds: tick.materialized.map((item) => item.kind),
            recoveredKinds: tick.recovered.runs.map((run) => run.kind),
            recoveredOutput: recoveredExecution?.run.output,
            recoveredStepCalls: counters.recovered,
            scheduledKinds: tick.sweep.scheduled.map((run) => run.kind),
            scheduledOutput: scheduledExecution?.run.output,
            scheduledStepCalls: counters.scheduled
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          materializedKinds: ["materialized"],
          recoveredKinds: ["running"],
          recoveredOutput: undefined,
          recoveredStepCalls: 0,
          scheduledKinds: ["completed"],
          scheduledOutput: {
            source: "schedule"
          },
          scheduledStepCalls: 1
        }),
        traceSql(
          "host-tick-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "host-tick-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
