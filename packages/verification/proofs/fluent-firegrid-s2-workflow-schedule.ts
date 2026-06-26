import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import { bindFluentDefinitions, every, run, schedule, workflow, workflowIdForHandler } from "@firegrid/fluent-firegrid"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "fluent S2 workflow schedule promise failed" })
  })

const handlers = {
  *reconcile(input: { readonly tenantId: string }) {
    return yield* run(() => ({ ok: true, tenantId: input.tenantId }), { name: "reconcile" })
  }
}

const jobs = workflow({
  name: "scheduled-jobs",
  handlers,
  schedules: [
    schedule<typeof handlers, "reconcile">({
      handler: "reconcile",
      id: "minute-reconcile",
      input: { tenantId: "tenant-1" },
      overlapPolicy: "skip",
      schedule: every.minutes(1)
    })
  ]
})

export default proof("fluent-firegrid-s2.workflow-schedule")
  .describedAs(
    "Proves fluent workflow schedule metadata lowers into the S2-backed TanStack runtime: a handler-targeted schedule materializes, sweeps once, and starts the fluent generator handler with the scheduled input."
  )
  .spec(({ property, trialId }) =>
    property("fluent-firegrid-s2.workflow-schedule-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 workflow schedule proof requires s2Lite" })
          }
          const config = {
            namespace: `fluent-workflow-schedule-${trialId}`,
            s2Endpoint
          }
          const host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([jobs])
          })
          const workflowId = workflowIdForHandler(jobs, "reconcile")
          const scheduleId = `${workflowId}:minute-reconcile`
          const runId = `${workflowId}:${scheduleId}:60000`

          const tick = yield* promise(() =>
            host.tick({
              now: 60_000,
              recoverStaleRuns: false
            })
          )
          const loaded = yield* promise(() => host.store.loadExecution(runId))

          return {
            loadedOutput: loaded?.run.output,
            loadedStatus: loaded?.run.status,
            materialized: tick.materialized.map((entry) => ({
              fireAt: entry.kind === "materialized" ? entry.fireAt : undefined,
              kind: entry.kind,
              scheduleId: entry.scheduleId,
              workflowId: entry.workflowId
            })),
            scheduledKinds: tick.sweep.scheduled.map((result) => result.kind),
            scheduledOutputs: tick.sweep.scheduled.map((result) => result.run?.output)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          loadedOutput: { ok: true, tenantId: "tenant-1" },
          loadedStatus: "finished",
          materialized: [
            {
              fireAt: 60_000,
              kind: "materialized",
              scheduleId: "workflow:scheduled-jobs:reconcile:minute-reconcile",
              workflowId: "workflow:scheduled-jobs:reconcile"
            }
          ],
          scheduledKinds: ["completed"],
          scheduledOutputs: [{ ok: true, tenantId: "tenant-1" }]
        }),
        traceSql(
          "fluent-workflow-schedule-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-workflow-schedule-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
