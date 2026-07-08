import { createWorkflow, defineWorkflowRuntime } from "@firegrid/fluent/runtime"
import { s2WorkflowExecutionStore } from "@firegrid/fluent/s2"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

interface StepCounters {
  beforeApproval: number
  afterApproval: number
}

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow runtime approval promise failed" })
  })

const makeApprovalWorkflow = (counters: StepCounters) =>
  createWorkflow({
    id: "approval-workflow"
  }).handler(async (ctx) => {
    await ctx.step("before-approval", async () => {
      counters.beforeApproval += 1
      return { requested: true }
    })

    const approval = await ctx.approve({
      description: "Confirm the deployment",
      id: "deployment-approval",
      title: "Deploy?"
    })

    await ctx.step("after-approval", async () => {
      counters.afterApproval += 1
      return { approved: approval.approved }
    })

    return {
      approved: approval.approved,
      feedback: approval.feedback
    }
  })

export default proof("store.runtime-approval")
  .describedAs(
    "Proves the S2 store backs TanStack Workflow runtime approvals: startRun pauses on ctx.approve, a recreated runtime resumes via deliverApproval, and duplicate approval delivery is idempotent."
  )
  .spec(({ property, trialId }) =>
    property("store.runtime-approval-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "store runtime approval proof requires s2Lite"
            })
          }

          const counters: StepCounters = { beforeApproval: 0, afterApproval: 0 }
          const workflow = makeApprovalWorkflow(counters)
          const config = {
            namespace: `runtime-approval-${trialId}`,
            s2Endpoint
          }
          const runId = "approval:run-1"

          const runtime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows: {
              "approval-workflow": {
                load: async () => workflow
              }
            }
          })

          const started = yield* promise(() =>
            runtime.startRun({
              input: {},
              now: 1_000,
              runId,
              workflowId: "approval-workflow"
            })
          )

          const pendingApproval = started.run?.pendingApproval
          if (pendingApproval === undefined) {
            return yield* new VerificationError({ message: "approval workflow did not persist a pending approval" })
          }

          const restartedRuntime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows: {
              "approval-workflow": {
                load: async () => workflow
              }
            }
          })

          const approval = {
            approvalId: pendingApproval.approvalId,
            approved: true,
            feedback: "ship it"
          }

          const completed = yield* promise(() =>
            restartedRuntime.deliverApproval({
              approval,
              now: 2_000,
              runId
            })
          )

          const duplicate = yield* promise(() =>
            restartedRuntime.deliverApproval({
              approval,
              now: 2_100,
              runId
            })
          )

          const loaded = yield* promise(() => s2WorkflowExecutionStore(config).loadExecution(runId))

          return {
            afterApprovalCalls: counters.afterApproval,
            beforeApprovalCalls: counters.beforeApproval,
            completedKind: completed.kind,
            duplicateKind: duplicate.kind,
            eventTypes: loaded?.events.map((event) => event.eventType),
            output: completed.run?.output,
            pendingApprovalTitle: pendingApproval.title,
            startKind: started.kind,
            startPendingApprovalIdPresent: pendingApproval.approvalId.length > 0
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          afterApprovalCalls: 1,
          beforeApprovalCalls: 1,
          completedKind: "completed",
          duplicateKind: "duplicate",
          eventTypes: [
            "STEP_FINISHED",
            "APPROVAL_REQUESTED",
            "APPROVAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          output: {
            approved: true,
            feedback: "ship it"
          },
          pendingApprovalTitle: "Deploy?",
          startKind: "paused",
          startPendingApprovalIdPresent: true
        }),
        traceSql(
          "runtime-approval-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "runtime-approval-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
