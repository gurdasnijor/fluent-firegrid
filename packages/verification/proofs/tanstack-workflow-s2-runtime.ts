import { createWorkflow } from "@tanstack/workflow-core"
import { defineWorkflowRuntime } from "@tanstack/workflow-runtime"
import { s2WorkflowExecutionStore } from "@firegrid/tanstack-workflow-s2"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

interface PaymentReceived {
  readonly paymentId: string
}

interface StepCounters {
  reserve: number
  ship: number
}

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "TanStack Workflow runtime promise failed" })
  })

const makeFulfillmentWorkflow = (counters: StepCounters) =>
  createWorkflow({
    id: "fulfillment"
  }).handler(async (ctx) => {
    await ctx.step("reserve-inventory", async () => {
      counters.reserve += 1
      return { reserved: true }
    })

    const payment = await ctx.waitForEvent<PaymentReceived>("payment-received")

    await ctx.step("ship-order", async () => {
      counters.ship += 1
      return { paymentId: payment.paymentId, shipped: true }
    })

    return {
      orderId: (ctx.input as { readonly orderId: string }).orderId,
      paymentId: payment.paymentId,
      shipped: true
    }
  })

export default proof("tanstack-workflow-s2.runtime-end-to-end")
  .describedAs(
    "Proves the S2 store backs the real TanStack Workflow runtime: startRun pauses on waitForEvent, a recreated runtime resumes via deliverSignal, and completed steps are not re-executed."
  )
  .spec(({ property, trialId }) =>
    property("tanstack-workflow-s2.runtime-end-to-end-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "tanstack-workflow-s2 runtime proof requires s2Lite" })
          }

          const counters: StepCounters = { reserve: 0, ship: 0 }
          const workflow = makeFulfillmentWorkflow(counters)
          const config = {
            namespace: `runtime-${trialId}`,
            s2Endpoint
          }
          const runId = "fulfillment:order-1"

          const runtime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows: {
              fulfillment: {
                load: async () => workflow
              }
            }
          })

          const started = yield* promise(() =>
            runtime.startRun({
              input: { orderId: "order-1" },
              now: 1_000,
              runId,
              workflowId: "fulfillment"
            })
          )

          const restartedRuntime = defineWorkflowRuntime({
            store: s2WorkflowExecutionStore(config),
            workflows: {
              fulfillment: {
                load: async () => workflow
              }
            }
          })

          const completed = yield* promise(() =>
            restartedRuntime.deliverSignal({
              name: "payment-received",
              now: 2_000,
              payload: { paymentId: "pay-1" },
              runId,
              signalId: "stripe:event-1"
            })
          )

          const duplicate = yield* promise(() =>
            restartedRuntime.deliverSignal({
              name: "payment-received",
              now: 2_100,
              payload: { paymentId: "pay-1" },
              runId,
              signalId: "stripe:event-1"
            })
          )

          const loaded = yield* promise(() => s2WorkflowExecutionStore(config).loadExecution(runId))

          return {
            completedKind: completed.kind,
            duplicateKind: duplicate.kind,
            eventTypes: loaded?.events.map((event) => event.eventType),
            output: completed.run?.output,
            reserveCalls: counters.reserve,
            shipCalls: counters.ship,
            startKind: started.kind,
            startWaitingFor: started.run?.waitingFor?.signalName
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          completedKind: "completed",
          duplicateKind: "duplicate",
          eventTypes: [
            "STEP_FINISHED",
            "SIGNAL_AWAITED",
            "SIGNAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          output: {
            orderId: "order-1",
            paymentId: "pay-1",
            shipped: true
          },
          reserveCalls: 1,
          shipCalls: 1,
          startKind: "paused",
          startWaitingFor: "payment-received"
        }),
        traceSql(
          "runtime-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "runtime-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
