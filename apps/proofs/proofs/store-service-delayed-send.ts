import { bindFluentDefinitions, run, sendServiceClient, service } from "@firegrid/fluent"
import { createS2ObjectRuntimeBinding, createS2WorkflowRuntimeHost } from "@firegrid/fluent/s2"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "fluent S2 delayed service promise failed" })
  })

const worker = service({
  name: "delayed-worker",
  handlers: {
    *double(input: { readonly value: number }) {
      return yield* run(() => input.value * 2, { name: "double" })
    }
  }
})

export default proof("store.service-delayed-send")
  .describedAs(
    "Proves S2-backed service delayed send: a delayed service send is durably admitted, does not create the workflow before notBefore, drains when due, and can be attached through the returned handle."
  )
  .spec(({ property, trialId }) =>
    property("store.service-delayed-send-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 service delayed-send proof requires s2Lite" })
          }
          let currentTime = 1_000
          const config = {
            namespace: `fluent-service-delayed-send-${trialId}`,
            s2Endpoint
          }
          const host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([worker])
          })
          const binding = createS2ObjectRuntimeBinding(host, {
            ...config,
            now: () => currentTime
          })
          const runId = "service:delayed-worker:double-delayed"

          const delayed = yield* sendServiceClient(binding, worker).double({ value: 21 }, {
            delay: { seconds: 5 },
            runId
          })
          const earlyDrain = yield* binding.drainDelayedStarts()
          const earlyLoaded = yield* promise(() => host.store.loadExecution(runId))

          currentTime = 7_000
          const dueDrain = yield* binding.drainDelayedStarts()
          const attached = yield* delayed.attach()
          const loaded = yield* promise(() => host.store.loadExecution(runId))

          return {
            attached,
            dueDrain,
            earlyDrain,
            earlyLoaded: earlyLoaded === undefined ? undefined : earlyLoaded.run.status,
            output: loaded?.run.output,
            reference: {
              handler: delayed.handler,
              invocationId: delayed.invocationId,
              kind: delayed.kind,
              name: delayed.name
            },
            status: loaded?.run.status
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          attached: 42,
          dueDrain: { started: 1 },
          earlyDrain: { started: 0 },
          earlyLoaded: undefined,
          output: 42,
          reference: {
            handler: "double",
            invocationId: "service:delayed-worker:double-delayed",
            kind: "service",
            name: "delayed-worker"
          },
          status: "finished"
        }),
        traceSql(
          "fluent-service-delayed-send-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-service-delayed-send-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
