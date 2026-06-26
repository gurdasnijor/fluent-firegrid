import { createS2WorkflowRuntimeHost } from "@firegrid/tanstack-workflow-s2"
import {
  awakeable,
  bindFluentDefinitions,
  createTanStackExternalSignalBinding,
  resolveAwakeable,
  run,
  service,
  workflowIdForHandler
} from "@firegrid/fluent-firegrid"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"

interface Counters {
  readonly after: { value: number }
  readonly before: { value: number }
}

const promise = <A>(evaluate: () => Promise<A>): Effect.Effect<A, VerificationError> =>
  Effect.tryPromise({
    try: evaluate,
    catch: (cause) => new VerificationError({ cause, message: "fluent S2 awakeable promise failed" })
  })

const makeReviewService = (
  counters: Counters,
  tokenRef: { value?: string }
) =>
  service({
    name: "reviews",
    handlers: {
      *request(input: { readonly documentId: string }) {
        const review = yield* awakeable<string>({ name: "review" })
        yield* run(() => {
          counters.before.value += 1
          tokenRef.value = review.id
          return { notified: true }
        }, { name: "notify-reviewer" })

        const decision = yield* review.await

        yield* run(() => {
          counters.after.value += 1
          return { recorded: true }
        }, { name: "record-decision" })

        return {
          decision,
          documentId: input.documentId
        }
      }
    }
  })

export default proof("fluent-firegrid-s2.awakeable")
  .describedAs(
    "Proves fluent awakeables over the S2-backed runtime: a handler parks on an encoded token, a recreated host resolves that token via deliverSignal, and replay resumes without re-running pre-wait work."
  )
  .spec(({ property, trialId }) =>
    property("fluent-firegrid-s2.awakeable-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({ message: "fluent S2 awakeable proof requires s2Lite" })
          }

          const counters: Counters = {
            after: { value: 0 },
            before: { value: 0 }
          }
          const tokenRef: { value?: string } = {}
          const reviews = makeReviewService(counters, tokenRef)
          const config = {
            namespace: `fluent-awakeable-${trialId}`,
            s2Endpoint
          }
          const runId = "reviews:request:doc-1"
          let host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([reviews])
          })

          const started = yield* promise(() =>
            host.runtime.startRun({
              input: {
                input: { documentId: "doc-1" }
              },
              now: 1_000,
              runId,
              workflowId: workflowIdForHandler(reviews, "request")
            })
          )
          if (tokenRef.value === undefined) {
            return yield* new VerificationError({ message: "awakeable token was not produced before parking" })
          }

          host = createS2WorkflowRuntimeHost({
            ...config,
            workflows: bindFluentDefinitions([reviews])
          })

          const resolved = yield* resolveAwakeable(
            createTanStackExternalSignalBinding(host, { now: () => 2_000 }),
            tokenRef.value,
            "approved"
          ).pipe(
            Effect.mapError((cause) =>
              cause instanceof VerificationError
                ? cause
                : new VerificationError({ cause, message: "fluent S2 awakeable resolve failed" })
            )
          )
          const duplicate = yield* resolveAwakeable(
            createTanStackExternalSignalBinding(host, { now: () => 3_000 }),
            tokenRef.value,
            "approved"
          ).pipe(
            Effect.mapError((cause) =>
              cause instanceof VerificationError
                ? cause
                : new VerificationError({ cause, message: "fluent S2 awakeable duplicate resolve failed" })
            )
          )

          const loaded = yield* promise(() => host.store.loadExecution(runId))

          return {
            afterCalls: counters.after.value,
            beforeCalls: counters.before.value,
            duplicateKind: duplicate.kind,
            eventTypes: loaded?.events.map((event) => event.eventType),
            output: loaded?.run.output,
            resolvedKind: resolved.kind,
            startKind: started.kind,
            startWaitingFor: started.run?.waitingFor?.signalName,
            tokenPrefix: tokenRef.value.slice(0, "ffg_awakeable:".length)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          afterCalls: 1,
          beforeCalls: 1,
          duplicateKind: "duplicate",
          eventTypes: [
            "STEP_FINISHED",
            "SIGNAL_AWAITED",
            "SIGNAL_RESOLVED",
            "STEP_FINISHED",
            "RUN_FINISHED"
          ],
          output: {
            decision: "approved",
            documentId: "doc-1"
          },
          resolvedKind: "completed",
          startKind: "paused",
          startWaitingFor: "__firegrid_awakeable:reviews:request:doc-1:signal:0:awakeable:review",
          tokenPrefix: "ffg_awakeable:"
        }),
        traceSql(
          "fluent-awakeable-used-s2-http",
          `
          SELECT countIf(SpanName = 'http.client GET') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fluent-awakeable-supervised-s2-lite",
          `
          SELECT countIf(SpanName = 'S2LiteSupervisor.spawn') = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
