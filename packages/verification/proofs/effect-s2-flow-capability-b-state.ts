import { counter } from "effect-s2-flow/examples/counter"
import { client, FlowRuntime } from "effect-s2-flow"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

export default proof("effect-s2-flow.capability-b.durable-state")
  .describedAs(
    "Proves the first Capability B flow slice: object state is journaled to S2, read-your-writes holds inside a handler, and a fresh process folds the state back."
  )
  .spec(({ property }) =>
    property("capability-b.effect-s2-flow.durable-state-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({
        "state-worker": effectS2FlowHost()
      })
      .workload(({ hosts, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "Capability B durable-state proof requires s2Lite"
            })
          }
          const runCounter = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
            effect.pipe(Effect.provide(FlowRuntime.layer({ s2Endpoint })))

          const addResult = yield* runCounter(
            client(counter, "user-1", { invocationId: "counter-add-5" }).add({ amount: 5 })
          )

          yield* hosts.restart("state-worker")

          const valueAfterRestart = yield* runCounter(
            client(counter, "user-1", { invocationId: "counter-value-after-restart" }).value({})
          )
          const readYourWrites = yield* runCounter(
            client(counter, "user-1", { invocationId: "counter-add-then-read" }).addThenRead({ amount: 7 })
          )

          return {
            addResult,
            readYourWrites,
            valueAfterRestart
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          addResult: 5,
          readYourWrites: {
            after: 12,
            before: 5
          },
          valueAfterRestart: 5
        }),
        traceSql(
          "state-records-committed",
          `
          SELECT countIf(
            SpanName = 'effect-s2.append'
            AND SpanAttributes['s2.stream'] = 'counter.object.user-1'
            AND SpanAttributes['s2.append.record_count'] = '2'
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "fresh-process-folded-object-state",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.owner.rehydrate'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.user-1'
          ) >= 3
          AND countIf(SpanName = 'verification.host.restart') = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "state-used-production-s2",
          `
          SELECT countIf(SpanName = 'effect-s2.read') >= 3
            AND countIf(SpanName = 'effect-s2.append') >= 6 AS ok
          FROM trial_spans
        `
        )
      ])
  )
