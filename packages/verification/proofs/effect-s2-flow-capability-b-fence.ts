import { counter } from "effect-s2-flow/examples/counter"
import { client, FlowRuntime } from "effect-s2-flow"
import { AppendInput, AppendRecord, FencingTokenMismatchError } from "effect-s2"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

export default proof("effect-s2-flow.capability-b.fenced-state")
  .describedAs(
    "Proves the first Capability B fence slice: object streams install a real S2 fence and stale token writes are rejected."
  )
  .spec(({ property }) =>
    property("capability-b.effect-s2-flow.fenced-state-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({
        "owner-a": effectS2FlowHost()
      })
      .workload(({ runtime, s2, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "Capability B fenced-owners proof requires s2Lite"
            })
          }
          const flowRuntime = FlowRuntime.layer({ s2Endpoint })

          const add5Fiber = yield* client(counter, "fenced-user", { invocationId: "counter-fence-add-5" }).add({
            amount: 5,
            delay: "750 millis"
          }).pipe(
            Effect.provide(flowRuntime),
            Effect.forkChild
          )
          yield* runtime.waitForSpan("effect-s2-flow.fence.claim", {
            attributes: { "effect-s2-flow.invocation.stream": "counter.object.fenced-user" }
          })
          const objectStream = yield* s2.stream({
            basin: "effect-s2-flow",
            stream: "counter.object.fenced-user"
          })
          const staleTokenError = yield* objectStream.append(
            AppendInput.create(
              [AppendRecord.string({ body: "stale-owner-write" })],
              { fencingToken: "not-current-owner" }
            )
          ).pipe(
            Effect.flip,
            Effect.filterOrFail(
              (error): error is FencingTokenMismatchError => error instanceof FencingTokenMismatchError,
              (error) => error
            )
          )
          const add5 = yield* Fiber.join(add5Fiber)
          const add7 = yield* client(counter, "fenced-user", { invocationId: "counter-fence-add-7" }).add({ amount: 7 })
            .pipe(
              Effect.provide(flowRuntime)
            )

          const finalValue = yield* client(counter, "fenced-user", { invocationId: "counter-fence-value" }).value({})
            .pipe(
              Effect.provide(flowRuntime)
            )

          return {
            addResults: [add5, add7],
            finalValue,
            staleTokenRejected: staleTokenError.expectedFencingToken !== ""
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          addResults: [5, 12],
          finalValue: 12,
          staleTokenRejected: true
        }),
        traceSql(
          "stale-owner-write-rejected-by-s2-fence",
          `
          SELECT countIf(
            SpanName = 'effect-s2.append'
            AND SpanAttributes['s2.operation.status'] = 'error'
            AND SpanAttributes['s2.error.kind'] = 'FencingTokenMismatchError'
            AND SpanAttributes['s2.error.code'] = 'APPEND_CONDITION_FAILED'
            AND SpanAttributes['s2.error.status'] = '412'
          ) >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
