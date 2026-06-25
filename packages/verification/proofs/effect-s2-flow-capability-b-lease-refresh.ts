import { client, FlowRuntime } from "effect-s2-flow"
import { counter } from "effect-s2-flow/examples/counter"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

const key = "lease-refresh-user"
const stream = `counter.object.${key}`

export default proof("effect-s2-flow.capability-b.lease-refresh")
  .describedAs(
    "Proves a live object owner refreshes its S2 fence while processing longer than the lease, so a second host backs off instead of stealing the object."
  )
  .spec(({ property }) =>
    property("capability-b.effect-s2-flow.lease-refresh-proof")
      .s2Lite({ persistence: "local-root" })
      .host("owner-a", effectS2FlowHost())
      .host("owner-b", effectS2FlowHost())
      .workload(({ hosts, runtime, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "lease-refresh proof cannot run without an s2Lite endpoint"
            })
          }
          const waitForObjectSpan = (span: string) =>
            runtime.waitForSpan(span, {
              attributes: { "effect-s2-flow.invocation.stream": stream },
              attempts: 800
            })

          yield* hosts.kill("owner-a")
          yield* hosts.kill("owner-b")

          const addFiber = yield* Effect.forkDetach(
            client(counter, key, { invocationId: "counter-lease-refresh-add" }).add({
              amount: 5,
              delay: "1500 millis"
            }).pipe(
              Effect.provide(FlowRuntime.layer({ s2Endpoint }))
            )
          )
          yield* runtime.waitForSpan("effect-s2-flow.client.invoke", {
            attributes: { "effect-s2-flow.request.id": "counter-lease-refresh-add" }
          })

          yield* hosts.restart("owner-a")
          yield* waitForObjectSpan("effect-s2-flow.fence.claim")
          yield* waitForObjectSpan("effect-s2-flow.fence.refresh")

          yield* hosts.restart("owner-b")
          yield* waitForObjectSpan("effect-s2-flow.fence.busy")

          const addResult = yield* Fiber.join(addFiber)
          const finalValue = yield* client(counter, key, { invocationId: "counter-lease-refresh-value" }).value({})
            .pipe(
              Effect.provide(FlowRuntime.layer({ s2Endpoint }))
            )
          return {
            addResult,
            finalValue
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          addResult: 5,
          finalValue: 5
        }),
        traceSql(
          "live-owner-refreshed-lease",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.fence.refresh'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.lease-refresh-user'
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "successor-backed-off-from-refreshed-lease",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.fence.busy'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.lease-refresh-user'
            AND SpanAttributes['effect-s2-flow.fencing.expected_token'] != ''
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "successor-did-not-claim-during-live-owner-work",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.fence.claim'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.lease-refresh-user'
            AND ResourceAttributes['firegrid.host.id'] = 'owner-b'
          ) = 0
          OR (
            countIf(
            SpanName = 'effect-s2-flow.invocation.completed'
            AND SpanAttributes['effect-s2-flow.request.id'] = 'counter-lease-refresh-add'
            ) = 1
            AND countIf(
              SpanName = 'effect-s2-flow.fence.claim'
              AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.lease-refresh-user'
              AND ResourceAttributes['firegrid.host.id'] = 'owner-b'
              AND Timestamp < (
                SELECT min(Timestamp)
                FROM trial_spans
                WHERE SpanName = 'effect-s2-flow.invocation.completed'
                  AND SpanAttributes['effect-s2-flow.request.id'] = 'counter-lease-refresh-add'
              )
            ) = 0
          ) AS ok
          FROM trial_spans
        `
        )
      ])
  )
