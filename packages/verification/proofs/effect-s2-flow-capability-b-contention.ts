import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { client, FlowRuntime } from "effect-s2-flow"
import { counter } from "effect-s2-flow/examples/counter"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

export default proof("effect-s2-flow.capability-b.owner-contention")
  .describedAs(
    "Proves Capability B contention: two hosts race the same object key on real S2, the active fence admits one owner, the other backs off, and the counter has no lost update."
  )
  .spec(({ property }) =>
    property("capability-b.effect-s2-flow.owner-contention-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({
        "owner-a": effectS2FlowHost(),
        "owner-b": effectS2FlowHost()
      })
      .workload(({ hosts, runtime, s2Endpoint }) =>
        Effect.gen(function*() {
          const endpoint = s2Endpoint ?? (yield* new VerificationError({
            message: "owner-contention proof cannot run without an s2Lite endpoint"
          }))
          const flowRuntime = FlowRuntime.layer({ s2Endpoint: endpoint })
          const runCounter = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.provide(flowRuntime))

          yield* Effect.all([
            hosts.kill("owner-a"),
            hosts.kill("owner-b")
          ], { concurrency: "unbounded" })

          const add5Fiber = yield* Effect.forkDetach(
            runCounter(
              client(counter, "contended-user", { invocationId: "counter-contended-add-5" }).add({
                amount: 5,
                delay: "1500 millis"
              })
            )
          )
          const add7Fiber = yield* Effect.forkDetach(
            runCounter(
              client(counter, "contended-user", { invocationId: "counter-contended-add-7" }).add({
                amount: 7,
                delay: "1500 millis"
              })
            )
          )
          yield* runtime.waitForSpan("effect-s2-flow.client.invoke", {
            attributes: { "effect-s2-flow.request.id": "counter-contended-add-5" }
          })
          yield* runtime.waitForSpan("effect-s2-flow.client.invoke", {
            attributes: { "effect-s2-flow.request.id": "counter-contended-add-7" }
          })

          yield* hosts.restart("owner-a")
          yield* runtime.waitForSpan("effect-s2-flow.fence.claim", {
            attributes: { "effect-s2-flow.invocation.stream": "counter.object.contended-user" }
          })
          yield* hosts.restart("owner-b")
          yield* runtime.waitForSpan("effect-s2-flow.fence.busy", {
            attributes: { "effect-s2-flow.invocation.stream": "counter.object.contended-user" },
            attempts: 400
          })

          const [add5, add7] = yield* Effect.all([
            Fiber.join(add5Fiber),
            Fiber.join(add7Fiber)
          ], { concurrency: "unbounded" })

          const finalValue = yield* runCounter(
            client(counter, "contended-user", { invocationId: "counter-contended-value" }).value({})
          )

          return {
            finalValue,
            serializableResults: (add5 === 5 && add7 === 12) || (add5 === 12 && add7 === 7)
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          finalValue: 12,
          serializableResults: true
        }),
        traceSql(
          "one-owner-claimed-active-fence",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.fence.claim'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.contended-user'
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "foreign-owner-backed-off",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.fence.busy'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.contended-user'
            AND SpanAttributes['effect-s2-flow.fencing.expected_token'] != ''
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "two-hosts-participated-in-contention",
          `
          SELECT uniqExact(ResourceAttributes['firegrid.host.id']) >= 2 AS ok
          FROM trial_spans
          WHERE SpanName IN ('effect-s2-flow.fence.claim', 'effect-s2-flow.fence.busy')
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.contended-user'
        `
        )
      ])
  )
