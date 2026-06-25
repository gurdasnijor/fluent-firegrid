import * as Effect from "effect/Effect"
import { join as joinFiber } from "effect/Fiber"
import { client, FlowRuntime } from "effect-s2-flow"
import { counter } from "effect-s2-flow/examples/counter"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

const requireEndpoint = (endpoint: string | undefined) =>
  endpoint === undefined
    ? new VerificationError({
      message: "lease-expiry proof cannot run without an s2Lite endpoint"
    })
    : Effect.succeed(endpoint)

export default proof("effect-s2-flow.capability-b.lease-expiry")
  .describedAs(
    "Proves Capability B lease expiry: a successor host initially backs off from a live foreign fence, then claims the expired fence and completes the pending object invocation."
  )
  .spec(({ property }) =>
    property("capability-b.effect-s2-flow.lease-expiry-proof")
      .s2Lite({ persistence: "local-root" })
      .host("owner-a", effectS2FlowHost())
      .host("owner-b", effectS2FlowHost())
      .workload(({ hosts, runtime, s2, s2Endpoint }) =>
        Effect.gen(function*() {
          const endpoint = yield* requireEndpoint(s2Endpoint)
          const flowRuntime = FlowRuntime.layer({ s2Endpoint: endpoint })
          const runCounter = <A, E, R>(effect: Effect.Effect<A, E, R>) => effect.pipe(Effect.provide(flowRuntime))
          const key = "lease-expiry-user"
          const stream = "counter.object.lease-expiry-user"

          yield* Effect.all([
            hosts.kill("owner-a"),
            hosts.kill("owner-b")
          ], { concurrency: "unbounded" })

          const addFiber = yield* Effect.forkDetach(
            runCounter(
              client(counter, key, { invocationId: "counter-lease-expiry-add" }).add({
                amount: 5,
                delay: "12 seconds"
              })
            )
          )
          yield* runtime.waitForSpan("effect-s2-flow.client.invoke", {
            attributes: { "effect-s2-flow.request.id": "counter-lease-expiry-add" }
          })

          yield* hosts.restart("owner-a")
          yield* runtime.waitForSpan("effect-s2-flow.fence.claim", {
            attributes: { "effect-s2-flow.invocation.stream": stream }
          })

          yield* hosts.restart("owner-b")
          yield* runtime.waitForSpan("effect-s2-flow.fence.busy", {
            attributes: { "effect-s2-flow.invocation.stream": stream },
            attempts: 400
          })
          yield* hosts.kill("owner-a")

          const addResult = yield* joinFiber(addFiber)
          const finalValue = yield* runCounter(
            client(counter, key, { invocationId: "counter-lease-expiry-value" }).value({})
          )
          const objectStream = yield* s2.stream({
            basin: "effect-s2-flow",
            stream
          })
          const tail = yield* objectStream.checkTail().pipe(
            Effect.mapError((cause) =>
              new VerificationError({
                message: "failed to check lease-expiry object stream tail",
                cause
              })
            )
          )
          const batch = yield* objectStream.read({
            start: { from: { seqNum: 0 } },
            stop: { limits: { count: tail.tail.seqNum } }
          }).pipe(
            Effect.mapError((cause) =>
              new VerificationError({
                message: "failed to read lease-expiry object stream",
                cause
              })
            )
          )
          const fenceOwners = batch.records
            .filter((record) =>
              record.headers.length === 1 && record.headers[0]?.[0] === "" && record.headers[0][1] === "fence"
            )
            .map((record) => record.body.split(":")[0] ?? "")

          return {
            addResult,
            finalValue,
            successorClaimedFence: fenceOwners.includes("owner-b")
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          addResult: 5,
          finalValue: 5,
          successorClaimedFence: true
        }),
        traceSql(
          "successor-backed-off-from-live-lease",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.fence.busy'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'counter.object.lease-expiry-user'
            AND SpanAttributes['effect-s2-flow.fencing.expected_token'] != ''
          ) >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "original-owner-was-killed",
          `
          SELECT countIf(
            SpanName = 'verification.host.kill'
            AND SpanAttributes['firegrid.host.id'] = 'owner-a'
          ) >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
