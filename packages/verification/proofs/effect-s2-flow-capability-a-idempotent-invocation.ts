import { greeter } from "effect-s2-flow/examples/greeter"
import { attach, FlowRuntime, sendClient } from "effect-s2-flow"
import { decodeRecord, invocationStream } from "effect-s2-flow/invocation-journal"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

const invocationId = "capability-a-idempotent-invocation"
const streamName = invocationStream("greeter", invocationId)

export default proof("effect-s2-flow.capability-a.idempotent-invocation")
  .describedAs(
    "Proves explicit service invocation IDs are idempotent: retrying the same request attaches to the existing S2 journal and does not re-run completed durable steps."
  )
  .spec(({ property }) =>
    property("capability-a.effect-s2-flow.idempotent-invocation-proof")
      .s2Lite({ persistence: "local-root" })
      .host("worker", effectS2FlowHost())
      .workload(({ s2, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "Capability A idempotent-invocation proof requires s2Lite"
            })
          }
          const flowRuntime = FlowRuntime.layer({ s2Endpoint })
          const request = { name: "Ada" }
          const firstHandle = yield* sendClient(greeter, { invocationId }).process(request).pipe(
            Effect.provide(flowRuntime)
          )
          const first = yield* attach(firstHandle).pipe(
            Effect.provide(flowRuntime)
          )
          const secondHandle = yield* sendClient(greeter, { invocationId }).process(request).pipe(
            Effect.provide(flowRuntime)
          )
          const second = yield* attach(secondHandle).pipe(
            Effect.provide(flowRuntime)
          )
          const stream = yield* s2.stream({
            basin: "effect-s2-flow",
            stream: streamName
          })
          const tail = yield* stream.checkTail()
          const batch = yield* stream.read({
            start: { from: { seqNum: 0 } },
            stop: { limits: { count: tail.tail.seqNum } }
          })
          const records = batch.records.map((record) => decodeRecord(record.body))
          return {
            completedCount: records.filter((record) => record._tag === "Completed").length,
            first,
            invokeCount: records.filter((record) => record._tag === "Invoke").length,
            second
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          completedCount: 1,
          first: { greeting: "Hello, Ada!" },
          invokeCount: 1,
          second: { greeting: "Hello, Ada!" }
        }),
        traceSql(
          "retry-did-not-rerun-step-1",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.example.side-effect'
            AND SpanAttributes['effect-s2-flow.step.name'] = 'step-1'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "client-observed-two-submissions",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.client.invoke'
            AND SpanAttributes['effect-s2-flow.request.id'] = '${invocationId}'
          ) = 2 AS ok
          FROM trial_spans
        `
        )
      ])
  )
