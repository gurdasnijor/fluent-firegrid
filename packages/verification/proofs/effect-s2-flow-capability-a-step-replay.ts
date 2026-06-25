import { greeter } from "effect-s2-flow/examples/greeter"
import { client, FlowRuntime } from "effect-s2-flow"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

export default proof("effect-s2-flow.capability-a.step-replay")
  .describedAs(
    "Proves durable function execution: a two-step handler survives kill -9 after step 1 is durably acknowledged without re-running step 1."
  )
  .spec(({ property }) =>
    property("capability-a.effect-s2-flow.step-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .host(
        "worker",
        effectS2FlowHost()
      )
      .workload(({ hosts, s2Endpoint }) =>
        Effect.gen(function*() {
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "Capability A step-replay proof requires s2Lite"
            })
          }
          const pending = yield* Effect.forkDetach(
            client(greeter, { invocationId: "capability-a-step-replay" }).process({ name: "Ada" }).pipe(
              Effect.provide(FlowRuntime.layer({ s2Endpoint }))
            )
          )

          yield* hosts.killAfterSpan("worker", {
            span: "effect-s2-flow.journal.append.ack",
            attributes: {
              "effect-s2-flow.record.type": "StepCompleted",
              "effect-s2-flow.step.name": "step-1"
            }
          })

          yield* hosts.restart("worker")
          return yield* Fiber.join(pending)
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({ greeting: "Hello, Ada!" }),
        traceSql(
          "step-1-side-effect-once",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.example.side-effect'
            AND SpanAttributes['effect-s2-flow.step.name'] = 'step-1'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "step-1-durable-ack-before-kill",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.journal.append.ack'
            AND SpanAttributes['effect-s2-flow.record.type'] = 'StepCompleted'
            AND SpanAttributes['effect-s2-flow.step.name'] = 'step-1'
          ) = 1
          AND countIf(
            SpanName = 'verification.host.kill'
            AND SpanAttributes['firegrid.host.id'] = 'worker'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "restart-folded-real-journal",
          `
          SELECT countIf(SpanName = 'effect-s2-flow.owner.rehydrate') >= 1
            AND countIf(SpanName = 'effect-s2.read') >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
