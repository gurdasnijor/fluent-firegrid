import { attach, FlowRuntime, sendClient } from "effect-s2-flow"
import { sleeper } from "effect-s2-flow/examples/sleeper"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"
import { VerificationError } from "../src/VerificationError.ts"
import { effectS2FlowHost } from "./effect-s2-flow-host.ts"

export default proof("effect-s2-flow.capability-c.durable-sleep")
  .describedAs(
    "Proves durable sleep: a handler records a timer, survives host loss while parked, resumes after the timer fires, and completes exactly once."
  )
  .spec(({ property }) =>
    property("capability-c.effect-s2-flow.durable-sleep-proof")
      .s2Lite({ persistence: "local-root" })
      .hosts({ worker: effectS2FlowHost() })
      .workload((context) =>
        Effect.gen(function*() {
          const { hosts, runtime, s2Endpoint } = context
          if (s2Endpoint === undefined) {
            return yield* new VerificationError({
              message: "Capability C durable-sleep proof requires s2Lite"
            })
          }

          const handle = yield* sendClient(sleeper, { invocationId: "capability-c-durable-sleep" })
            .nap({ delay: "250 millis", name: "Ada" })
            .pipe(Effect.provide(FlowRuntime.layer({ s2Endpoint })))

          yield* hosts.killAfterSpan("worker", {
            span: "effect-s2-flow.timer.set",
            attributes: {
              "effect-s2-flow.request.id": "capability-c-durable-sleep",
              "effect-s2-flow.timer.name": "nap"
            }
          })

          yield* hosts.restart("worker")
          const result = yield* attach(handle).pipe(
            Effect.provide(FlowRuntime.layer({ s2Endpoint }))
          )
          yield* runtime.waitForSpan("effect-s2-flow.timer.fired", {
            attempts: 120,
            attributes: {
              "effect-s2-flow.request.id": "capability-c-durable-sleep",
              "effect-s2-flow.timer.name": "nap"
            },
            interval: "50 millis"
          })
          return result
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({ woke: "Ada" }),
        traceSql(
          "timer-set-and-fired-once",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.timer.set'
            AND SpanAttributes['effect-s2-flow.request.id'] = 'capability-c-durable-sleep'
            AND SpanAttributes['effect-s2-flow.timer.name'] = 'nap'
          ) = 1
          AND countIf(
            SpanName = 'effect-s2-flow.timer.fired'
            AND SpanAttributes['effect-s2-flow.request.id'] = 'capability-c-durable-sleep'
            AND SpanAttributes['effect-s2-flow.timer.name'] = 'nap'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "host-loss-after-durable-timer-set",
          `
          SELECT countIf(
            SpanName = 'verification.host.kill'
            AND SpanAttributes['firegrid.host.id'] = 'worker'
          ) = 1
          AND countIf(
            SpanName = 'verification.host.restart'
            AND SpanAttributes['firegrid.host.id'] = 'worker'
          ) = 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "resume-folded-timer-from-s2",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.owner.rehydrate'
            AND SpanAttributes['effect-s2-flow.invocation.stream'] = 'sleeper.invocation.capability-c-durable-sleep'
          ) >= 2
          AND countIf(SpanName = 'effect-s2.read') >= 1 AS ok
          FROM trial_spans
        `
        ),
        traceSql(
          "post-sleep-step-once",
          `
          SELECT countIf(
            SpanName = 'effect-s2-flow.journal.append.ack'
            AND SpanAttributes['effect-s2-flow.record.type'] = 'StepCompleted'
            AND SpanAttributes['effect-s2-flow.step.name'] = 'after-nap'
          ) = 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
