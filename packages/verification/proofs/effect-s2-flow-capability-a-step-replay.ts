import { client, run, service } from "effect-s2-flow"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"

import { processHost } from "../src/ProcessHost.ts"
import { proof } from "../src/Proof.ts"

const greeter = service({
  name: "greeter",
  handlers: {
    *process(input: { readonly name: string }) {
      const greeting = yield* run(
        "step-1",
        Effect.sync(() => `Hello, ${input.name}`)
      )
      return yield* run(
        "step-2",
        Effect.succeed({ greeting: `${greeting}!` })
      )
    }
  }
})

/**
 * PDD forcing proof for Capability A.
 *
 * This proof is intentionally not registered in `proofs/main.ts` yet. It is the
 * target contract for the next runtime slice: when run directly through
 * `runProof`, it should stay red until `effect-s2-flow` provides a real host,
 * client, durable `run`, journal append, checkpoint, and replay path.
 */
export default proof("effect-s2-flow.capability-a.step-replay")
  .describedAs(
    "Proves durable function execution: a two-step handler survives kill -9 after step 1 is durably acknowledged without re-running step 1."
  )
  .spec(({ property }) =>
    property("capability-a.effect-s2-flow.step-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .host(
        "worker",
        processHost({
          command: "pnpm",
          args: [
            "--filter",
            "effect-s2-flow",
            "host"
          ]
        })
      )
      .workload(({ hosts }) =>
        Effect.gen(function*() {
          const pending = yield* Effect.forkDetach(
            client(greeter).process({ name: "Ada" })
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
            AND countIf(SpanName = 'effect-s2.read-session') >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  )
