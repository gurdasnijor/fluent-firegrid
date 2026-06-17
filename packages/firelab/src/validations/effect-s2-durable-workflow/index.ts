import { Duration, Effect, Schema } from "effect"
import { resolveSignal, run, signal, workflow, workflowAttach, workflowRunId, workflowSubmit } from "effect-s2-durable"
import { serviceLayer } from "effect-s2-durable"
import { assertEquals } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

// A workflow is an OBJECT SPECIALIZATION, not a third runtime: its `run` is an exclusive
// handler admitted AT MOST ONCE per workflow id. This proves the load-bearing run-once
// mechanic VERTICALLY over one s2 lite backend, through the public workflow surface
// (workflowSubmit / workflowAttach / workflowRunId + residency-independent resolveSignal):
// a duplicate start returns "alreadyStarted" (NOT a deduped second run), the run body
// executes exactly once, the long-running run parks on a durable promise across an engine
// restart, and a fresh engine recovers + resumes it via ingress (the SDD's long-running
// workflow scenario with restart and signal ingress).

// real executions of the run body's durable step — a duplicate start or a restart replay
// must NEVER execute it twice.
const runExecutions = { count: 0 }
// test-side marker: the run records its value AFTER the `run` step returns (so the
// Journaled run fact is durable) and BEFORE it parks on the promise. A driver waits for
// this so the duplicate-start / teardown genuinely race a parked run, not a bare admission.
const reachedPark = new Set<string>()

const approval = workflow({
  name: "firelab-wf-approval",
  *run(amount: number) {
    const n = yield* run(Effect.sync(() => (runExecutions.count++, amount)), { output: Schema.Number })
    reachedPark.add(`${amount}`) // run fact is durable; about to park on the promise
    const ok = yield* signal("approved", Schema.Boolean) // park until ingress resolves it
    return ok ? n : 0
  },
  runSchema: { input: Schema.Number, output: Schema.Number },
})

// poll until the run body has passed its `run` step and is about to park (bounded by the
// firelab run timeout; if it never parks that is itself a failure).
const waitForPark = (amount: number): Effect.Effect<void> =>
  Effect.suspend((): Effect.Effect<void> =>
    reachedPark.has(`${amount}`)
      ? Effect.void
      : Effect.sleep(Duration.millis(10)).pipe(Effect.flatMap(() => waitForPark(amount)))
  )

export default defineValidation({
  id: "effect-s2-durable-workflow",
  description:
    "Proves workflow run-once admission as an object specialization over one s2 lite backend, through the "
    + "public workflowSubmit/workflowAttach/workflowRunId + resolveSignal path: a duplicate start returns "
    + "\"alreadyStarted\" (never a deduped second run), the run body executes exactly once, and a long-running "
    + "run parks on a durable promise across an engine restart — a fresh engine recovers and resumes it via "
    + "residency-independent ingress — with OTel evidence for admission, owner enumeration, drain, and boot-recovery.",
  feature: {
    product: "effect-s2-durable",
    name: "object-actor-model",
  },
  backend: S2LiteLive,
  component: ({ key }) => Effect.succeed({ key }),
  requirements: [
    {
      id: "WORKFLOW.1",
      description:
        "a workflow `run` is an exclusive handler admitted at most once per workflow id: a duplicate start "
        + "returns \"alreadyStarted\" (NOT a deduped second run) both while running and after an engine restart; "
        + "the run body executes EXACTLY ONCE; the parked run is boot-recovered by a fresh engine and resumed "
        + "by a residency-independent ingress signal, then attach returns its result",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.admit")) && spans.exists(s, named(s, "effect-s2-durable.object.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.object.ownerKeys")) && spans.exists(s, named(s, "effect-s2-durable.object.drain"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const id = key
          reachedPark.delete("5")
          const engine = serviceLayer(approval)
          const before = runExecutions.count
          // process 1: start the workflow; the run body EXECUTES its step here, then parks.
          // A DUPLICATE start while it is parked must return "alreadyStarted", never re-run.
          yield* Effect.gen(function*() {
            const first = yield* workflowSubmit(approval, id, 5)
            assertEquals(first, "started")
            yield* waitForPark(5) // run step executed + durable, run is parked on the promise
            const dup = yield* workflowSubmit(approval, id, 5)
            assertEquals(dup, "alreadyStarted") // run-once: not a deduped second run
          }).pipe(Effect.provide(engine), Effect.scoped)
          // process 2: a fresh engine boot-recovers the parked run. A duplicate start is STILL
          // "alreadyStarted"; a residency-independent ingress signal resolves the promise; attach
          // returns the decoded result.
          const result = yield* Effect.gen(function*() {
            const dup = yield* workflowSubmit(approval, id, 5)
            assertEquals(dup, "alreadyStarted") // already started across the restart, too
            const runId = yield* workflowRunId(approval, id)
            yield* resolveSignal(runId, "approved", Schema.Boolean, true)
            return yield* workflowAttach(approval, id)
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(result, 5) // the run resumed and returned its (replayed) value
          // executed EXACTLY once — across two duplicate starts AND an engine restart.
          assertEquals(runExecutions.count - before, 1)
        }),
    },
  ],
})
