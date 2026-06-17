import { Duration, Effect, Option, Schema } from "effect"
import {
  resolvePromise,
  resolveSignal,
  run,
  serviceLayer,
  sharedClient,
  signal,
  state,
  workflow,
  workflowAttach,
  workflowRunId,
  workflowSubmit,
} from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { assertEquals } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

// A workflow is an OBJECT SPECIALIZATION, not a third runtime: its `run` is an exclusive
// handler admitted AT MOST ONCE per workflow id, and its signal/query handlers are
// ordinary SHARED handlers over the run's owner projection. This proves, VERTICALLY over
// one s2 lite backend through the public workflow surface:
//  • WORKFLOW.1 run-once: a duplicate start returns "alreadyStarted" (never a deduped
//    second run) while running, after a restart, AND after completion; exactly-once exec.
//  • WORKFLOW.2 shared handlers: a query runs CONCURRENTLY over the snapshot while the run
//    is parked (not queued behind the exclusive run) and reads the run's committed state.
//  • WORKFLOW.3 promises resolved by shared handlers: the run parks on a durable promise;
//    a SHARED signal handler resolves it (an ingress SignalResolved append, not a
//    user-state write); the run resumes and attach returns the result.

// real executions of the run body's durable step — a duplicate start or a restart replay
// must NEVER execute it twice.
const runExecutions = { count: 0 }
// test-side marker: the run records its value AFTER the `run` step returns (so the
// Journaled run fact is durable) and BEFORE it parks on the promise. A driver waits for
// this so the duplicate-start / teardown genuinely race a parked run, not a bare admission.
const reachedPark = new Set<string>()

// the workflow's user state — the run writes it before parking; a shared query reads it.
class Amt extends Table<Amt>("amt")({ id: Schema.String.pipe(primaryKey), value: Schema.Number }) {}

const approval = workflow({
  name: "firelab-wf-approval",
  *run(amount: number) {
    yield* state(Amt).set({ id: "v", value: amount }) // committed user state a query can read
    const n = yield* run(Effect.sync(() => (runExecutions.count++, amount)), { output: Schema.Number })
    reachedPark.add(`${amount}`) // run fact is durable; about to park on the promise
    const ok = yield* signal("approved", Schema.Boolean) // park until the promise is resolved
    return ok ? n : 0
  },
  handlers: {
    // a SHARED query handler — read-only over the snapshot, concurrent with the parked run.
    *peek() {
      return Option.match(yield* state(Amt).get("v"), { onNone: () => -1, onSome: (r) => r.value })
    },
    // a SHARED signal handler — resolves the run's durable promise via an INGRESS append
    // (resolvePromise → SignalResolved on the run stream); it never mutates user state.
    *approve(ok: boolean) {
      yield* resolvePromise("approved", Schema.Boolean, ok)
      return ok ? "approved" : "denied"
    },
  },
  runSchema: { input: Schema.Number, output: Schema.Number },
  sharedSchemas: { approve: { input: Schema.Boolean } },
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
        + "returns \"alreadyStarted\" (NOT a deduped second run) while running, after an engine restart, AND "
        + "after completion (the AlreadyCompleted projection branch); the run body executes EXACTLY ONCE; the "
        + "parked run is boot-recovered by a fresh engine and resumed by a residency-independent ingress signal, "
        + "then attach returns its result",
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
          // "alreadyStarted" (the pending branch across a restart); a residency-independent ingress
          // signal resolves the promise; attach returns the decoded result; and a duplicate start
          // AFTER completion is "alreadyStarted" too (the AlreadyCompleted admit branch — the one
          // case run-once must also cover, never a re-run).
          const result = yield* Effect.gen(function*() {
            const dupPending = yield* workflowSubmit(approval, id, 5)
            assertEquals(dupPending, "alreadyStarted") // already started across the restart (pending branch)
            const runId = yield* workflowRunId(approval, id)
            yield* resolveSignal(runId, "approved", Schema.Boolean, true)
            const out = yield* workflowAttach(approval, id) // the run has now COMPLETED
            const dupCompleted = yield* workflowSubmit(approval, id, 5)
            assertEquals(dupCompleted, "alreadyStarted") // already started AFTER completion (completed branch)
            return out
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(result, 5) // the run resumed and returned its (replayed) value
          // executed EXACTLY once — across duplicate starts while PENDING, across an engine
          // restart, AND across a duplicate start after COMPLETION (none re-ran the body).
          assertEquals(runExecutions.count - before, 1)
        }),
    },
    {
      id: "WORKFLOW.2",
      description:
        "workflow signal/query handlers are ordinary SHARED handlers: while the run is parked on its promise "
        + "(holding the exclusive head), a shared QUERY handler runs CONCURRENTLY over the folded snapshot — it "
        + "is never queued behind the parked exclusive run — and reads the user state the run committed before "
        + "parking (an admitted exclusive call would hang behind the parked head; the shared read returns promptly)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.admit")) && spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.object.shared")) && spans.exists(s, named(s, "effect-s2-durable.object.snapshot"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const id = key
          reachedPark.delete("42")
          const engine = serviceLayer(approval)
          yield* Effect.gen(function*() {
            yield* workflowSubmit(approval, id, 42)
            yield* waitForPark(42) // the run committed its state, then parked holding the exclusive head
            // a SHARED query runs concurrently over the snapshot — if it were queued behind the
            // parked exclusive run this would hang; instead it returns the committed state promptly.
            const seen = yield* sharedClient(approval, id).peek()
            assertEquals(seen, 42)
            // resolve via the shared signal handler + attach so the scope completes cleanly.
            yield* sharedClient(approval, id).approve(true)
            assertEquals(yield* workflowAttach(approval, id), 42)
          }).pipe(Effect.provide(engine), Effect.scoped)
        }),
    },
    {
      id: "WORKFLOW.3",
      description:
        "a durable promise the run body awaits is resolved BY a shared handler: the run parks on signal(\"approved\"); "
        + "a SHARED signal handler (sharedClient(wf, id).approve) resolves it via resolvePromise — appending a "
        + "SignalResolved INGRESS event to the run's owner stream, NOT a user-state write — and the parked run "
        + "resumes and attach returns its result (no residency-independent ingress door involved)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.admit")) && spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.object.shared")) && spans.exists(s, named(s, "effect-s2-durable.resolveSignal"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const id = key
          reachedPark.delete("8")
          const engine = serviceLayer(approval)
          const before = runExecutions.count
          const result = yield* Effect.gen(function*() {
            const started = yield* workflowSubmit(approval, id, 8)
            assertEquals(started, "started")
            yield* waitForPark(8) // the run is parked on the durable promise
            // the SHARED signal handler resolves the promise the run awaits (ingress append).
            const verdict = yield* sharedClient(approval, id).approve(true)
            assertEquals(verdict, "approved")
            return yield* workflowAttach(approval, id) // the run resumed and completed
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(result, 8) // resumed via the shared-handler resolution
          assertEquals(runExecutions.count - before, 1) // the run executed once; the shared handler never ran it
        }),
    },
  ],
})
