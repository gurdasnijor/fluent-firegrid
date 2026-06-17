import { Duration, Effect, Schema } from "effect"
import { attach, object, resolveSignal, run, sendClient, serviceLayer, signal } from "effect-s2-durable"
import { assertEquals } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

// Object boot recovery proven VERTICALLY across an engine restart over one s2 lite
// backend (two engine scopes; only one engine live at a time), driving the PUBLIC
// object path. A fresh engine enumerates owner streams (StreamDb.list, NAME
// enumeration only) and restarts the pending head; durable run facts replay (never
// re-executed), a parked signal re-parks, and a completed call is never re-run.

// real executions of the `post` run step — recovery must never double-execute it.
const runExecutions = { count: 0 }
// test-side marker: the handler records its amount AFTER `run` returns (so its
// Journaled run fact is durable) and BEFORE it parks. Process 1 waits for this so the
// restart genuinely happens with a recorded fact + a parked call (not merely admitted).
const reachedRunFor = new Set<number>()

const ledger = object({
  name: "firelab-rec-ledger",
  handlers: {
    *post(amount: number) {
      const n = yield* run(Effect.sync(() => (runExecutions.count++, amount)), { output: Schema.Number })
      reachedRunFor.add(amount) // run fact is now durable; about to park
      yield* signal("posted", Schema.Boolean) // park until released
      return n
    },
  },
})

// poll until the handler has passed its run step in the live engine (bounded by the
// firelab run timeout; if the run never happens that is itself a failure).
const waitForRun = (amount: number): Effect.Effect<void> =>
  Effect.suspend((): Effect.Effect<void> =>
    reachedRunFor.has(amount)
      ? Effect.void
      : Effect.sleep(Duration.millis(10)).pipe(Effect.flatMap(() => waitForRun(amount)))
  )

export default defineValidation({
  id: "effect-s2-durable-object-recovery",
  description:
    "Proves object boot recovery across an engine restart over one s2 lite backend, through the public "
    + "object/sendClient/resolveSignal/attach path: a fresh engine enumerates owner keys and restarts the "
    + "pending head (a parked signal call resumes; a recorded run fact replays exactly once; a completed "
    + "call is never re-run) — with OTel evidence for object boot-recovery + owner enumeration + drain.",
  feature: {
    product: "effect-s2-durable",
    name: "object-actor-model",
  },
  backend: S2LiteLive,
  component: ({ key, keyFor }) => Effect.succeed({ key, keyFor }),
  requirements: [
    {
      id: "RECOVERY.1",
      description:
        "boot enumerates object owner keys and restarts the pending head: a signal-parked object call, "
        + "left incomplete (genuinely parked) when its engine tore down, is re-driven by a fresh engine and "
        + "then settled by a residency-independent resolveSignal + attach",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.object.ownerKeys")) && spans.exists(s, named(s, "effect-s2-durable.object.drain"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          reachedRunFor.delete(5)
          const engine = serviceLayer(ledger)
          // process 1: submit AND wait until the handler is genuinely parked, then tear down.
          const id = yield* Effect.gen(function*() {
            const id = yield* sendClient(ledger, key).post(5)
            yield* waitForRun(5)
            return id
          }).pipe(Effect.provide(engine), Effect.scoped)
          // process 2: a fresh engine boot-recovers the pending head (re-parks), then a
          // residency-independent resolve + attach settle it.
          const result = yield* Effect.gen(function*() {
            yield* resolveSignal(id, "posted", Schema.Boolean, true)
            return yield* attach(id, Schema.Number)
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(result, 5)
        }),
    },
    {
      id: "RECOVERY.3",
      description:
        "replay folds without re-executing: a `run` fact recorded + made durable in process 1 (the handler "
        + "is parked) replays its value exactly once when a fresh engine re-drives the call — the run effect "
        + "is never re-executed across the restart",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.object.drain"))',
      claim: ({ keyFor }) =>
        Effect.gen(function*() {
          const k = keyFor("replay")
          reachedRunFor.delete(9)
          const engine = serviceLayer(ledger)
          const before = runExecutions.count
          // process 1: run EXECUTES here (count++), its fact becomes durable, then it parks.
          const id = yield* Effect.gen(function*() {
            const id = yield* sendClient(ledger, k).post(9)
            yield* waitForRun(9) // guarantees the run executed + the Journaled fact is durable
            return id
          }).pipe(Effect.provide(engine), Effect.scoped)
          // process 2: recovery re-drives the call; `run` must REPLAY the recorded fact.
          const result = yield* Effect.gen(function*() {
            yield* resolveSignal(id, "posted", Schema.Boolean, true)
            return yield* attach(id, Schema.Number)
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(result, 9)
          // executed exactly once (in process 1); process 2 replayed the fact, no re-exec.
          assertEquals(runExecutions.count - before, 1)
        }),
    },
    {
      id: "RECOVERY.2",
      description:
        "stream existence is not liveness: a COMPLETED object call's owner stream persists, but a fresh "
        + "engine's boot recovery does not re-run it (only the genuinely pending head is driven)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.object.ownerKeys"))',
      claim: ({ keyFor }) =>
        Effect.gen(function*() {
          const k = keyFor("settled")
          const engine = serviceLayer(ledger)
          // process 1: run a call to COMPLETION (run + resolve + attach).
          const before = yield* Effect.gen(function*() {
            const id = yield* sendClient(ledger, k).post(4)
            yield* resolveSignal(id, "posted", Schema.Boolean, true)
            yield* attach(id, Schema.Number)
            return runExecutions.count
          }).pipe(Effect.provide(engine), Effect.scoped)
          // process 2: a fresh engine boot-recovers, then runs ONE new call on the same
          // key. If the completed call were re-run, the delta would be 2.
          const after = yield* Effect.gen(function*() {
            const id = yield* sendClient(ledger, k).post(100)
            yield* resolveSignal(id, "posted", Schema.Boolean, true)
            yield* attach(id, Schema.Number)
            return runExecutions.count
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(after - before, 1) // only the new call ran; the completed one was not re-run
        }),
    },
  ],
})
