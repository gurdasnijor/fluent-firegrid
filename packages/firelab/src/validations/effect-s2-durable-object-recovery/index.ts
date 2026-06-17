import { Effect, Schema } from "effect"
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

const ledger = object({
  name: "firelab-rec-ledger",
  handlers: {
    // a durable run step (counts executions) followed by a park on a signal, so the
    // call is left pending across a restart with a recorded run fact.
    *post(amount: number) {
      const n = yield* run(Effect.sync(() => (runExecutions.count++, amount)), { output: Schema.Number })
      yield* signal("posted", Schema.Boolean) // park until released
      return n
    },
  },
})

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
        + "left incomplete when its engine tore down, is re-driven by a fresh engine and then settled by a "
        + "residency-independent resolveSignal + attach",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.object.ownerKeys")) && spans.exists(s, named(s, "effect-s2-durable.object.drain"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const engine = serviceLayer(ledger)
          // process 1: submit; the handler parks on the signal; tear the engine down.
          const id = yield* sendClient(ledger, key).post(5).pipe(Effect.provide(engine), Effect.scoped)
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
        "replay folds without re-executing: a recorded `run` fact on a recovered, still-pending call "
        + "replays its value exactly once across the restart (the run effect is never re-executed)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.boot-recover")) && spans.exists(s, named(s, "effect-s2-durable.object.drain"))',
      claim: ({ keyFor }) =>
        Effect.gen(function*() {
          const k = keyFor("replay")
          const engine = serviceLayer(ledger)
          const before = runExecutions.count
          const id = yield* sendClient(ledger, k).post(9).pipe(Effect.provide(engine), Effect.scoped)
          const result = yield* Effect.gen(function*() {
            yield* resolveSignal(id, "posted", Schema.Boolean, true)
            return yield* attach(id, Schema.Number)
          }).pipe(Effect.provide(engine), Effect.scoped)
          assertEquals(result, 9)
          // the run effect ran EXACTLY once across process 1 + the recovered process 2.
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
