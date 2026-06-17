import { Effect, Layer, Option, Schema } from "effect"
import { Actor } from "effect-s2-durable"
import { assertEquals, assertTrue } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

// Phase 3b — the effectful actor runtime over real S2 (s2 lite): ActorLog
// (readDecoded + S2Client.append), admission (projection + CAS), the serial
// drainer, and ingress. Complements the pure-core validation
// (effect-s2-durable-object-actor-model); together they cover object-actor-model.

const BASE = "firelab-actor"

// A tiny exclusive handler set: `add` reads/writes user state; `collide` writes
// composite-keyed state; `boom` fails.
const handlers: Actor.Handlers = {
  add: (input, ctx) =>
    Effect.gen(function*() {
      const current = Option.getOrElse(yield* ctx.state.get("balance", "acct"), () => 0)
      const next = Number(current) + Number(input)
      yield* ctx.state.set("balance", "acct", next)
      return next
    }),
  collide: (_input, ctx) =>
    Effect.gen(function*() {
      yield* ctx.state.set("a", "b/c", 1)
      yield* ctx.state.set("a/b", "c", 2)
      return null
    }),
  boom: () => Effect.fail("handler failed"),
}

export default defineValidation({
  id: "effect-s2-durable-actor-runtime",
  description:
    "Drives the effectful actor runtime against s2 lite: the ActorLog over "
    + "readDecoded + S2Client.append, projection+CAS admission, the serial drainer "
    + "(state journaled as events, completion), and residency-independent ingress.",
  feature: {
    product: "effect-s2-durable",
    name: "object-actor-model",
  },
  backend: Layer.merge(S2LiteLive, Actor.DrainerLocks.layer),
  component: ({ key }) =>
    Effect.gen(function*() {
      // the stream path is derived by encoding the owner through its key codec (ROUTING.3).
      const log = yield* Actor.logForOwner(BASE, Schema.String, key)
      return { log, handlers }
    }),
  requirements: [
    {
      id: "LAYERING.7",
      description: "the actor log is read via readDecoded and written via S2Client.append (not a StreamDb fold)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.log.read")) && spans.exists(s, named(s, "S2.read")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 5)
          const entries = yield* log.read() // typed ActorEvents back out of S2
          assertTrue(
            entries.some((entry) => entry.event._tag === "Accepted" && entry.event.callId === "c1"),
            "the appended Accepted reads back as a typed ActorEvent",
          )
        }),
    },
    {
      id: "LAYERING.2",
      description: "an actor instance is one stream plus a deterministic interpreter over it",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.read"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 4)
          yield* Actor.drain(log, handlers) // the interpreter folds the stream and advances
          assertEquals((yield* Actor.attachLog(log, "c1"))._tag, "Success")
        }),
    },
    {
      id: "LAYERING.3",
      description: "effect-s2-durable owns admission, draining, and completion",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.admit")) && spans.exists(s, named(s, "effect-s2-durable.drain"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 3)
          yield* Actor.drain(log, handlers)
          assertEquals(yield* Actor.attachLog(log, "c1"), { _tag: "Success", value: 3 })
        }),
    },
    {
      id: "ADMISSION.1",
      description: "an exclusive call is admitted by durably appending an Accepted event, decoupled from execution",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.admit")) && spans.exists(s, named(s, "effect-s2-durable.log.casAppend")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          const result = yield* Actor.admit(log, "c1", "add", 5)
          assertEquals(result._tag, "Admitted")
          const entries = yield* log.read()
          assertTrue(entries.some((e) => e.event._tag === "Accepted" && e.event.callId === "c1"), "Accepted is durable")
        }),
    },
    {
      id: "ADMISSION.4",
      description: "admission is idempotent by callId via the projection — re-admit returns the existing status, never a duplicate",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.admit")) && spans.exists(s, named(s, "effect-s2-durable.log.casAppend"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          assertEquals((yield* Actor.admit(log, "c1", "add", 5))._tag, "Admitted")
          assertEquals((yield* Actor.admit(log, "c1", "add", 5))._tag, "AlreadyPending") // pending projection
          yield* Actor.drain(log, handlers)
          assertEquals((yield* Actor.admit(log, "c1", "add", 5))._tag, "AlreadyCompleted") // completed projection
          // exactly one Accepted for c1 — never duplicated.
          const accepts = (yield* log.read()).filter((e) => e.event._tag === "Accepted" && e.event.callId === "c1")
          assertEquals(accepts.length, 1)
        }),
    },
    {
      id: "ADMISSION.6",
      description: "admission is producer-only — appending Accepted does not require hosting the drainer",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.admit")) && spans.exists(s, named(s, "effect-s2-durable.log.casAppend"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 5) // no drainer hosted here
          assertTrue((yield* log.read()).some((e) => e.event._tag === "Accepted"), "Accepted is durable without a drainer")
          assertEquals((yield* Actor.attachLog(log, "c1"))._tag, "Pending") // not executed (no drainer ran)
        }),
    },
    {
      id: "EXECUTION.1",
      description: "a single drainer runs per key — concurrent drains do not double-run the head",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.drain")) && spans.exists(s, named(s, "effect-s2-durable.runCall"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 5)
          // two concurrent drainers; the per-key lock serializes them so the head runs once.
          yield* Effect.all([Actor.drain(log, handlers), Actor.drain(log, handlers)], { concurrency: 2 })
          const completes = (yield* log.read()).filter((e) => e.event._tag === "Completed" && e.event.callId === "c1")
          assertEquals(completes.length, 1) // exactly one Completed — the head ran once
        }),
    },
    {
      id: "EXECUTION.2",
      description: "a handler's state reads/writes are journaled — read-modify-write is replay-stable (no double-apply)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.runCall")) && spans.exists(s, named(s, "effect-s2-durable.log.append")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          // state writes are StateChanged events.
          yield* Actor.admit(log, "c1", "add", 7)
          yield* Actor.drain(log, handlers)
          const entries = yield* log.read()
          assertTrue(
            entries.some((e) => e.event._tag === "StateChanged" && e.event.op === "set"),
            "state write is a StateChanged event",
          )
          assertEquals(Option.getOrNull(Actor.stateValue(Actor.replay(entries), "balance", "acct")), 7)

          // crash-mid-call: a fresh call wrote its journaled read + StateChanged but no
          // Completed. Recovery re-runs it; the journaled read replays its ORIGINAL value,
          // so the read-modify-write is NOT double-applied (balance stays 12, not 17).
          yield* log.append({ _tag: "Accepted", callId: "c2", method: "add", input: 5 })
          yield* log.append({ _tag: "Journaled", callId: "c2", step: "read/0", value: { present: true, value: 7 } })
          yield* log.append({ _tag: "StateChanged", op: "set", table: "balance", key: "acct", value: 12 })
          yield* Actor.drain(log, handlers) // recover c2
          assertEquals(Option.getOrNull(Actor.stateValue(Actor.replay(yield* log.read()), "balance", "acct")), 12)
          assertEquals((yield* Actor.attachLog(log, "c2"))._tag, "Success")
        }),
    },
    {
      id: "INGRESS.1",
      description: "resolveSignal appends a SignalResolved event and succeeds whether or not the call is resident",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.resolveSignal")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.resolveSignal(log, "c1", "approval", true) // no call resident or running
          const snapshot = Actor.replay(yield* log.read())
          assertEquals(Option.getOrNull(Actor.signalValue(snapshot, "c1", "approval")), true)
        }),
    },
    {
      id: "INGRESS.2",
      description: "the durable SignalResolved row is the source of truth (read back from the log)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.resolveSignal")) && spans.exists(s, named(s, "effect-s2-durable.log.read"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.resolveSignal(log, "c1", "go", 42)
          const snapshot = Actor.replay(yield* log.read())
          assertEquals(Option.getOrNull(Actor.signalValue(snapshot, "c1", "go")), 42)
        }),
    },
    {
      id: "SYSTEM_OF_RECORD.1",
      description: "state, accept-log, and results all live in one ActorEvent stream",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.read"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 5)
          yield* Actor.drain(log, handlers)
          const tags = new Set((yield* log.read()).map((e) => e.event._tag))
          assertTrue(tags.has("Accepted") && tags.has("StateChanged") && tags.has("Completed"), "one stream holds all")
        }),
    },
    {
      id: "SYSTEM_OF_RECORD.2",
      description: "in-memory state is cache — the projection is recovered by folding the stream from scratch",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.read"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 9)
          yield* Actor.drain(log, handlers)
          // a FRESH log over the same stream — no in-memory carryover.
          const reopened = Actor.openLog(log.streamName)
          assertEquals(yield* Actor.attachLog(reopened, "c1"), { _tag: "Success", value: 9 })
        }),
    },
    {
      id: "PLANNING.3",
      description: "composite identities are path-aware and cannot collide (table/key kept structural)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.append"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "collide", null)
          yield* Actor.drain(log, handlers)
          const snapshot = Actor.replay(yield* log.read())
          assertEquals(Option.getOrNull(Actor.stateValue(snapshot, "a", "b/c")), 1)
          assertEquals(Option.getOrNull(Actor.stateValue(snapshot, "a/b", "c")), 2) // no collision
        }),
    },
    {
      id: "DEPENDENCIES.1",
      description: "the object engine reads/writes the log via effect-s2 (readDecoded + S2Client.append), not a StreamDb fold",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.log.read")) && spans.exists(s, named(s, "S2.read")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ log }) =>
        Effect.gen(function*() {
          yield* Actor.admit(log, "c1", "add", 1)
          yield* Actor.drain(log, handlers)
          assertEquals((yield* Actor.attachLog(log, "c1"))._tag, "Success")
        }),
    },
  ],
})
