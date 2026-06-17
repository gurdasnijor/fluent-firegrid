import { Effect, Layer, Option, Schema } from "effect"
import {
  attach,
  awakeable,
  client,
  deferred,
  object,
  resolveAwakeable,
  resolveSignal,
  run,
  sendClient,
  serviceLayer,
  signal,
  state,
} from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { assertEquals } from "../../assertions.ts"
import { S2LiteLive } from "../../s2lite.ts"
import { defineValidation } from "../../types.ts"

// Phase 3 Slice A — proves the PUBLIC object call path end-to-end through
// `DurableExecutionRuntime` (object/client/sendClient/attach), with OTel evidence
// that the runtime drove the per-owner ActorEvent log (admit → drain → Completed →
// attach). No `Actor.*` helper is touched — the behaviour under test is the product.

class Counter extends Table<Counter>("counterState")({
  id: Schema.String.pipe(primaryKey),
  value: Schema.Number,
}) {}

// A keyed virtual object: durable per-key state + exclusive (serialized) methods.
const counter = object({
  name: "firelab-object-counter",
  handlers: {
    *add(amount: number) {
      const st = state(Counter)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      const next = cur + amount
      yield* st.set({ id: "v", value: next })
      return next
    },
    *value() {
      const st = state(Counter)
      return Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
    },
    // parks on a durable signal; the value is supplied by a residency-independent
    // resolveSignal(callId, ...) appended to the owner stream.
    *awaitApproval() {
      const approved = yield* signal("approved", Schema.Boolean)
      return approved ? "approved" : "denied"
    },
    // parks on an awakeable whose id is a deterministic `${callId}/awk/0`; an ingress
    // caller resolves it residency-independently via resolveAwakeable.
    *awaitAwakeable() {
      const awk = yield* awakeable(Schema.Boolean)
      return (yield* awk.promise) ? "awoke" : "no"
    },
    // a handler-local durable promise: resolve it, then read it back within the same
    // execution (resolveLocal appends a SignalResolved; the durable row is the truth).
    *deferLocal() {
      const done = deferred("done", Schema.Boolean)
      yield* done.resolve(true)
      return (yield* done.get()) ? "resolved" : "no"
    },
    // two run steps with the SAME name: the first executes the effect and records a
    // `Journaled` run fact; the second REPLAYS that fact (no re-execution). The
    // `state.get` writes a read-journal fact alongside (a distinct kind, so it cannot
    // collide with the run fact). `runExecutions` counts real executions.
    *tally(amount: number) {
      const a = yield* run(Effect.sync(() => (runExecutions.count++, amount * 2)), { name: "double", output: Schema.Number })
      const b = yield* run(Effect.sync(() => (runExecutions.count++, amount * 2)), { name: "double", output: Schema.Number })
      const st = state(Counter)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      yield* st.set({ id: "v", value: cur + a + b })
      return a + b
    },
  },
})

// real executions of the `tally` run action (a duplicate/replayed call must NOT bump it).
const runExecutions = { count: 0 }

export default defineValidation({
  id: "effect-s2-durable-object-call",
  description:
    "Drives the PUBLIC virtual-object path (object/client/sendClient/attach) on the one "
    + "DurableExecutionRuntime boundary against s2 lite: a method call is admitted as an Accepted "
    + "event, runs through the exclusive per-key drainer, journals state + a Completed event, and "
    + "attach(callId) self-routes to the owner projection — proven end-to-end with OTel spans showing "
    + "the object runtime used ActorEvent log operations.",
  feature: {
    product: "effect-s2-durable",
    name: "object-actor-model",
  },
  // the one runtime boundary, seeded with the object's handlers, over s2 lite.
  backend: serviceLayer(counter).pipe(Layer.provide(S2LiteLive)),
  component: ({ key, keyFor }) => Effect.succeed({ key, keyFor }),
  requirements: [
    {
      id: "LAYERING.1",
      description:
        "the public object call returns the decoded result, backed by the internal actor log (the "
        + "abstraction is internal — the public client API is unchanged)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.admit")) && spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.casAppend"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          assertEquals(yield* client(counter, key).add(5), 5) // end-to-end public call
        }),
    },
    {
      id: "LAYERING.2",
      description: "an object instance is one durable owner stream + interpreter — state persists across calls",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.append")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          assertEquals(yield* client(counter, key).add(5), 5)
          assertEquals(yield* client(counter, key).add(3), 8) // a fresh call sees the prior write
          assertEquals(yield* client(counter, key).value(), 8) // folded from the same owner stream
        }),
    },
    {
      id: "ADMISSION.1",
      description: "an exclusive call is admitted by appending an Accepted event (durable, decoupled from execution)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.admit")) && spans.exists(s, named(s, "effect-s2-durable.log.casAppend")) && spans.exists(s, named(s, "S2.append"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          assertEquals(yield* client(counter, key).add(7), 7)
        }),
    },
    {
      id: "ADMISSION.4",
      description:
        "admission is idempotent by callId (read-then-CAS) — a pinned idempotency key is not re-run or double-applied",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.admit")) && spans.exists(s, named(s, "effect-s2-durable.callId.encode"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const c = client(counter, key)
          assertEquals(yield* c.add(5, { idempotencyKey: "pinned" }), 5)
          assertEquals(yield* c.add(5, { idempotencyKey: "pinned" }), 5) // same callId → served, not re-run
          assertEquals(yield* c.value(), 5) // not 10
        }),
    },
    {
      id: "EXECUTION.2",
      description:
        "an object method's durable `run` step records a terminal fact on the owner stream (Journaled); "
        + "a second same-named run within the SAME execution replays that recorded fact instead of "
        + "re-executing the effect (typed output mirrors service run; not admission dedup)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.append")) && spans.exists(s, named(s, "effect-s2-durable.object.admit"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const before = runExecutions.count
          // a = run("double") executes (3*2=6); b = run("double") REPLAYS a's fact (no re-exec) → 6.
          assertEquals(yield* client(counter, key).tally(3), 12) // a + b = 6 + 6
          assertEquals(runExecutions.count - before, 1) // the run effect ran exactly once; b replayed the fact
          assertEquals(yield* client(counter, key).value(), 12) // cur(0) + a + b
        }),
    },
    {
      id: "EXECUTION.1",
      description: "a single per-key drainer runs exclusive calls serially — concurrent RMW is lost-update-free",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.casAppend"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          yield* Effect.all(Array.from({ length: 8 }, () => client(counter, key).add(1)), {
            concurrency: "unbounded",
          })
          assertEquals(yield* client(counter, key).value(), 8) // exactly 8 — exclusive, no lost updates
        }),
    },
    {
      id: "COMPLETION.1",
      description: "a call settles by appending a single Completed event carrying its Exit",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.append"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          assertEquals(yield* client(counter, key).add(9), 9)
        }),
    },
    {
      id: "COMPLETION.4",
      description: "attach(callId) reads the result from the owner projection; a duplicate is served, never re-run",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.status")) && spans.exists(s, named(s, "effect-s2-durable.callId.decode")) && spans.exists(s, named(s, "effect-s2-durable.log.read"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const id = yield* sendClient(counter, key).add(4) // fire-and-forget → callId
          assertEquals(yield* attach(id, Schema.Number), 4)
          assertEquals(yield* attach(id, Schema.Number), 4) // duplicate served from the projection
          assertEquals(yield* client(counter, key).value(), 4) // not re-applied
        }),
    },
    {
      id: "INGRESS.1",
      description:
        "object signal AND awakeable ingress are residency-independent: a parked object method resumes "
        + "when resolveSignal/resolveAwakeable(callId, name, value) appends a SignalResolved to the owner "
        + "stream (routed by the call id); the awakeable name is the deterministic `${callId}/awk/0`",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.resolveSignal")) && spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.append"))',
      claim: ({ key, keyFor }) =>
        Effect.gen(function*() {
          // signal: a parked method resumes from a residency-independent resolveSignal.
          const sid = yield* sendClient(counter, key).awaitApproval()
          yield* resolveSignal(sid, "approved", Schema.Boolean, true)
          assertEquals(yield* attach(sid, Schema.String), "approved")
          // awakeable: deterministic id `${callId}/awk/0`, resolved residency-independently.
          const aid = yield* sendClient(counter, keyFor("awk")).awaitAwakeable()
          yield* resolveAwakeable(aid, `${aid}/awk/0`, Schema.Boolean, true)
          assertEquals(yield* attach(aid, Schema.String), "awoke")
        }),
    },
    {
      id: "INGRESS.2",
      description:
        "a handler-local durable promise (deferred) resolves and reads back from the durable owner-stream "
        + "row within one execution — the row is the source of truth (resolveLocal appends a SignalResolved)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.object.drain")) && spans.exists(s, named(s, "effect-s2-durable.log.append")) && spans.exists(s, named(s, "effect-s2-durable.object.admit"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          assertEquals(yield* client(counter, key).deferLocal(), "resolved")
        }),
    },
    {
      id: "ROUTING.1",
      description: "a callId self-routes to its owner stream — attach derives the owner from the id alone, no roster",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.callId.decode")) && spans.exists(s, named(s, "effect-s2-durable.object.status")) && spans.exists(s, named(s, "S2.read"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const id = yield* sendClient(counter, key).add(6)
          assertEquals(yield* attach(id, Schema.Number), 6) // routed purely from the call id
        }),
    },
    {
      id: "ROUTING.3",
      description: "the callId encoding is a reversible Effect Schema codec (owner recovered by a pure decode)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.callId.encode")) && spans.exists(s, named(s, "effect-s2-durable.callId.decode"))',
      claim: ({ key }) =>
        Effect.gen(function*() {
          const id = yield* sendClient(counter, key).add(1) // encode at mint, decode at submit/attach
          assertEquals(yield* attach(id, Schema.Number), 1)
        }),
    },
  ],
})
