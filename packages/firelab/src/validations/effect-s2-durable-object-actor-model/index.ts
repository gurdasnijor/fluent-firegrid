import { Effect, Option } from "effect"
import { Actor } from "effect-s2-durable"
import { assertEquals, assertTrue } from "../../assertions.ts"
import { defineValidation } from "../../types.ts"

// Phase 3a — the PURE actor core. No S2, no Runtime, no StreamDb (object-actor-model
// "Settled decisions": pure types/functions only). Each claim drives the
// instrumented edges (effect-s2-durable.transition / .replay / .attach /
// .callId.{encode,decode}) so the gate has production-path span evidence, while the
// underlying transition/replay/attach/codec are pure and unit-tested separately.

const accepted = (seqNum: number, callId: string, input: unknown = null): Actor.LogEntry => ({
  seqNum,
  event: { _tag: "Accepted", callId, method: "m", input },
})

const completed = (
  seqNum: number,
  callId: string,
  exit: Actor.ActorExit = { _tag: "Success", value: null },
): Actor.LogEntry => ({ seqNum, event: { _tag: "Completed", callId, exit } })

const TRANSITION = 'spans.exists(s, named(s, "effect-s2-durable.transition"))'
const REPLAY = 'spans.exists(s, named(s, "effect-s2-durable.replay"))'

export default defineValidation({
  id: "effect-s2-durable-object-actor-model",
  description:
    "Proves the pure actor core (object-actor-model): the reversible CallId codec, the pure "
    + "transition (admission, completion-derived advance, single-writer), replay-as-fold, "
    + "recovered-head restart, and the attach/poll projection views — with no S2.",
  feature: {
    product: "effect-s2-durable",
    name: "object-actor-model",
  },
  component: () => Effect.succeed({ callId: Actor.ActorCallId }),
  requirements: [
    // ── ROUTING ─────────────────────────────────────────────────────────────
    {
      id: "ROUTING.1",
      description: "a callId carries a schema-decodable owner identity",
      evidence: 'spans.exists(s, named(s, "effect-s2-durable.callId.decode"))',
      claim: ({ callId }) =>
        Effect.gen(function*() {
          const encoded = yield* Actor.encodeCallId(callId, { owner: "obj:1", method: "add", nonce: "x" })
          const parts = yield* Actor.decodeCallId(callId, encoded)
          assertEquals(parts.owner, "obj:1") // owner recovery is a pure decode
        }),
    },
    {
      id: "ROUTING.2",
      description: "the owner stream is derived from the callId alone (no roster or side index)",
      evidence: 'spans.exists(s, named(s, "effect-s2-durable.callId.decode"))',
      claim: ({ callId }) =>
        Effect.gen(function*() {
          // only the callId string is in scope — no index, no parsing.
          const encoded = yield* Actor.encodeCallId(callId, { owner: "counter:7", method: "incr", nonce: "n" })
          const { owner } = yield* Actor.decodeCallId(callId, encoded)
          assertEquals(owner, "counter:7")
        }),
    },
    {
      id: "ROUTING.3",
      description: "the callId codec is reversible (decode∘encode and encode∘decode round-trip)",
      evidence:
        'spans.exists(s, named(s, "effect-s2-durable.callId.encode")) && spans.exists(s, named(s, "effect-s2-durable.callId.decode"))',
      claim: ({ callId }) =>
        Effect.gen(function*() {
          const parts = { owner: "obj:42", method: "transfer", nonce: "abc" }
          const encoded = yield* Actor.encodeCallId(callId, parts)
          const decoded = yield* Actor.decodeCallId(callId, encoded)
          assertEquals(decoded, parts) // decode ∘ encode
          const reEncoded = yield* Actor.encodeCallId(callId, decoded)
          assertEquals(reEncoded, encoded) // encode ∘ decode
        }),
    },
    // ── ADMISSION ───────────────────────────────────────────────────────────
    {
      id: "ADMISSION.1-1",
      description: "an Accepted carries { callId, method, input }; admission order is the appended seq_num",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const [snap] = yield* Actor.planStep(Actor.empty, accepted(0, "c", 5))
          assertEquals(snap.pending, ["c"])
          assertEquals(snap.cursor, 0) // admission order = the appended seq_num
        }),
    },
    {
      id: "ADMISSION.2",
      description: "the accept-log is append-only — the transition mutates nothing, there is no status field",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const [snap] = yield* Actor.planStep(Actor.empty, accepted(0, "c"))
          assertEquals(Actor.empty.pending, []) // the input snapshot is untouched
          assertTrue(snap !== Actor.empty, "transition returns a new snapshot")
        }),
    },
    {
      id: "ADMISSION.3",
      description: "S2 seq_num is the only total order — no second app-level sequence field",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const s1 = (yield* Actor.planStep(Actor.empty, accepted(5, "a")))[0]
          const s2 = (yield* Actor.planStep(s1, accepted(9, "b")))[0]
          const [s3] = yield* Actor.planStep(s2, accepted(12, "c"))
          assertEquals(s3.pending, ["a", "b", "c"]) // append order = the seq_nums
          assertEquals(s3.cursor, 12)
        }),
    },
    // ── EXECUTION / HANDLERS ──────────────────────────────────────────────────
    {
      id: "EXECUTION.1",
      description: "a single drainer runs the lowest-seq_num call to completion before the next",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const s1 = (yield* Actor.planStep(Actor.empty, accepted(0, "a")))[0]
          const [s2, busy] = yield* Actor.planStep(s1, accepted(1, "b"))
          assertEquals(busy, []) // b enqueues, does not start while a runs
          const [, advance] = yield* Actor.planStep(s2, completed(2, "a"))
          assertEquals(advance, [{ _tag: "StartCall", callId: "b" }]) // next head starts on completion
        }),
    },
    {
      id: "HANDLERS.2",
      description: "at most one exclusive call is active per key (single-writer)",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const s1 = (yield* Actor.planStep(Actor.empty, accepted(0, "a")))[0]
          const [s2, actions] = yield* Actor.planStep(s1, accepted(1, "b"))
          assertEquals(Option.getOrNull(s2.active), "a") // still exactly one active
          assertEquals(actions, []) // no second StartCall
        }),
    },
    // ── COMPLETION ────────────────────────────────────────────────────────────
    {
      id: "COMPLETION.1",
      description: "a call settles by appending a single Completed carrying an Exit",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const s = (yield* Actor.planStep(Actor.empty, accepted(0, "a")))[0]
          const [done] = yield* Actor.planStep(s, completed(1, "a", { _tag: "Failure", error: "boom" }))
          assertEquals(done.results.get("a"), { _tag: "Failure", error: "boom" })
        }),
    },
    {
      id: "COMPLETION.2",
      description: "a call is done iff its Completed event exists; pending = accepted ∧ ¬completed",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const [open] = yield* Actor.planStep(Actor.empty, accepted(0, "a"))
          assertTrue(!Actor.isDone(open, "a") && open.pending.includes("a"), "pending while uncompleted")
          const [done] = yield* Actor.planStep(open, completed(1, "a"))
          assertTrue(Actor.isDone(done, "a") && !done.pending.includes("a"), "done once Completed exists")
        }),
    },
    {
      id: "COMPLETION.3",
      description: "advance is re-derived, not dequeued — a completed call cannot re-run (window-2 impossible)",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          const settled = (yield* Actor.planStep(
            (yield* Actor.planStep(Actor.empty, accepted(0, "a")))[0],
            completed(1, "a"),
          ))[0]
          const [after, actions] = yield* Actor.planStep(settled, accepted(2, "a")) // stray duplicate admission
          assertEquals(after.pending, []) // not re-queued
          assertEquals(actions, []) // not re-run
        }),
    },
    {
      id: "COMPLETION.4",
      description: "attach reads the result; a duplicate completed callId is served from it, never re-run",
      evidence: `${TRANSITION} && spans.exists(s, named(s, "effect-s2-durable.attach"))`,
      claim: () =>
        Effect.gen(function*() {
          const open = (yield* Actor.planStep(Actor.empty, accepted(0, "a")))[0]
          assertEquals(yield* Actor.attachView(open, "a"), { _tag: "Pending" })
          const [done] = yield* Actor.planStep(open, completed(1, "a", { _tag: "Success", value: 42 }))
          assertEquals(yield* Actor.attachView(done, "a"), { _tag: "Success", value: 42 })
        }),
    },
    // ── PLANNING ──────────────────────────────────────────────────────────────
    {
      id: "PLANNING.1",
      description: "the next action is planned from the projection; recovery re-plans from durable events",
      evidence: `${REPLAY} && ${TRANSITION}`,
      claim: () =>
        Effect.gen(function*() {
          const snap = yield* Actor.replayLog([accepted(0, "a"), accepted(1, "b")])
          const [, actions] = yield* Actor.planStep(snap, completed(2, "a"))
          assertEquals(actions, [{ _tag: "StartCall", callId: "b" }]) // re-planned purely from durable events
        }),
    },
    {
      id: "PLANNING.2",
      description: "runtime state is an actor snapshot at an S2 cursor; attach/poll are views over it",
      evidence: `${REPLAY} && spans.exists(s, named(s, "effect-s2-durable.attach"))`,
      claim: () =>
        Effect.gen(function*() {
          const snap = yield* Actor.replayLog([accepted(0, "a"), completed(1, "a", { _tag: "Success", value: 9 })])
          assertEquals(snap.cursor, 1) // snapshot at a cursor
          assertEquals(yield* Actor.attachView(snap, "a"), { _tag: "Success", value: 9 }) // a view over it
        }),
    },
    {
      id: "PLANNING.7",
      description: "ordering/advance live in a PURE transition (snapshot, event) -> (snapshot, action[])",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          // same input twice → identical output (deterministic, no I/O).
          const a = yield* Actor.planStep(Actor.empty, accepted(0, "a"))
          const b = yield* Actor.planStep(Actor.empty, accepted(0, "a"))
          assertEquals(a[0].pending, b[0].pending)
          assertEquals(a[1], b[1])
        }),
    },
    {
      id: "PLANNING.8",
      description: "the transition is testable as snapshot + event -> snapshot + actions without S2/timers/fibers",
      evidence: TRANSITION,
      claim: () =>
        Effect.gen(function*() {
          // this whole validation runs with NO backend (no s2 lite) — the proof itself.
          const [snap, actions] = yield* Actor.planStep(Actor.empty, accepted(0, "a"))
          assertEquals(snap.pending, ["a"])
          assertEquals(actions, [{ _tag: "StartCall", callId: "a" }])
        }),
    },
    // ── RECOVERY ──────────────────────────────────────────────────────────────
    {
      id: "RECOVERY.3",
      description: "replay folds history into a snapshot WITHOUT executing actions",
      evidence: REPLAY,
      claim: () =>
        Effect.gen(function*() {
          // replay returns only a snapshot — actions are discarded by construction.
          const snap = yield* Actor.replayLog([accepted(0, "a"), completed(1, "a"), accepted(2, "b")])
          assertTrue(Actor.isDone(snap, "a"), "a is settled after fold")
          assertEquals(snap.pending, ["b"]) // deterministic, no re-run of a
        }),
    },
    {
      id: "RECOVERY.4",
      description: "active is the durable head; recovery restarts it regardless of fiber residency",
      evidence: REPLAY,
      claim: () =>
        Effect.gen(function*() {
          // after a cold fold-only replay, active = Some(head) with no resident fiber.
          const snap = yield* Actor.replayLog([accepted(0, "a"), accepted(1, "b")])
          assertEquals(Option.getOrNull(snap.active), "a")
          assertEquals(Actor.recoveredHeadActions(snap), [{ _tag: "StartCall", callId: "a" }])
        }),
    },
  ],
})
