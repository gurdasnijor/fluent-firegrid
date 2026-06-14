# Spike findings — S2-backed durable execution (`s2-durable`)

Deliverable §11.3. What was verified, and where a primitive behaved differently
than the SDD assumed. The deltas are the spike's most valuable output.

> **Substrate caveat.** This spike runs against an in-memory `s2-lite` emulation
> (`src/s2InMemory.ts`) that faithfully models S2's two load-bearing primitives —
> a fencing token (set-once-per-lease, highest wins) and a `match_seq_num`-guarded
> conditional append — plus follow reads and trim. It does **not** call the hosted
> S2 TS SDK; the environment had no basin access. The `S2` service (`src/s2.ts`)
> is the seam: a real SDK Layer drops in behind it untouched. Every Q1–Q6 answer
> below is therefore "verified against the contract the runtime depends on" — the
> hosted-basin parity run (§9) remains the one open validation.

## Q1 — `fencing_token` AND `match_seq_num` on the same append

**Resolved (contract): combine both.** The `append` signature carries both as
options and the emulation enforces fence first, then `match_seq_num`
(`test/s2.test.ts` "combines fencing_token AND match_seq_num on one append"). The
runtime presents both on every journal write.

**Load-bearing path taken:** `match_seq_num` on *every* journal write is the
exactly-once guard; the fence is the coarse zombie-stopper. We did **not** rely on
fence-only safety. This matches the SDD fallback recommendation, so the design is
robust even if the hosted SDK turns out **not** to combine them — `match_seq_num`
alone secures the single-writer-at-tail invariant (two writers compute the same
expected seq; first wins, second gets `412 position-taken`), which is proven in
`test/s2.test.ts`.

## Q2 — append vs read sessions / Effect `Sink`/`Stream` mapping

**Resolved.** `read` is surfaced as `Stream<S2Record>` (with `follow` keeping it
open at the tail); `append` is an `Effect` returning the new tail rather than a
long-lived `Sink`. For replay-based journaling a per-write conditional `Effect` is
the better fit than a streaming `Sink` — each append needs its own
`match_seq_num` precondition and a typed `412` outcome, which a `Sink`'s
fold-and-emit shape obscures. A real SDK append *session* can still back this
`Effect` (open once, reuse); the `Effect` boundary is where Q3's re-establishment
lives. Sessions are confirmed re-establishable in the emulation (a fresh `read`
re-subscribes; `append` is stateless at the service boundary).

## Q3 — append session poisoned on `412`, re-establishment

**Resolved by construction.** Because `append` is modelled as a discrete
conditional `Effect` (not a held-open `Sink`), a `412` (`AppendCondFailed`) is an
ordinary typed failure — it cannot wedge a writer. The runtime distinguishes the
two `412` flavours (`reason: "fence-mismatch"` vs `"position-taken"`) and reacts:
fence-mismatch ⇒ `LostLeaseError`, abandon the tick; position-taken ⇒ idempotent
resume (re-read that op's recorded result). No session teardown/recreate dance was
needed at this layer. **Flag for the hosted SDK:** if its append is a true held
session, the `S2` Layer must catch `412`, drop the session, and reopen before the
next write — the seam is ready for that, but it is unverified against the SDK.

## Q4 — suspend-via-defect vs error-channel

**Resolved: defect.** `Suspend` is raised with `Effect.die` and caught at the host
with `Effect.catchDefect` / `Exit` + `Cause.findDefect` (`src/context.ts`,
`src/runtime.ts`). Verified that user-level `catchAll` over the handler cannot
intercept it — the demo's branches and `ctx.run` error handling sit in the typed
error channel, which is orthogonal to the defect channel the suspend travels in.
`DivergenceError` uses the same defect path so a replay mismatch also cannot be
swallowed (`test/acceptance.test.ts` AC-2). No fallback to a runtime-private error
channel was required.

## Q5 — replay determinism under Effect concurrency

**Respected the spike constraint: sequential `Effect.gen` only.** The op-index is a
single `Ref` incremented in handler order; concurrent durable ops (`Effect.all`,
`fork`) are out of scope and would need a branch-keyed op-index. Determinism of
ordinary `Clock`/`Random` inside handler code is provided by seed-sourced Layers
(`src/determinism.ts`, `test/journal.test.ts`) so business logic replays
identically without special ctx calls. AC-2 confirms identical input ⇒ identical
`op-index → (kind,name)` sequence.

## Q6 — in-stream snapshot atomicity (`trim` + snapshot record)

**Resolved for the single-record recipe.** `snapshot` appends a `kind=snapshot`
record at `match_seq_num = cursor`, then `trim(cursor)` so the snapshot becomes the
new head; recovery folds from the snapshot, not from zero (`test/acceptance.test.ts`
AC-5, `test/journal.test.ts`). The emulation makes the snapshot-append and trim two
calls; true single-batch atomicity is an SDK concern. Framed (>1 MiB) snapshots are
explicitly deferred per the SDD.

---

## Biggest delta from the SDD — lease issuance must be journal-monotonic

The SDD's fence model assumes a restarting/overlapping worker leases with a
*higher* epoch than the one it replaces ("keep the fence even with one worker — it
covers the deploy-overlap window"). Seeding each worker's epoch from `Date.now()`
**does not guarantee this**: two workers started within the same millisecond (a
fast crash-restart, exactly the §9 fault harness) mint *colliding* lease tokens. A
dead worker can leave behind a fence token *higher* than the newcomer's first
lease, so the live worker fails its conditional appends (`412 fence-mismatch`),
loses its lease to a corpse, and re-runs side effects until its local counter
climbs past the stale fence.

This was caught by the fault harness: AC-1's "executed-but-not-appended" case
intermittently re-ran `chargeCard` a third time. The exactly-once **invariant still
held** (`charged === 1`, via the idempotency key) — but the wasted re-execution
and potential live-lock are real.

**Fix applied:** leases are acquired *above the journal's current fence*
(`acquireLease` in `src/runtime.ts`, backed by a new `S2.checkFence`):
`epoch = max(localCounter, currentFence + 1)`. The journal — the single source of
truth — becomes the coordination point for lease monotonicity, exactly in the
spirit of "the journal is the only source of truth." With it, restarts are
deterministic and side effects re-run at most once.

This is the spike's headline finding: **fencing tokens cannot be minted from
wall-clock alone; they must be derived from the durable fence the journal already
holds.** Any multi-worker successor must preserve this property (it generalises to
"read-fence-then-lease-strictly-above" under contention, where `match_seq_num`
remains the tiebreaker).

## Other notes

- **Side effects are exactly-once *journaled*, at-least-once *executed*.** A step
  whose result landed in the journal never re-runs (replay short-circuits); a step
  that ran but whose append did not land re-runs on resume. The §8 invariant
  ("charge counter == 1") therefore requires effects to be idempotent on a stable
  key derived from `(execId/input, op)` — demonstrated in `test/demo.ts`. This is
  the standard durable-execution contract, not a defect.
- **Dispatch is intentionally not deduped.** A dropped poke is a lost wakeup; since
  `tick` is idempotent, a redundant poke is only a wasted fold. Poke sources are
  bounded (timer fire, event resolution, inbox watcher), so it cannot busy-loop.
- **`runLoop` must let interrupts through.** Swallowing *all* tick causes (to
  survive a bad tick) also swallowed interrupts, so a "crashed" worker kept
  running. It now re-raises interrupt-bearing causes so `dispose` actually stops
  the host (`Cause.hasInterrupts`).
- **Inbox→poke translation.** External event resolution writes the unfenced inbox;
  a detached follow-read on the inbox translates that write into a host poke
  (`watchInbox`), so resolution works even from a separate process and even when
  the event is delivered before its `waitForEvent` registers (AC-4).
- **Codec is throwaway.** Journal records use a self-describing JSON envelope, not
  S2 headers + typed bodies; arbitrary step results/errors are JSON round-tripped
  (lossy for class instances). Production would put `kind`/`op`/`name` in S2 record
  headers and use Schema-typed per-step codecs.
