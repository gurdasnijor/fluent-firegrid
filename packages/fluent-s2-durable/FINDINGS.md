# Spike findings — S2-backed durable execution (`s2-durable`)

Deliverable §11.3. What was verified, and where a primitive behaved differently
than the SDD assumed. The deltas are the spike's most valuable output.

> **Substrate:** every test runs against a **real `s2 lite` server** (in-memory
> object store), spawned per suite (`test/s2lite.ts`), driven through the **real
> S2 TS SDK** (`@s2-dev/streamstore`) wrapped by `src/s2Live.ts`. There is **no
> in-memory emulation of S2** — S2 is our Bifrost (see below), and faking it would
> fake the thing under test. The one un-exercised path is a hosted basin; the
> `S2` service is the seam for it.

## Restate / Bifrost alignment

This package is the durable-primitive substrate for an Effect-native
[`restate-sdk-gen`](https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk-gen)-style
API. The mapping that drove several design decisions:

| Restate | Here |
|---|---|
| **Bifrost** (replicated WAL; LSN; loglet providers in-mem/local/S3) | **S2** (`s2.ts` interface, `s2Live.ts`). `seqNum` = LSN; `s2 lite` storage flags = loglet provider |
| **StateMachine** (interpret entries → state) | `journal.ts` fold (`Match.tag` over Schema records) |
| **entries / commands** | `record.ts` (`Step`/`TimerSet`/`Awakeable`/…) |
| **journal** (per-invocation log) | one S2 stream `wf/{execId}` |
| **Leader / leadership** | `acquireLease` + S2 fencing token |
| **Invoker** | `Dispatch` |
| SDK `ctx.run`/`sleep`/`promise`/`state`/`call` | `Ctx` (`run`/`sleep`/`awakeable` built; `state`/`call`/`send` typed seams) |

The structured-concurrency layer (`all`/`race`/`spawn`) is **not rebuilt** — in an
Effect-native world it *is* Effect (`Effect.all`/`race`/`fork`). The infra only
provides the journal-backed primitives as plain `Effect`s.

## Q1 — `fencing_token` AND `match_seq_num` on the same append

**Resolved empirically: yes.** `AppendInput.create(records, { matchSeqNum, fencingToken })`
carries both, and the real server enforces fence first, then seq. The runtime
presents both on every journal write. A `412` surfaces as one of two distinct
SDK errors — `FencingTokenMismatchError` (`expectedFencingToken` = current fence)
or `SeqNumMismatchError` (`expectedSeqNum` = real tail) — which map cleanly to our
`AppendCondFailed` `reason`. Verified by AC-6 and AC-1 against the live server.

## Q2 — append vs read sessions / Effect `Sink`/`Stream` mapping

**Resolved.** Reads are `Stream` (`S2Stream.read` for bounded, `readSession` —
an `AsyncIterable` — for `follow`, wrapped with `Stream.fromAsyncIterable`).
`append` is a discrete conditional `Effect` returning the new tail, not a
held-open `Sink` — the better fit since every write needs its own `match_seq_num`
and a typed `412` outcome.

## Q3 — append session poisoned on `412`, re-establishment

**Resolved by construction.** Because `append` is a discrete `Effect`, a `412`
(`AppendCondFailed`) is an ordinary typed failure; it cannot wedge a writer. The
runtime distinguishes the two flavours: fence-mismatch ⇒ `LostLeaseError`, abandon
the tick; position-taken ⇒ idempotent resume (re-fold, return the recorded step).

## Q4 — suspend-via-defect vs error-channel

**Resolved: defect.** `Suspend` and `DivergenceError` are raised with `Effect.die`
and caught via `Exit` + `Cause.findDefect`. AC-2 confirms a replay mismatch fails
loudly through the defect channel, which user-level `catchAll` (the typed error
channel) cannot intercept.

## Q5 — replay determinism under Effect concurrency — **dissolved**

The SDD constrained the spike to "sequential `Effect.gen` only" because a global
positional op-index is non-deterministic under `Effect.all`/`fork`. **Resolved
differently: journal entries are keyed by a stable *name*, not a position**
(`ctx.run("charge")`, `ctx.sleep("cooloff")`, `ctx.awakeable("approval")` — exactly
how the restate examples name every entry). Name-addressing makes replay
order-independent, so durable ops compose under Effect concurrency. Divergence
(AC-2) becomes "same name, different record kind". This is the property the
user-facing combinator layer is built on, and it removes the Q5 limitation rather
than working around it.

## Q6 — in-stream snapshot atomicity (`trim` + snapshot record)

**Resolved, with a real-server caveat.** `snapshot` appends a `Snapshot` record at
`matchSeqNum = checkTail`, then `trim(cursor)`. The fold reseeds `byName` from the
`Snapshot` record, so recovery is bounded by the snapshot regardless of physical
truncation. **Finding:** on the real server **`trim` is eventual** — records below
the cursor remain readable immediately after — so "bounded replay" is a *logical*
property of the fold (it resets at the `Snapshot`), not an immediate physical
truncation. AC-5 asserts the fold-reseed, not instant truncation.

---

## Deltas the real server forced (the spike's most valuable output)

1. **`fence`/`trim` are real records that consume sequence numbers.** They appear
   on read with `["", "fence"]` / `["", "trim"]` headers. Consequences: the fold
   *skips* them; the physical `match_seq_num` is read from `checkTail`, never
   inferred from journal-record seq numbers (the two diverge); `checkFence` derives
   the current lease from the latest fence record. The original SDD treated tail as
   `fold.tail` — that's wrong once fencing writes records.

2. **`checkTail`/`read` 404 on a never-appended stream.** `create_stream_on_append`
   only fires on *append*. The live layer maps 404 → empty/tail-0; genesis is keyed
   off "no `Seed` in the fold", not "physical tail == 0" (a fence record already sits
   at seq 0 by then).

3. **Lease tokens must be journal-monotonic, not wall-clock.** Two workers started
   in the same millisecond mint colliding leases, and a dead worker can leave a
   fence *higher* than a fast-restart's first lease — locking the live worker out.
   `acquireLease` reads the current fence and mints `max(local, fence+1)`, so the
   journal is the coordination point (Restate leadership ≈ Bifrost sealing). Caught
   by the fault harness; without it AC-1 intermittently re-ran the side effect.

4. **Side effects are exactly-once *journaled*, at-least-once *executed*.** A step
   whose result landed never re-runs (replay short-circuits); one that ran but
   didn't land re-runs on resume. The §8 invariant ("charge counter == 1") therefore
   requires idempotency on a stable key — standard durable-execution semantics,
   demonstrated by AC-1a (no re-run) and AC-1b (re-run, deduped).

5. **`runLoop` must let interrupts through.** Swallowing *all* tick causes (to
   survive a bad tick) also swallowed interrupts, so a "crashed" worker kept running.
   It re-raises interrupt-bearing causes (`Cause.hasInterrupts`); a crash is then
   `Fiber.interrupt`, and structured children (`forkChild` timers/inbox-watchers)
   cascade.

6. **`@effect/vitest` injects a `TestClock` by default**, which freezes
   `Effect.sleep`/`Schedule.spaced` — the readiness probe and every durable timer
   need real time. Tests use `excludeTestServices: true`.

## Other notes

- **Codec is Schema-first.** Records are `Schema.TaggedClass`; the wire codec is
  `Schema.fromJsonString` (no hand-rolled `JSON.parse`). Opaque payloads (step
  results, input, event values) are `Schema.Unknown` — genuinely unconstrained at
  this layer since the SDK is generic over them.
- **Dispatch is intentionally not deduped.** A dropped poke is a lost wakeup; since
  `tick` is idempotent a redundant poke is only a wasted fold. Poke sources are
  bounded (timer fire, event resolution, inbox watcher), so it cannot busy-loop.
- **Inbox→poke translation.** External event resolution writes the unfenced inbox;
  a `forkChild` follow-read translates that write into a host poke, so resolution
  works from a separate worker and even when delivered before its `awaitable`
  registers (AC-4).
