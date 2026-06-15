# Handoff — `@firegrid/fluent-s2-durable`

For the next agent picking this up. Read this first, then `FINDINGS.md`, then the
two design docs. **Branch:** `worktree-s2-durable` · **PR:** #16.

---

## TL;DR

A working spike of **replay-based durable execution on S2** (Effect-native, no
`@effect/workflow`), validated against a **real `s2 lite` server**. 14 tests green
(M0/M1 units + AC-1…AC-6), all repo gates pass. The runtime works but its public
surface is a grab-bag; a **factoring refactor is designed but not implemented**
(`docs/log-interface-sketch.md`). The spike's hypothesis is **confirmed**: S2's
primitives (sequenced log + conditional append + fencing + trim + follow reads)
are sufficient for durable execution with **no Kafka/SQS/Redis/etcd/RPC**, at
single-worker scale.

---

## Where things are

```
packages/fluent-s2-durable/
  src/
    errors.ts        — tagged errors (AppendCondFailed, S2Error, LostLeaseError, DivergenceError, …)
    record.ts        — §4.3 entries as Schema.TaggedClass + Schema.fromJsonString codec (name-keyed)
    journal.ts       — fold(records) → Journal (byName HashMap); the StateMachine
    determinism.ts   — seed-sourced deterministic Clock/Random layers
    context.ts       — Ctx (run/sleep/awakeable + state/call/send seams) + defect-based Suspend
    dispatch.ts      — in-memory ready-set (poke/claim)
    timerHeap.ts     — durable-timer arming (forkChild)
    s2.ts            — the `S2` Effect SERVICE (append/read/checkTail) over SDK types
    s2Live.ts        — the ONLY file that imports @s2-dev/streamstore; the adapter
    runtime.ts       — make()→Worker: the host loop (BLURRY — target of the refactor)
  test/
    s2lite.ts        — boots a real `s2 lite` server as a Scope-managed Layer
    harness.ts       — §9 fault injection (kill-points; crash = Fiber.interrupt)
    demo.ts          — the §8 order workflow + idempotent charge/fulfill
    units.test.ts    — M0/M1 (codec, fold, determinism)
    acceptance.test.ts — AC-1…AC-6 against real s2-lite
docs/
  sdds/s2-durable-sdd.md             — the original spec
  s2-durable-approaches-comparison.md — this build (Path B) vs PR #15 (WorkflowEngine backend, Path A)
  log-interface-sketch.md            — THE PROPOSED REFACTOR (Log/Journal/Executor/Scheduler/Engine/Ctx)
packages/fluent-s2-durable/FINDINGS.md — Q1–Q6 + the real-server deltas
```

## Run it

Requires the `s2` CLI on `PATH` (`brew install s2`). Tests spawn their own server.
```sh
pnpm --filter @firegrid/fluent-s2-durable typecheck
pnpm --filter @firegrid/fluent-s2-durable test
# gates: pnpm lint ; pnpm lint:dead (knip) ; pnpm lint:dup (jscpd) ; pnpm lint:deps (depcruise) ; diagnostics
```
The suite has been run 6–8× repeatedly with no flakes. If you see one, see "Gotchas".

---

## Decisions already made — do NOT re-litigate these

1. **Real s2-lite, no fakes.** An earlier in-memory S2 emulation was deleted —
   emulating S2 emulates the thing under test. Everything runs on the real SDK +
   server. (User was emphatic.)
2. **No `@effect/workflow`.** The SDD forbids it; this builds the runtime from
   scratch. (PR #15 takes the *opposite* path — implements Effect's
   `WorkflowEngine` backend. That's the live strategic fork, see below.)
3. **Name-keyed entries**, not a positional op-index. `ctx.run("charge")` etc.
   This is what lets durable ops compose under Effect concurrency and dissolves
   SDD Q5. (Confirmed: it's how Effect's own engine keys activities.)
4. **Conditional-append fencing**, not monotonic epochs. `acquireLease` is a
   `matchSeqNum`-gated fence append; the token is a plain owner-id. Transient
   `position-taken` is **retried** (a crashed worker's in-flight append still
   lands — interruption can't cancel an in-flight HTTP write); only persistent
   contention is `LostLeaseError`.
5. **S2 service carries SDK types** (`AppendRecord`/`ReadRecord`, `number`
   positions). Earlier custom `S2Write`/`S2Record` were removed. Errors stay our
   tagged ones (needed for `catchTag`). NOTE: the refactor sketch (§below)
   *re-introduces* a principled re-model (`Log` port with branded `SeqNum` etc.) —
   that's a deliberate reversal toward a legible seam, not a contradiction.
6. **Idiomatic Effect** per the effect-ts skill: Schema-first records, `Match.tag`/
   `Schema.is`, `HashMap`/`Option`/`Array`, `@effect/vitest` (`it.effect`/`layer`),
   no `JSON.parse`, no `Effect.runPromise` in tests, no for-loops in src.

## Validated findings (the spike's output — see FINDINGS.md for detail)

- Q1: `fencingToken` AND `matchSeqNum` on one append — **yes** (real SDK).
- `fence`/`trim` are **real records that consume seq numbers** → `match_seq_num`
  comes from `checkTail`, never the fold; the bounded read paginates by *physical*
  seq and filters commands in code (per-tick fencing makes commands dense, so
  `ignoreCommandRecords` alone can't resume past an all-command window — it stays
  only on the follow read).
- `trim` is **eventual**; `checkTail`/`read` **404** on a never-appended stream;
  reads beyond the tail throw `RangeNotSatisfiableError`.
- Side effects are exactly-once **journaled**, at-least-once **executed** →
  idempotency keys required (standard).
- Fencing is the **conditional-append-is-the-lock** pattern (Restate's framing):
  no lock service, no etcd. The "locking" is the log's own admission control.
- **There is no off-the-shelf Effect type for our log** — `EventJournal` is
  UUID-keyed event-sourcing (no positional CAS / fencing); `KeyValueStore`/
  `PersistedQueue` aren't ordered logs. The type-aligned move is to *define* a
  `Log` port (sketched), not borrow one.

---

## The proposed next step — implement the factoring (`docs/log-interface-sketch.md`)

`runtime.ts` fuses Executor + Scheduler + Journal-wiring + Ctx-wiring + host-loop
into one closure; the public surface (`Worker`: start/tick/resolveEvent/awaitResult/
boot/snapshot/runLoop) is a grab-bag. The sketch splits it into named
`Context.Service`s:

```
Log (S2 port) ◀ Journal (fold) ◀ Executor (advance 1 exec, emits StepResult)
                                  Scheduler (which/when; in-memory)
                                  Engine (compose + lifecycle: submit/signal/result/run)
                                  ◀ Ctx (run/sleep/awakeable)
```

Key artifact: **`StepResult`** (`Idle | Completed | Suspended({timers, awaiting})`)
— the Executor emits it; the Engine/Scheduler react. The full interfaces, the
9-responsibility taxonomy (control-plane vs data-plane; #4 Run is the future
split seam), and the `Layer.provide` wiring are in the sketch.

**Suggested implementation order:**
1. Introduce `Log` (`src/log.ts`) — branded `SeqNum`/`FenceToken`/`LogId`, `Write`
   taggedEnum, `Conflict`/`LogError`, the `Log` service. Make `s2Live.ts` provide
   it. (This re-abstracts the S2 boundary; keep `s2Live` as the sole SDK importer.)
2. Re-point `journal.ts` `fold` at `LogRecord` bodies (trivial).
3. Pull `Executor` out of `runtime.ts` — `start` + `step(id) → StepResult`. Pure of
   scheduling. This is most of today's `tick`.
4. Unify `dispatch.ts`+`timerHeap.ts` into `Scheduler` (submit/ready/armTimer).
5. `Engine` = the pump + `submit`/`signal`/`result`/`run`, over Log+Executor+Scheduler.
6. Move tests from driving `Worker` to driving `Engine`; harness wraps `Log`.

Do this as its own commit series; keep the 14 tests green throughout.

---

## Strategic forks still open (get user intent before big moves)

- **Path A vs Path B.** Keep building our own runtime (Path B, current) OR adopt
  Effect's `effect/unstable/workflow/WorkflowEngine` and become a backend over S2
  (Path A = what PR #15 did). The comparison doc covers this. User has NOT chosen;
  they reacted negatively to "just use WorkflowEngine" as a dodge.
- **The end-game** is an Effect-native `restate-sdk-gen`-style combinator API
  (Operation/Future/all/race/spawn) on top of these primitives — see the SDD
  discussion. The structured-concurrency layer is *just Effect*; this package only
  provides the journal-backed primitives. Not started.
- **Fleet / multi-worker.** Out of scope; the split seam is `Executor.step` (#4).
  Ownership across a fleet = a registry (the user's `STATE-PROTOCOL.md`) + the
  fence — the registry decides who *should* own, the fence enforces who *gets to
  write* (the two are different layers; don't conflate). See the fencing discussion.
- **`ctx.state` / `ctx.call` / `ctx.send`** are typed seams that currently `die`.
  Build when the API layer needs them.
- **Hosted-basin run** — never executed (no basin available). The `S2`/`Log`
  service is the seam for it.

## Gotchas (non-obvious things that will bite you)

- **`@effect/vitest` injects a `TestClock`** by default → freezes `Effect.sleep`/
  `Schedule.spaced`, hanging the s2-lite readiness probe and all durable timers.
  Tests use `layer(S2LiteLive, { excludeTestServices: true })`. Keep that.
- **A "crashed" worker's in-flight append still lands** (interruption can't cancel
  an HTTP write in flight) — this is why `acquireLease` retries transient
  `position-taken`. It's also the realistic "appended-but-not-acked" case.
- **`runLoop` must re-raise interrupts** (`Cause.hasInterrupts`) — swallowing all
  causes kept a "crashed" worker running. Crash = `Fiber.interrupt`; background
  fibers are `forkChild` (structured) so they cascade.
- **Dispatch is intentionally NOT deduped** — a dropped poke is a lost wakeup;
  `tick` is idempotent so a redundant poke is only a wasted fold.
- **effect-smol API differs from mainline**: `Effect.callback` (not `async`),
  `Effect.service(Tag)` (yielding the tag directly errors), `Array.filterMap` takes
  a `Result`-returning fn (use `Array.flatMap` with `[]`/`[x]`), `Schema.Union([...])`
  takes an array, `Data.taggedEnum`, `Layer.effect` (no `Layer.scoped`),
  `Order.String` (capital). Grep existing code before guessing.
- **Lint is strict** (`--max-warnings 0` + custom `local/no-date-now`,
  `no-for-of-in-source`, `effect/no-pipe-first-arg-call`, the eslint-effect
  `no-restricted-syntax` that flags `Effect.die`). `Effect.die` for Suspend/
  divergence needs an eslint-disable with justification. `pipe(fn(x), …)` is
  flagged — bind to a const first.

## Definition of done for the spike (per the SDD §11)

1. ✅ The package (S2 service, Journal, Ctx, deterministic Clock/Random, runtime).
2. ✅ Demo workflow + fault harness wired to AC-1…AC-6.
3. ✅ Findings note (Q1–Q6) — `FINDINGS.md`.

The spike is **complete against its charter**. Everything beyond is the
refactor (legibility) and the forward roadmap (API layer / fleet), which are
choices, not unfinished spec.
