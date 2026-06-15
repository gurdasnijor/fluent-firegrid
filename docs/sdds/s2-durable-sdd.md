# Spike SDD — S2-Backed Durable Execution Runtime (Effect)

**Status:** Spike / draft · **Working name:** `s2-durable` · **Substrate:** [S2](https://s2.dev) + Effect-TS · **No** dependency on `@effect/workflow`

---

## 1. Purpose & hypothesis

Validate, with a throwaway-quality but correct implementation, that S2's two concurrency primitives are sufficient to build replay-based durable execution in Effect — exactly-once steps, crash-safe replay, durable sleep/await — with **zero non-S2 infra** (no SQS/Redis/etcd) at single-worker scale.

> **Hypothesis under test:** A stream-per-execution journal, with `fencing_token` as the executor lease and `match_seq_num` as the exactly-once append guard, gives us Restate/Inngest/CF-Workflows semantics. Dispatch and timers live in worker memory and are reconstructed by folding S2 on restart. The journal is the only source of truth.

The spike succeeds if the acceptance tests in §8 pass against `s2-lite` (local) and one hosted basin. It fails fast if any open question in §3 has no clean answer.

## 2. Goals / Non-goals

**Goals**
- `ctx.run` (durable step), `ctx.sleep` (durable timer), `ctx.waitForEvent` (durable await/awakeable).
- Exactly-once side effects across crashes; deterministic replay with divergence detection.
- Single-process host loop with in-memory dispatch + timer heap, rebuilt by folding S2 on boot.
- Snapshot-and-follow to bound replay cost.
- Everything behind Effect `Layer`s so the multi-worker version drops in later untouched.

**Non-goals (do not build)**
- Multi-worker / sharding / membership / failover protocol.
- Durable RPC between executions (`ctx.call`), one-way `ctx.send`.
- Virtual-Object K/V state (`ctx.set/get`) beyond a stub.
- Production trimming/GC policy, observability, backpressure tuning, payloads > 1 MiB (framed snapshots).
- Auth/security, multi-tenant routing.

## 3. Open questions to resolve (de-risking targets)

The spike must produce a short findings note answering each. These are the real risk; resolve them before milestone work where flagged.

| # | Question | Why it's risky | Resolution path |
|---|----------|----------------|-----------------|
| Q1 | Can the S2 TS SDK set **`fencing_token` AND `match_seq_num` on the same append**? | Whole journal-write safety model assumes both as preconditions on one write. | Read SDK append signature; test empirically. **Fallback if not combinable:** `match_seq_num` alone secures single-writer-at-tail — two writers compute the same expected seq, first wins, second gets `412`. So mandate `match_seq_num` on *every* journal write (load-bearing) and use `fencing` set-once-on-lease as the coarse zombie-stopper. Document which path we took. |
| Q2 | How does the SDK surface **append sessions vs read sessions**, and do they map to Effect `Sink`/`Stream` incl. backpressure? | `effect-durable-streams` ergonomics depend on this. | Wrap both in the `S2` service (§5.1); confirm a session is re-establishable. |
| Q3 | **Append session is poisoned on a `412`** — does our `Sink` wrapper re-establish cleanly mid-run? | A conditional failure must not wedge the writer. | On `412`, tear down + recreate the append session/`Sink`; surface as a typed error, retry from re-fold. |
| Q4 | **Suspend-via-defect vs error-channel** in Effect — interaction with interruption and user `catchAll`. | If user step code can swallow the suspend signal, resume breaks. | Prefer `Suspend` as a **defect** (`Effect.die`) caught at host via `Effect.catchAllDefect`/`Effect.sandbox`, so user-level `catchAll` can't intercept it. Validate against Effect's interruption semantics; fall back to a runtime-private error channel the handler type forbids catching. |
| Q5 | **Replay determinism under Effect concurrency** (`Effect.all`, `fork`). | Concurrent durable ops get non-deterministic op-index → replay divergence. | Spike constraint: **sequential `Effect.gen` only**. Concurrent durable ops are out of scope (need branch-keyed op-index). Assert single-fiber op issuance. |
| Q6 | In-stream snapshot **atomicity** of `trim` command + snapshot record in one batch under our codec/size. | Recovery-from-head correctness depends on it. | Follow the single-record snapshot recipe (atomic batch, `match_seq_num = cursor`); test head-read recovery. Framed path explicitly deferred. |

## 4. Architecture

### 4.1 Component map

```
                 ┌────────────────────────── worker process ──────────────────────────┐
                 │                                                                      │
  poke (in-mem)  │   Dispatch(in-mem set) ──▶ Runtime.tick ──▶ lease+fold+run handler  │
  timer fire ───▶│   TimerHeap(in-mem, 1x setTimeout)                  │               │
                 │                                                     ▼               │
                 │                                              WorkflowContext (ctx)  │
                 │                                                     │ append/read   │
                 └─────────────────────────────────────────────────── │ ──────────────┘
                                                                       ▼
                                                            S2 service (Layer over TS SDK)
                                                       append · read(follow) · checkTail · fence · trim
                                                                       │
                                                              s2://{basin}/wf/{execId}
```

Memory holds only *caches*. Truth is the S2 journal. On boot: enumerate active executions, fold each, re-arm timers/awaits. A timer whose `fireAt` elapsed during downtime fires immediately on recovery (durable sleep = "at least", not "at exactly").

### 4.2 Stream layout (per execution)

- `wf/{execId}` — the fenced journal (single writer = current lease holder).
- `wf/{execId}/inbox` — **unfenced** external-input stream (many writers); the lease holder folds it into the journal under its fence. Used for `waitForEvent` resolution.

### 4.3 Record taxonomy

Discriminate on a `kind` header (S2 headers are arbitrary bytes). `op` = logical op-index (§6.1). Body = codec-encoded payload.

```
kind=lease-fenced    epoch                                  # bookkeeping; fence command also issued
kind=seed            body=seed                              # deterministic clock/random
kind=step            op,name      body=result|error         # ctx.run
kind=timer-set       op,name      fireAt                    # ctx.sleep
kind=timer-fired     op
kind=awakeable       op,name      id                        # ctx.waitForEvent
kind=awakeable-done  op           body=payload              # folded from inbox
kind=snapshot        covers       body=state                # snapshot-and-follow
kind=completed       body=result|error
```

## 5. Core interfaces (Effect)

Sketches, not final. Agent fills implementations.

### 5.1 `S2` service — thin Layer over the S2 TS SDK

```ts
class S2 extends Context.Tag("S2")<S2, {
  readonly append: (
    stream: string,
    records: ReadonlyArray<Rec>,
    opts: { fencingToken?: string; matchSeqNum?: bigint }
  ) => Effect.Effect<{ tail: bigint }, AppendCondFailed | S2Error>;

  // read session as a Stream; `from` is a seq_num; `follow` keeps it open at the tail
  readonly read: (
    stream: string,
    from: bigint,
    opts?: { follow?: boolean }
  ) => Stream.Stream<Rec, S2Error>;

  readonly checkTail: (stream: string) => Effect.Effect<bigint, S2Error>;
  readonly fence:     (stream: string, token: string) => Effect.Effect<void, S2Error>; // fence command record
  readonly trim:      (stream: string, upTo: bigint) => Effect.Effect<void, S2Error>;  // trim command record
}>() {}
```

`AppendCondFailed` carries the structured `412` body (current fencing token / assignable seq) so callers distinguish *lost-lease* from *already-written* (Q1).

### 5.2 `Journal` — fold + codec

```ts
interface Journal {
  readonly byOp: ReadonlyMap<number, Rec>; // op-index → record (steps/timers/awaitables)
  readonly tail: bigint;                    // physical next seq (for match_seq_num)
  readonly seed: Uint8Array;                // recorded determinism seed
  readonly status: "running" | "completed";
}
// fold(stream): Stream<Rec> → Journal   (read from snapshot cursor, then deltas)
```

### 5.3 `WorkflowContext` (the SDK surface)

```ts
interface Ctx {
  run<A, E>(name: string, effect: Effect.Effect<A, E>): Effect.Effect<A, E | WfError>;
  sleep(name: string, duration: Duration.Duration): Effect.Effect<void, WfError>;
  waitForEvent<A>(name: string, opts?: { schema: Schema<A> }): Effect.Effect<A, WfError>;
  // deterministic time/random provided via swapped Clock/Random layers (§5.4), not ctx methods
}
type Handler<I, O> = (ctx: Ctx, input: I) => Effect.Effect<O, WfError>;
```

### 5.4 Deterministic `Clock` / `Random`

Provide replay-deterministic `Clock` and `Random` **Layers** sourced from the journal `seed`, so ordinary Effect `Clock.currentTimeMillis` / `Random.next` inside handler code are automatically durable without special ctx calls. Seed is appended once (`kind=seed`) on first execution and replayed thereafter.

### 5.5 Host runtime + in-mem dispatch/timers

```ts
class Dispatch extends Context.Tag("Dispatch")<Dispatch, {
  poke: (execId: string) => Effect.Effect<void>;     // add to ready-set
  claim: () => Effect.Effect<string>;                // next ready execId (blocks)
}>() {}

class TimerHeap extends Context.Tag("TimerHeap")<TimerHeap, {
  arm:   (e: { fireAt: number; execId: string; op: number }) => Effect.Effect<void>;
  // single setTimeout to the nearest entry; on fire → append timer-fired + Dispatch.poke
}>() {}

// Runtime.run: loop { execId = Dispatch.claim(); tick(execId) }
// tick: fence(new epoch) → fold → run handler → on Suspend record+release → on Completed finalize
```

Single-worker in-mem implementations of `Dispatch`/`TimerHeap`. Keep the fence even with one worker — it covers the deploy-overlap window (old process not fully exited when new one starts).

## 6. Mechanics

### 6.1 op-index vs seq_num (do not conflate)

- **op-index** — per-execution monotonic counter incremented in handler order, stored in headers. Drives replay matching + divergence detection.
- **S2 `seq_num`** — physical, S2-assigned across *all* records. Used only as `match_seq_num` (the current `Journal.tail`).

Fold builds `op-index → record`. A new live-edge append uses `matchSeqNum = journal.tail`.

### 6.2 Live-edge append

```ts
// inside ctx.run, live edge only (no recorded result at this op-index):
const result = yield* effect;
yield* s2.append(
  wf(execId),
  [{ kind: "step", op: i, name, body: encode(result) }],
  { fencingToken: lease, matchSeqNum: journal.tail }   // Q1: both if combinable, else match only
);
```
On `AppendCondFailed`: if fence mismatch → **lost lease**, fail the tick (another owner has it). If position taken → **already written**, read that op's record and return it (idempotent resume). Re-establish the append session before any further write (Q3).

### 6.3 Suspend / resume (Effect-specific)

Wait-ops with no recorded result do **not** block — they raise `Suspend` (as a defect, Q4) carrying the records to schedule; the stack unwinds to the host.

```ts
sleep(name, duration) = Effect.gen(function* () {
  const i = nextOp();
  const rec = journal.byOp.get(i);
  if (rec?.kind === "timer-fired") return;                 // resumed past it
  yield* Effect.die(new Suspend([{ kind: "timer-set", op: i, name, fireAt: now + ms }]));
});

// host tick:
Effect.gen(function* () {
  const out = yield* handler(ctx, input);
  yield* s2.append(wf(id), [{ kind: "completed", body: encode(out) }], { fencingToken: lease, matchSeqNum: journal.tail });
}).pipe(
  Effect.catchAllDefect((d) =>
    isSuspend(d)
      ? s2.append(wf(id), d.scheduled, { fencingToken: lease, matchSeqNum: journal.tail })
          .pipe(Effect.zipRight(releaseLease(id)))         // record the wait, stop running
      : Effect.die(d)
  )
);
```
Resume = re-fold (now the awaited record is present) and re-run from the top; memoized `ctx.run` results short-circuit so side effects don't repeat. **Optimization (optional):** while connected and the sleep is short, keep the fiber alive and resolve in-process instead of suspending.

### 6.4 Snapshot-and-follow

Periodically: fold to a cursor, append in one atomic batch a `trim` command + `kind=snapshot` record with `matchSeqNum = cursor`; after trim, the snapshot is the new stream head. Recovery folds from the snapshot, not from zero (Q6). External-snapshot variant (cursor in object metadata) is acceptable too; pick one in the spike.

## 7. Milestones (ordered; each independently testable)

- **M0 — S2 service + smoke.** Implement §5.1 over the TS SDK; round-trip append/read/checkTail/fence/trim against `s2-lite`. *Resolves Q1–Q3.*
- **M1 — Journal fold + codec.** Records ↔ bytes; `fold` builds `byOp/tail/seed`.
- **M2 — `ctx.run` only.** Exactly-once step with fence + `match_seq_num`; replay short-circuits; mid-step crash recovers without re-running the effect. *Killer test = AC-1.*
- **M3 — Suspend/resume + `ctx.sleep`.** Defect-based suspend (Q4), in-mem `TimerHeap`, durable sleep across restart. *AC-3.*
- **M4 — `ctx.waitForEvent`.** Inbox stream + external resolution folded into journal. *AC-4.*
- **M5 — Snapshot-and-follow.** Bound replay; recover-from-head. *AC-5.*
- **M6 — Host loop + demo.** Wire Dispatch/TimerHeap/Runtime; make the demo workflow (§8) pass end-to-end including injected crashes.

## 8. Acceptance criteria (the invariant tests)

All against `s2-lite` with a fault-injection harness (§9). The demo workflow:

```ts
const order: Handler<OrderInput, Receipt> = (ctx, input) =>
  Effect.gen(function* () {
    const charge = yield* ctx.run("charge", chargeCard(input));      // side effect w/ external counter
    yield* ctx.sleep("cooloff", Duration.seconds(10));
    const ok = yield* ctx.waitForEvent<boolean>("approval");        // resolved externally
    if (!ok) return { status: "rejected" };
    return yield* ctx.run("fulfill", fulfill(charge));
  });
```

- **AC-1 Exactly-once under crash.** Kill the worker *after* `chargeCard` runs but *before* its record is acked; restart. Assert the external charge counter == 1 and execution completes. (Also test crash *after* ack: still 1.)
- **AC-2 Replay determinism + divergence.** Re-running a completed execution produces an identical `op-index→(kind,name)` sequence. Mutating the handler to issue a different op at index N fails loudly with a divergence error (no silent corruption).
- **AC-3 Durable sleep across restart.** Enter `cooloff`, kill worker, restart with the timer's `fireAt` already elapsed; assert it fires immediately and proceeds.
- **AC-4 Durable await.** Suspend on `approval`; resolve via inbox from a separate process; assert resume with the correct payload. Resolving twice does not double-advance (idempotent via `match_seq_num`).
- **AC-5 Bounded replay.** After a snapshot at cursor C, a fresh fold reads from head (snapshot) and applies only deltas after C; recovery time/records are independent of pre-C history.
- **AC-6 Fence safety (manual two-worker probe).** Point two processes at the same `execId`; the one without the current fence cannot commit a step (gets `412`); no double-commit. (Multi-worker is otherwise out of scope — this is a safety probe only.)

## 9. Testing strategy

- **Local backend:** `s2-lite` single-binary/emulation via endpoint override — no cloud needed for the loop; one hosted-basin run at the end to confirm parity.
- **Fault injection:** explicit kill-points around (a) effect-executed-but-not-appended, (b) appended-but-not-acked, (c) mid-suspend, (d) mid-snapshot. A harness that wraps the host loop and aborts at a named point, then restarts a fresh worker against the same streams.
- **Idempotency probes:** every externally-observable effect increments a counter asserted == 1.
- Determinism asserted by comparing serialized journals across replays.

## 10. Explicitly out of scope (do not gold-plate)

Multi-worker/sharding/membership; `ctx.call`/`send`; VO K/V state; framed (>1 MiB) snapshots; trimming/GC policy; OTel/metrics; backpressure tuning; auth; cross-language codegen. Leave clean `Layer` seams (`Dispatch`, `TimerHeap`, `S2`) so these slot in later without touching the SDK core or handler API.

## 11. Deliverables

1. `s2-durable` package: `S2` service + Layer, `Journal`, `Ctx`, deterministic Clock/Random Layers, single-worker `Runtime`/`Dispatch`/`TimerHeap`.
2. The demo workflow + fault-injection harness wired to AC-1…AC-6.
3. **Findings note** answering Q1–Q6 with what was verified against the SDK/`s2-lite`, and any primitive that behaved differently than this SDD assumes — that delta is the spike's most valuable output.