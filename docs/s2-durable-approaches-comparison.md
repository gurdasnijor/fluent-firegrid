# Two ways to build durable execution on S2 — a comparison

**Status:** analysis / findings · Compares **PR #16** (`@firegrid/fluent-s2-durable`,
this spike — a from-scratch runtime) against **PR #15**
(`@firegrid/fluent-s2-workflow-engine` — an `effect/unstable/workflow` backend).

The goal here is *understanding*, not picking a winner: both validate the spike's
core hypothesis (S2's primitives are sufficient for durable execution — no
Kafka/SQS/Redis/etcd/RPC). They differ mainly in **how much of the runtime they
build vs. delegate**, and in a few concrete S2 techniques worth standardizing on.

---

## The one structural difference that explains most of the rest

| | PR #16 (this spike) | PR #15 |
|---|---|---|
| What it builds | the **runtime** from scratch | a **persistence backend** for Effect's runtime |
| `@effect/workflow` | **forbidden** by the SDD | **implements** `WorkflowEngine.Encoded` (`effect/unstable/workflow`) |
| Owns replay / op-identity / suspend / determinism | **yes** (rebuilt) | **no** — Effect's `WorkflowEngine`/`WorkflowInstance` owns it |
| Owns S2 persistence + fencing | yes | yes |
| Lines (src) | ~1.0k | ~0.7k |

Because PR #15 implements Effect's official engine *backend* (`register` /
`execute` / `poll` / `interrupt` / `activityExecute` / deferred / clock), the hard
runtime problems are **Effect's code, not theirs**. The SDD for this spike
explicitly said *"No dependency on `@effect/workflow`"*, so PR #16 rebuilt the
runtime — and therefore had to solve those problems directly.

So most "issues" surfaced by this spike aren't *circumvented* by PR #15 — they
live one layer down, inside Effect's engine.

### Issue-by-issue

| Issue surfaced by this spike | PR #16 | PR #15 |
|---|---|---|
| Op identity (positional index breaks under concurrency — SDD Q5) | solved by **name-keying entries** | Effect's engine; activities keyed `executionId/activityName/attempt`, deferreds/timers by name — **same name-keying model** |
| Suspend as a defect (Q4) | `Effect.die(Suspend)` caught at host | Effect's `Workflow.Suspended` result, persisted |
| Divergence detection (AC-2) | `DivergenceError` (loud defect) | Effect's engine |
| Deterministic Clock/Random (§5.4) | seed-sourced Layers | Effect's engine |
| Replay / activity memoization | `ctx.run` short-circuit on fold | backend persists `ActivityCompleted`; fold replays first-wins |

**Takeaway:** name-keying (the headline design change in PR #16) is independently
confirmed as correct — it is literally how Effect's `WorkflowEngine` addresses
activities. The other runtime concerns are real, but in PR #15 they're Effect's
responsibility.

---

## S2 techniques PR #15 uses that this build should adopt

These are genuine improvements, independent of which path you take.

### 1. `ignoreCommandRecords: true` on read

The S2 SDK can **natively skip `fence`/`trim` command records** when reading:

```ts
stream.read({ start: { from: { seqNum }, clamp: true }, ignoreCommandRecords: true }, { as: "string" })
```

PR #16 hand-filters command records by header (`["", "fence"]` / `["", "trim"]`).
The flag is the right way. (The underlying fact still holds — command records
consume sequence numbers, so the physical tail ≠ the logical record count, and
`match_seq_num` must come from `checkTail` — the flag just removes the manual
filtering.)

### 2. `matchSeqNum`-gated fence acquisition — the clean answer to lease monotonicity

This is the most valuable technique to adopt. PR #15 acquires the fence as a
**conditional append at the current tail**:

```ts
const tail = yield* stream.checkTail()
const input = AppendInput.create([AppendRecord.fence(token)], { matchSeqNum: tail.tail.seqNum })
// SeqNumMismatchError ⇒ someone else fenced first ⇒ "raced"
```

So the **fence token is just an owner-id**, and *who wins ownership* is decided by
S2's conditional append, not by comparing token values. This **completely avoids**
PR #16's "read the current fence and mint a strictly-higher epoch"
machinery (the lease-monotonicity finding).

It's also the concrete realization of the Restate framing
([*every system is a log*](https://www.restate.dev/blog/every-system-is-a-log-avoiding-coordination-in-distributed-applications)):
*the conditional append **is** the lock.* There is no lock service and no epoch
arithmetic — whoever wins the `match_seq_num` race at the fence position owns the
stream; a superseded writer's later appends fail `FencingTokenMismatchError` and
it stops.

### 3. Paginated `readAll`

PR #15 loops read batches to the tail, handling S2's ~1000-record bounded-read
cap. PR #16 reads once (flagged the cap but didn't loop).

---

## Pieces PR #15 is missing that this build has

- **Bounded replay (snapshots).** PR #15 has **no snapshot/trim** — replay cost
  grows with the stream. PR #16 implements S2's atomic single-record
  snapshot-and-follow (`[trim-command, snapshot-record]` in one batch) and the
  fold reseeds from the snapshot.
- **Schema-first codec.** PR #15 uses plain `JSON.parse`/`stringify`; PR #16 uses
  `Schema.fromJsonString` over `Schema.TaggedClass` records (the effect-ts skill
  bans `JSON.parse`).
- **Independence from `@effect/workflow`.** PR #16 owns the primitives end-to-end,
  so it can be shaped directly toward the target user API
  ([restate-fluent](https://github.com/restatedev/sdk-typescript/tree/main/packages/libs/restate-sdk-gen))
  without inheriting Effect's workflow model.

---

## The strategic fork

Both paths validate S2 as the substrate; neither needs heavier infra. The choice
is about *who owns the durable-execution model*:

- **Path A — `WorkflowEngine` backend (PR #15).** ~700 lines. Effect owns replay,
  op-identity, suspend/resume, determinism; you own persistence + fencing. Fast to
  stand up; you live inside Effect's workflow abstraction and inherit its
  capabilities and limits.
- **Path B — runtime from scratch (PR #16).** Full control of the primitives and
  the journal format; you can grow the ergonomic combinator API directly on top.
  More code, and you re-discover everything Effect's engine already does.

Fencing is identical in both (epoch-tagged conditional append); PR #15 just
*acquires* it more cleverly.

---

## Concrete adoptions for this build (Path B), regardless of the fork

1. Switch reads to **`ignoreCommandRecords: true`** and drop the manual header filter.
2. Replace `acquireLease`'s "mint above current fence" with **`matchSeqNum`-gated
   fence acquisition** (`SeqNumMismatchError` ⇒ raced/lost). Removes the
   monotonic-epoch logic entirely.
3. **Paginate `read`/`fold`** to the tail (handle the 1000-record cap).
4. Keep the snapshot-and-follow and the Schema codec (PR #15 lacks both).

These would put PR #16 and PR #15 on equal footing for the S2-persistence layer,
leaving the only real difference the intended one: build-the-runtime vs.
back-Effect's-engine.
