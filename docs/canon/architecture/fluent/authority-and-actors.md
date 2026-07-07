# Write Authority and the Actor Composition

Doc-Class: canon
Status: active
Date: 2026-07-06
Owner: Firegrid Architecture
Substrate: S2

The unifying interaction under turn streams, lifecycle, wake routing, and
checkpoint election is **write authority**: who may append to a subject's
authoritative log, under what regime, and how that right transfers. Three
codebases arrived at the same shape independently — eff-firegrid's
`{key}/log` + `{key}/in` + `InboxFold`, the encore-ds spike's
stream-per-actor inbox with `insertOrGet` claims and epoch re-keying, and the
green lease/fencing/stale-owner proofs in this repo. This page names it once.

## The Stack

```text
Subject          ordered log            SubjectHistory (ported, proven)
Write Authority  append regime          open-CAS | fenced-owner(epoch) | sealed
Mailbox          external-write intake  inbox subject + admission fold (InboxFold lineage)
Actor            the composition        claim -> drain mailbox -> append own log
Bindings         domain instances       turn, session, object, shard router, timer wheel
```

**The actor is the write authority over its own authoritative log. The process
is only the current holder of the claim.** External writers never append to an
authoritative log — they append to the actor's mailbox; the holder admits
arrivals under its fence with source-sequence provenance, so duplicate sends
fold once.

## Authority Regimes and Laws

- **Open (CAS admission):** anyone may attempt; `appendExpected` at the
  observed tail decides one winner. For admission records, dedupe claims,
  checkpoint election.
- **Fenced owner (epoch):** exactly one live holder; the holder's writes carry
  the epoch's fence token. **Deposal is epoch increment** (re-key / fence
  rotation), not revocation: the old epoch's authority is immortal and
  harmless; a stale holder may compute but cannot commit. Time-based takeover
  is a *policy* layered on top (when to mint the next epoch), never a clock
  baked into the primitive.
- **Sealed:** a terminal record extinguishes authority and closes the subject.
  Appends after seal fail for every holder.

Laws (each already evidenced by a green proof family — leases, live deposed
owner, stale-owner takeover, object serialization, checkpoint race lands with
MS-C1): at most one holder per epoch; stale holders cannot commit; claims are
idempotent per epoch; seal is first-valid-terminal-wins; mailbox admission
dedupes by source provenance.

## What the Bindings Dissolve Into

| Program concept | Actor reading |
| --- | --- |
| Session (managed agent) | The actor; prompts and cancels are mailbox sends |
| Turn stream (MS-C2) | The actor's output log for one execution; seal = terminal |
| Durable cancel (MS-C5) | An ordinary mailbox message the holder observes |
| Wake shard (MS-C3) | A shard-granularity mailbox; the router is its actor |
| Timer wheel (MS-C3) | Actor whose mailbox is timer intents |
| Checkpoint writer (MS-C1) | Bare authority (open-CAS election), no mailbox |
| Virtual object (state-materialization SDD) | Actor whose folds are its table |
| agent-ui `POST /api/chat` | Mailbox send to the session actor |

The RFC already reserved the vocabulary: *Durable Claim* and *claimed-work
operators* are the neutral names for authority claims and holders.

## Non-Goals

Named boundaries, because actor systems invite framework ambition (the
encore-ds spike's deletions are the model): no shard manager, no rebalancing
protocol, no resident actor processes, no distributed liveness clock, no
location directory — addressing is naming, activation is claiming, and a
released actor is just an unclaimed log.

## The Processor

The actor composition lands as one generic shell (the **Processor**) plus a
pure decision core (the **Handler**). The Handler is sans-IO — Fable-Rust-safe
and deterministically testable; the Processor is written once, not per binding,
and is the generalization of eff-firegrid's `DurableHost.claimAndRunTick`
(P3's port supplies the implementation).

```fsharp
type WakeReason = MailboxReady | TimerFired of TimerId * Timestamp | ChildTerminal of SubjectId

type Intent =                                 // requests TO the shell
    | SetTimer of TimerId * dueAt: Timestamp
    | Send of target: ActorAddress * MailboxMessage // the only cross-actor channel; shell stamps envelope provenance
    | Execute of EffectId * payload           // claim-first external effect

type Decision<'state, 'record, 'terminal> =
    { State: 'state
      Append: 'record list                    // own authoritative log, fenced
      Intents: Intent list                    // dispatched idempotently by shell
      Seal: 'terminal option }

type Handler<'state, 'msg, 'record, 'terminal> =
    { Initial: 'state
      Fold: 'state -> StoredRecord<'record> -> 'state
      OnAdmitted: 'state -> Admitted<'msg> -> Decision<'state, 'record, 'terminal>
      OnWake: 'state -> WakeReason -> Decision<'state, 'record, 'terminal> }

module Processor =
    val drive : DriveEnv -> ActorAddress -> Handler<'s,'m,'r,'t> -> Async<DriveOutcome<'t>>
    // DriveOutcome = Idle | Advanced | Sealed of 't | Deposed of expectedFence | Failed of DriveError
```

The drive tick and its invariants:

1. **Claim** — epoch increment via fence rotation (I5); at most one holder.
2. **Rebuild** — fold own log from latest checkpoint; no resident memory.
3. **Admit** — inbox from fenced cursor, provenance-deduped.
4. **Decide** — pure handler call per admitted message / wake.
5. **Commit** — append `Decision.Append` (including `Execute` *intents*) under
   the fence, then dispatch intents idempotently. Intent-before-effect: a
   crash between the two replays the intent, never doubles the decision.
6. **Checkpoint** — inbox cursor advances as a fenced record; ack is a cursor
   past a message whose outcome is already durable.
7. **Park or seal** — register wake interest and release, or append terminal
   and close. A deposed holder fails at step 5 and exits `Deposed`: it
   computed, but could not commit.

I/O contract: inputs are mailbox envelopes (open-append), wake events
(delivered as mailbox messages by the kernel), and the actor's own log
(rebuild). Outputs are fenced appends to its own log, sends to other actors'
mailboxes with provenance stamped by the shell from committed intent identity,
and intent-recorded external effects. Readers (attach, folds,
tables) never need authority. There is no actor-to-actor call primitive — a
reply is a send keyed by execution id.

Two handler flavors share this one drive protocol, per
[`execution-models.md`](./execution-models.md): the pure `Handler` (replay —
objects, routers, timer wheels) and the session adapter (reconstruction — an
effectful handler that drives an external harness but still writes only under
the processor's fence, emitting L1 facts as its `Append`). The processor
unifies coordination; execution strategy is the pluggable part.

## Relation to Restate's Command Processor

The Processor tracks Restate's partition state-machine loop closely, with the
broker outsourced: S2 plays Bifrost (ordered durable appends + fencing, bought
not built). In both systems the broker is the *ordering* authority while the
epoch-fenced leader/holder is the *semantic* authority — commands in the log
are proposals; the holder's application decides what they mean. Bifrost log ↔
mailbox; partition leader ↔ claim holder; command application ↔
`Handler → Decision`; partition store ↔ checkpointed folds; actions/outbox ↔
`Intents`/`Send`; followers ↔ authority-free readers.

The load-bearing difference: **Restate fuses mailbox and authoritative log; we
split them.** Restate's state machine is strictly deterministic, so decisions
are implicit — any replica re-derives state by re-applying commands. Our
headline executor (a managed session driving an external model harness) is
non-deterministic, so decisions must be *recorded*: proposals land in the
mailbox, the holder's decisions land on the actor's own log as facts. This is
the replay-vs-reconstruction spine expressed as storage layout —
deterministic-only systems may fuse the logs; systems hosting non-deterministic
executors must split them.

Smaller deltas: Restate's fixed partitions force RocksDB indexing, a shard
manager, and rebalancing — per-entity S2 streams dissolve those into naming
and claiming (hence our non-goals). Restate leaders are resident and tail
their logs; our actors are non-resident and claimed on wake — the latency gap
the wake path (MS-C3) closes.

## Consequences for the Ledger

- **B1** delivers the `Authority` protocol module as its generic core
  (cross-lane interface **I5**), with `DurableLog` = `SubjectHistory` +
  `Authority` + seal, and Turn as an address/codec binding.
- **P3**'s `InboxFold` port is the `Mailbox` admission primitive; generalize
  the module name, keep the proven semantics.
- **B3/C1/A2** consume I5 rather than hand-rolling fence/CAS usage per lane.
