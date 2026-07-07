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

## Consequences for the Ledger

- **B1** delivers the `Authority` protocol module as its generic core
  (cross-lane interface **I5**), with `DurableLog` = `SubjectHistory` +
  `Authority` + seal, and Turn as an address/codec binding.
- **P3**'s `InboxFold` port is the `Mailbox` admission primitive; generalize
  the module name, keep the proven semantics.
- **B3/C1/A2** consume I5 rather than hand-rolling fence/CAS usage per lane.
