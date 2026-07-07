# SDD: Managed Sessions — agent-ui as First Production User

Doc-Class: SDD
Status: active
Date: 2026-07-06
Owner: Firegrid Architecture
Substrate: S2

## Status

This SDD tracks the path from the current state — replay-model substrate proven,
TanStack scaffolding running, agent-ui on hand-rolled infrastructure — to the
target state: a managed-session (reconstruction-model) kernel exposed as a
production API, with agent-ui in `home-observability-stack` running on it in
production.

Two workstreams, one binding rule:

- **Capabilities** (platform side, this repo): no capability is done without a
  green proof in `apps/proofs`.
- **Milestones** (consumer side, agent-ui): no milestone is done without deleted
  agent-ui code and passing acceptance evidence.

Day-to-day execution is tracked in
[`../execution/managed-sessions-lanes.md`](../execution/managed-sessions-lanes.md).
This SDD is the design authority; the lanes doc is the work ledger.

## Objective

agent-ui's `app/api/chat/` ends up containing three things: ingress, attach, and
a harness adapter. Resumable turn streams, turn lifecycle (claims, fencing,
cancel, timeouts), history and thread projections, and session metadata are all
consumed from this repo's packages. The reconstruction execution model described
in [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md)
exists as a proven production surface, not only as canon.

## Document Relationships

| Document | Relationship |
| --- | --- |
| [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md) | Replay vs. reconstruction contract this SDD implements the reconstruction half of. |
| [`../canon/architecture/fluent/s2-substrate.md`](../canon/architecture/fluent/s2-substrate.md) | Primitive semantics (conditional append, cooperative fencing, read sessions, trim) all capabilities lower to. |
| [`../canon/architecture/fluent/harness-io.md`](../canon/architecture/fluent/harness-io.md) | Role map for the C6 adapter contract. |
| [`../requirements/agent-ui-stress-test.md`](../requirements/agent-ui-stress-test.md) | The consumer requirements this SDD schedules. Requirement gaps found during milestones flow back there. |
| [`../rfc/agent-substrate/operating/conformance.md`](../rfc/agent-substrate/operating/conformance.md) | Neutral invariant statements. Each capability lists the invariants it adds or proves. |
| [`fluent-firegrid-finish-line-sdd.md`](./fluent-firegrid-finish-line-sdd.md) | Authored-procedure ergonomics and native-kernel direction. Explicitly not this SDD's scope; shared kernel modules are coordinated through the lanes doc. |
| [`tanstack-workflow-s2-store-sdd.md`](./tanstack-workflow-s2-store-sdd.md) | Frozen scaffolding. This SDD adds no features to the TanStack path. |

## Current State

**Proven** (green in CI):

- Substrate capabilities: read-after-append visibility, cursor-fold restart,
  matchSeqNum contention, cooperative fence semantics in `apps/proofs`.
- Foundation behavior: subject history, `StateView`, and `KvStore` properties in
  `Firegrid.Foundation.Proofs`.

**Historical proof references** (checked in but not active conformance evidence):

- Coordination/runtime/object proof files under `apps/proofs/proofs/store-*`.
  They still describe useful obligations, but they target retired or moved TS
  package surfaces and must be rehomed before any capability cites them as green.

**Running unproven** (agent-ui, e2e-verified 2026-07-05 but outside any proof
harness): deterministic per-turn stream ids; mid-stream client-disconnect
survival; server-side turn materialization; resume/replay of in-flight turns;
cancel-by-thread; session-id persistence per thread.

**Absent**: checkpoint + trim; turn streams as a first-class primitive;
tail-driven wakes (everything temporal is sweep/poll); harness adapter contract
and any concrete adapter; session projections; the managed-session kernel
itself.

## Vocabulary

Aligned to [`../rfc/agent-substrate/concepts/terminology.md`](../rfc/agent-substrate/concepts/terminology.md):

- **Session** — durable conversation identity (agent-ui thread).
- **Turn** — one prompt→terminal unit of agent work within a session.
- **Turn stream** — the per-turn S2 stream carrying the live output record
  sequence; closed at terminal.
- **Attach** — replay a turn stream from zero plus live-tail to terminal.
- **Materialize** — fold a finalized turn into session history.
- **Wake shard** — the S2 stream a kernel host tails to learn an entity needs
  driving.
- **Harness adapter** — protocol lowering between one agent harness and the L1
  observation vocabulary.
- **Native resume artifact** — harness-owned resume state (e.g. Claude session
  id) durably kept by the kernel, fenced per session.
- **L1 observation / L2 coordination fact** — per
  [`../canon/architecture/fluent/execution-models.md`](../canon/architecture/fluent/execution-models.md).

## Non-Goals

- Authored-procedure ergonomics (finish-line SDD).
- Human-in-the-loop approvals and `wait_until` self-prompts as product features
  (next SDD; C3 wake path is their prerequisite and *is* in scope).
- Multi-tenant security hardening beyond scoped S2 access tokens.
- More than one additional harness beyond the MS-M5 smoke criterion.
- Blob/attachment claim-check implementation (C2 makes the decision; the
  implementation is deferred unless MS-M3 forces it).
- ACP conductor (editor-facing) role.

## Capability Ladder

Every capability entry has: production surface, proof obligations (falsifiable,
named), RFC invariants touched, and the consumer milestone it unblocks. Proof
names follow the house registry pattern in `apps/proofs`.

**Target Surface convention (gate G6).** Each capability carries a Target
Surface subsection — module placement, exported types and signatures, and the
laws the surface obeys. Surface language follows the two-zone rule in
[`../canon/architecture/fluent/language-and-targets.md`](../canon/architecture/fluent/language-and-targets.md):
platform capabilities (C1–C5) are F# signatures (modules, DU-typed errors,
sans-IO core), while TS-facing surfaces (the C6 adapter contract and the client
seam) use Effect shapes per `LLMS.md`. The surface is written and
architect-approved *before* proofs or implementation; proofs import only the
public surface. Signatures below are directional — the implementing WP refines
them under G6, and the merged PR updates this document to match. MS-C2 is
written out as the exemplar (WP B1's F# surface — `DurableLog` is the
P2-ported `SubjectHistory` plus the `Authority` protocol (I5) and seal, so B1
extends the port rather than starting fresh); the first WP of each other
capability supplies its section at the same altitude.

### MS-C1 — Checkpoint + Trim

The deferred state-story completion: snapshot records carrying
`{state, asOfSeqNum}`, rebuild = latest snapshot + suffix replay, trim behind
committed snapshots, checkpoint races resolved by open-CAS election (the Open
regime of `Authority`/I5 — checkpoint writing is a *bare authority with no
mailbox*, per
[`../canon/architecture/fluent/authority-and-actors.md`](../canon/architecture/fluent/authority-and-actors.md)).

Production surface: checkpointed fold in the substrate layer
(`src/Firegrid.Store` / `packages` seam per the package-boundary principle).

Target Surface (F# — WP A1; gate G6 + G1 sign-off pending):

This is the **table half** of the table/stream duality named in MS-C2 (the log
half is B1's `DurableLog`): a durable fold whose state is periodically
*snapshotted* so a cold host reconstructs it as `latest snapshot + suffix
replay` instead of replaying from `Seq 0`. It is one generic F# module,
`Checkpoint`, grounded in the P2-ported `Firegrid.Foundation.SubjectHistory`
(A1 depends on P2 only). Snapshots live on a **derived sidecar subject**
(`checkpointSubject source`, never a random name) so trimming the source never
drops a snapshot, and the snapshot log is itself an ordinary open-CAS log.
(Deferred, recorded: the sidecar itself grows unbounded — its compaction, by
trim-behind-latest or S2 retention on the snapshot log, is out of A1 scope.)

**Version is the exclusive upper bound** (the P2 convention, non-negotiable): a
`Snapshot.AsOf` is the source `Version` its `State` has folded *through* — the
`State` reflects every source record with `Seq < AsOf`, and `rebuild` resumes
the fold from `Seq AsOf`. `AsOf = Version 0` is the empty fold.

**Checkpoint election is open-CAS, not fenced-owner.** Per
[`../canon/architecture/fluent/authority-and-actors.md`](../canon/architecture/fluent/authority-and-actors.md),
the checkpoint writer is a *bare authority* (Open/CAS regime of `Authority`, I5),
no mailbox: two writers `commit` at the observed sidecar tail and a conditional
append decides one winner. A1 grounds `commit` directly on the P2
`SubjectHistory.appendExpected` (open-CAS); when I5 (B1) lands, `commit` is an
`Authority.admit` instance over the sidecar subject with no surface change — and
A2's `state.checkpoint-race` proof must drive `commit` through that
`Authority.admit` protocol, not a private CAS path. Coordinate with B1, do not
re-implement the fence/CAS mechanics.

```fsharp
// namespace Firegrid.Foundation — generic, domain-free (extends the P2 port).
// open Firegrid.Log; open Firegrid.Foundation.SubjectHistory
// Deps passed in (sans-IO shell): S2.Basin + codecs. No EffSharp.

module Checkpoint =
    /// I4 — the checkpoint record shape (cross-lane interface; consumed by A4's
    /// session-history fold and any long-lived fold). Folded state tagged with
    /// the source Version it is as-of (exclusive upper bound). Changing this
    /// shape is a G1 gate.
    type Snapshot<'state> = { AsOf: Version; State: 'state }

    /// State serialization for the snapshot sidecar. Source records keep the
    /// existing SubjectHistory.Codec; only the folded state needs this.
    type StateCodec<'state> =
        { Encode: 'state -> string
          Decode: string -> Result<'state, string> }

    /// A checkpointed fold: a source subject bound to its derived sidecar
    /// snapshot subject, with the seed state and fold function. Abstract — the
    /// consumer never sees stream names, seq nums, or the sidecar address.
    type Fold<'record, 'state>                 // abstract; holds S2.Basin + subjects + codecs + apply

    [<RequireQualifiedAccess>]
    type CommitFailure<'state> =
        | Raced of AppendConflict<Snapshot<'state>>  // open-CAS: another checkpointer took the slot
        | Regressed of requested: Version * latest: Version  // AsOf <= latest committed AsOf (stale state)
        | Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type TrimFailure =
        | AheadOfCheckpoint of requested: Version * committed: Version  // would trim past the snapshot
        | Failed of S2Errors.S2Failure

    /// Derived (never random) sidecar address for a source subject's snapshots.
    val checkpointSubject : SubjectId -> SubjectId

    /// Bind a fold over `source` (its sidecar is derived).
    val make :
        S2.Basin -> Codec<'record> -> StateCodec<'state> -> source: SubjectId
            -> initial: 'state -> apply: ('state -> StoredRecord<'record> -> 'state)
            -> Fold<'record, 'state>

    /// Pure rebuild plan (sans-IO core): from a latest snapshot (or none) decide
    /// the resume Seq and the seed state. `None -> (Seq 0, initial)`.
    val resumeFrom : Snapshot<'state> option -> initial: 'state -> Seq * 'state

    /// Latest committed snapshot on the sidecar, or None when never checkpointed.
    val latest : Fold<'record, 'state> -> Async<Snapshot<'state> option>

    /// Rebuild = latest snapshot + suffix replay to the source tail. With no
    /// snapshot this is a fold-from-zero. Returns the folded state and the
    /// source Version it is as-of. No resident memory — a cold Fold rebuilds.
    val rebuild : Fold<'record, 'state> -> Async<'state * Version>

    /// Commit a snapshot to the sidecar under open-CAS at its observed tail.
    /// Two racing writers: exactly one wins; the loser gets `Raced`. Snapshots
    /// are monotonic: rejects `Regressed` when `snapshot.AsOf <= latest.AsOf`
    /// (a slow checkpointer cannot overwrite `latest` with stale state).
    val commit :
        Fold<'record, 'state> -> Snapshot<'state> -> Async<Result<Version, CommitFailure<'state>>>

    /// Convenience: rebuild to the current source tail, then commit that snapshot.
    val checkpoint : Fold<'record, 'state> -> Async<Result<Snapshot<'state>, CommitFailure<'state>>>

    /// Trim the source behind a committed snapshot. `upTo` must be <= the latest
    /// committed snapshot's AsOf (`AheadOfCheckpoint` otherwise); a reader
    /// starting at the trim floor still rebuilds equivalent state.
    val trim : Fold<'record, 'state> -> upTo: Version -> Async<Result<unit, TrimFailure>>
```

Ergonomic sample (the consumer test — usable with no proof-harness knowledge):

```fsharp
// A checkpointed fold over a source subject, seeded with initial + apply.
let fold = Checkpoint.make basin recordCodec stateCodec source initial apply

// Rebuild with no resident memory: latest snapshot + suffix replay to tail.
let! state, asOf = Checkpoint.rebuild fold

// Snapshot the current tail (open-CAS election; `Raced` if another writer won).
match! Checkpoint.checkpoint fold with
| Ok snap ->
    // snap.State reflects every source record with Seq < snap.AsOf.
    let! _ = Checkpoint.trim fold snap.AsOf   // trim source behind the snapshot
    ()
| Error (Checkpoint.CommitFailure.Raced _) -> ()      // lost the election; re-read latest
| Error (Checkpoint.CommitFailure.Regressed _) -> ()  // stale state; latest already ahead
| Error (Checkpoint.CommitFailure.Failed _) -> ()
```

Laws (stated here; A1 proves rebuild-equivalence, A2 proves race + trim-safety):

- **Rebuild equivalence** (`state.checkpoint-rebuild-equivalence`, A1). For any
  source log and any sequence of committed snapshots, `rebuild` yields the same
  `(state, Version)` as `SubjectHistory.foldTo` from `Seq 0` to the source tail —
  *including across a host restart*: a fresh `Fold` with no resident memory
  reconstructs identical state from `latest snapshot + suffix`. Checkpoint +
  suffix replay ≡ full replay.
- **Checkpoint race** (`state.checkpoint-race`, A2). Two `commit`s at the same
  observed sidecar tail: exactly one wins (open-CAS single-winner); the loser
  gets `Raced`, its snapshot rejected, never interleaved.
- **Monotonic snapshots** (A2, with `checkpoint-race`). Committed snapshots are
  monotonic in `AsOf`: `commit` rejects `Regressed` when
  `snapshot.AsOf <= latest.AsOf` (absent a snapshot, `latest = Version 0`). A
  slow checkpointer that wins a CAS slot with stale state therefore cannot
  regress `latest` — keeping `rebuild` suffixes minimal and `trim`'s high-water
  guard reading the true latest `AsOf`.
- **Trim safety** (`state.trim-safety`, A2). `trim upTo` never advances past the
  latest committed snapshot's `AsOf` (else `AheadOfCheckpoint`); a reader
  starting at the trim floor still rebuilds equivalent state, because the
  snapshot covers the trimmed prefix.
- **No leaks.** Consumers never see S2 stream names, the sidecar address, seq
  nums, or CAS tokens.

Scope guard (both directions): the checkpointed fold is the *table half* only —
snapshot state stays domain-free, and the module does not grow a KStreams
operator algebra (joins, windowing, repartitioning) ahead of a consumer that
demands it. The duality surface is: log (`create/append/seal/attach`, B1), table
(`make/rebuild/commit/checkpoint/trim` here + `StateView`), predicate-wait (the
bridge). The `Snapshot<'state>` shape is cross-lane interface **I4** (consumed by
A4 and any long-lived fold); election reuses `Authority`'s Open regime (**I5**,
B1) rather than a bespoke per-lane CAS.

Validation Gates (F#-zone, G6 — per
[`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)):

- **Native shapes, EffSharp-free.** `Async` + `Result` + DU errors + `Codec`
  records + a pure `resumeFrom` core — no EffSharp (per the 2026-07-06
  deprecation), so the package-level EffSharp decision is "none, nothing new."
- **Ergonomic sample** above passes the consumer test.
- **Works-without-EffSharp** is inherent (nothing introduced). **Emitted-TS
  sample + Fable build check** ride P4's Fable→TS facade; the surface is
  Fable-safe by construction — it adds no BCL-only API beyond what
  `SubjectHistory` already emits.

Proof obligations:

- `state.checkpoint-rebuild-equivalence` — fold-from-checkpoint equals
  fold-from-zero for the same stream, including across a host restart.
- `state.checkpoint-race` — two racing checkpointers commit exactly one snapshot
  at a given `asOfSeqNum`; the loser's snapshot is rejected, not interleaved.
- `state.trim-safety` — trim never advances past the latest committed snapshot;
  a reader starting at the trim point still rebuilds equivalent state.

RFC invariants: extends durable-log requirements with checkpoint/trim contract.
Unblocks: MS-M3.

### MS-C2 — Turn Streams

The managed-session storage primitive. Per-turn S2 stream with deterministic
naming derived from ids the client already holds
(`sessions/{sessionId}/turns/{turnId}` shape, matching agent-ui's existing
`streamIdFor`); producer-fenced appends; a terminal record plus stream closure
as EOF; attach as replay-from-zero + live tail + terminal status.

Decision (made here): the turn stream carries the token-level output record
sequence — the same wire chunks the UI consumes — so attach is byte-faithful
and folds stay off the token hot path. Block-level L1 observation facts land on
the session stream (C6), not the turn stream. Alternative (L1 facts at token
granularity) rejected for fold cost and append rate.

Decision (made here): attachments and other large payloads are stored by
reference (claim-check); turn and session streams never carry inline blobs
larger than one S2 record comfortably allows. Implementation deferred; the
record schema reserves the reference shape now.

Target Surface (F# — WP B1; gate G6 + G1 sign-off pending):

The generic primitive is the *stream half* of the table/stream duality — a
sealed, single-writer, schema-coded durable log. It factors into two generic
F# modules, both grounded in the P2-ported `Firegrid.Foundation.SubjectHistory`
and the proven S2 fence primitives (`Record.fence`,
`AppendOptions.fencingToken`, `appendIfFenced`, `FencingTokenMismatch`):

- **`Authority`** — the write-authority protocol (cross-lane interface **I5**),
  per [`../canon/architecture/fluent/authority-and-actors.md`](../canon/architecture/fluent/authority-and-actors.md).
  Claim, epoch deposal, and seal over a subject, in the canon's three regimes:
  Open (CAS admission), FencedOwner (epoch), Sealed. B3 (lifecycle), C1 (router
  leadership), and A2 (checkpoint election) consume this one protocol instead of
  hand-rolling fence/CAS usage per lane.
- **`DurableLog`** — `SubjectHistory` + `Authority` + seal: the domain-free
  sealed log. Turn is a **binding** of it (an address scheme plus chunk/terminal
  schemas), not a new API.

The *table half* is Lane A's checkpointed fold/`StateView` (prior art:
firegrid's `effect-durable-operators` `DurableTable` — collections with
`insert/get/query/subscribe` over a changelog); predicate-waits over tables,
already proven on the object side (`store-object-state-wait`,
`store-object-index-wait`), bridge the two halves.

**Fence token ↔ epoch.** The substrate fence token is the epoch's string
encoding, so `claim` rotates the fence to `epoch + 1` and a stale holder's
fenced append surfaces `S2Errors.FencingTokenMismatch expected`, from which the
deposing epoch reads back directly. Deposal is epoch increment, never
revocation: the prior epoch is immortal and harmless — it may compute but cannot
commit. Time-based takeover is *policy* layered above `claim`, never a clock in
the primitive.

```fsharp
// namespace Firegrid.Foundation — generic, domain-free (extends the P2 port).
// open Firegrid.Log; open Firegrid.Foundation.SubjectHistory
// Deps passed in (sans-IO shell): S2.Basin + SubjectHistory.Codec. No EffSharp.

module Authority =
    /// Monotonic authority generation; a claim mints the next epoch.
    type Epoch = Epoch of int64
    /// Substrate fence token (the epoch's encoding). Opaque — never seen by consumers.
    type Fence                                 // abstract
    /// Identity of a would-be holder (worker/process). Same identity re-attaches;
    /// a different identity takes over. (encore-ds `insertOrGet` shape.)
    type HolderId = HolderId of string
    /// A live claim, bound to one epoch's fence — carries its Basin + Codec, so
    /// commit/seal take no extra args. Only the holder commits or seals.
    type Holder<'record>                        // abstract; Basin + Codec + Subject + Epoch + Fence
    val epoch : Holder<'record> -> Epoch

    [<RequireQualifiedAccess>]
    type ClaimError =
        | Sealed                               // subject already terminal
        | Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type CommitError =
        | Deposed of by: Epoch                 // a newer epoch rotated the fence
        | Sealed                               // subject sealed; no holder may append
        | Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type AdmitError<'record> =
        | Lost of AppendConflict<'record>      // open-CAS: another writer took the slot
        | Failed of S2Errors.S2Failure

    // FencedOwner regime — exactly one live holder.
    /// Claim the subject under `holderId`: returns the existing holder if it
    /// already holds the current epoch (idempotent), else rotates the fence to
    /// `epoch + 1` and takes over. `claim` is the sole deposal mechanism; fails
    /// `Sealed` if the subject is already terminal.
    val claim  : S2.Basin -> Codec<'r> -> SubjectId -> HolderId -> Async<Result<Holder<'r>, ClaimError>>
    /// Fenced append of domain records under the holder's epoch.
    val commit : Holder<'r> -> 'r list -> Async<Result<Version, CommitError>>
    /// Append the terminal record and extinguish authority (first-terminal-wins).
    val seal   : Holder<'r> -> 'r -> Async<Result<unit, CommitError>>

    // Open regime — leaderless single-winner CAS (checkpoint election, dedupe
    // claims, admission records). Thin naming over `SubjectHistory.appendExpected`.
    val admit  : S2.Basin -> Codec<'r> -> SubjectId -> Version -> 'r list
                     -> Async<Result<Version, AdmitError<'r>>>

module DurableLog =
    /// Derived (never random) address → subject/stream name.
    type Address = { Segments: string list }
    /// Streamed-body and terminal codecs for one log.
    type Codec<'chunk, 'terminal> =
        { Chunk: SubjectHistory.Codec<'chunk>
          Terminal: SubjectHistory.Codec<'terminal> }
    type Producer<'chunk, 'terminal>           // abstract; wraps an Authority.Holder over a chunk|terminal codec
    type Attachment<'chunk, 'terminal>         // abstract; a reader, no authority needed

    [<RequireQualifiedAccess>]
    type CreateError = Sealed | Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type AppendError = Deposed | Sealed | Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type AttachError = NotFound | Failed of S2Errors.S2Failure

    /// `create` = `Authority.claim` under `holderId`, bound to the address: the
    /// same identity re-attaches to the live log; a different identity takes it
    /// over under a new epoch (never forks a second stream). AlreadyLive
    /// rejection is lifecycle policy (MS-C5), not a mechanism here.
    val create : S2.Basin -> Codec<'c,'t> -> Address -> Authority.HolderId -> Async<Result<Producer<'c,'t>, CreateError>>
    val append : Producer<'c,'t> -> 'c -> Async<Result<unit, AppendError>>
    val seal   : Producer<'c,'t> -> 't -> Async<Result<unit, AppendError>>

    val attach : S2.Basin -> Codec<'c,'t> -> Address -> Async<Result<Attachment<'c,'t>, AttachError>>
    /// Pull-cursor (canon stream idiom): blocks-with-wait until the next chunk or
    /// the seal arrives (the `SubjectHistory.openCursorWithWait` idiom), yielding
    /// `Ok (Some c)` per chunk and `Ok None` at the terminal. No EffSharp.
    val next     : Attachment<'c,'t> -> Async<Result<'c option, AttachError>>
    /// Resolves once the log is sealed: the terminal record's body.
    val terminal : Attachment<'c,'t> -> Async<Result<'t, AttachError>>
    val close    : Attachment<'c,'t> -> Async<unit>

// Turn: a DurableLog binding — address + codecs, ZERO new methods. If a turn
// needs an operation DurableLog lacks, that is gate G1/G6, not a Turn method.
// Domain zone; TurnChunk/TurnTerminal reserve the claim-check reference shape.
module Turn =
    type SessionId = SessionId of string
    type TurnId = TurnId of string
    val address : SessionId -> TurnId -> DurableLog.Address   // sessions/{s}/turns/{t}
    val codec   : DurableLog.Codec<TurnChunk, TurnTerminal>
```

Ergonomic sample (the consumer test — usable with no proof-harness knowledge):

```fsharp
// Producer: claim → append → seal. Same holderId re-attaches; a different one
// takes over. `create` is idempotent per identity.
match! DurableLog.create basin Turn.codec (Turn.address s t) holderId with
| Ok producer ->
    let! _ = DurableLog.append producer chunk    // Error AppendError.Deposed if fenced out
    let! _ = DurableLog.seal producer terminal
    ()
| Error _ -> ()

// Reader: attach mid-flight, drain replay + live tail, then the terminal.
match! DurableLog.attach basin Turn.codec (Turn.address s t) with
| Ok a ->
    let rec drain () = async {
        match! DurableLog.next a with
        | Ok (Some chunk) -> render chunk; return! drain ()
        | Ok None -> let! t = DurableLog.terminal a in finish t
        | Error _ -> () }
    do! drain ()
| Error _ -> ()
```

Laws (stated here; proven by WP B2 at the generic level):

- **Fenced single-writer.** At most one holder per epoch; a deposed holder's
  `commit`/`append` fails `Deposed` — it computes but cannot commit
  (`store-object-live-fencing` lineage).
- **Idempotent claim.** `claim subject holderId` while `holderId` still holds
  the current epoch returns the same `Holder` (idempotent per epoch); a
  different `holderId` rotates to `epoch + 1` and takes over — the
  `insertOrGet({subject}/{epoch}, holderId)` shape. `claim` is the sole deposal
  mechanism.
- **Create is claim-by-identity.** `create address holderId` on a live address
  re-attaches for the same identity and takes over (new epoch) for a different
  one — so two creates for one address yield one durable log, never a fork. An
  `AlreadyLive` *rejection* is lifecycle policy (MS-C5), never a log mechanism;
  `session.turn-idempotent-create` is a same-identity re-attach, not a
  second-create no-op.
- **Seal is terminal.** First-valid-terminal wins; `commit`/`append`/`seal`
  after seal fail `Sealed`; `attach` after seal replays the full prefix then
  yields the terminal.
- **Attach tails with wait.** `next` blocks until the next chunk or the seal,
  then yields `Ok None` at the terminal (the `openCursorWithWait` idiom) — B2's
  attach proof pins the observed prefix + terminal against this.
- **Open regime is single-winner.** `admit`/`claim` at an observed tail yields
  exactly one winner; the losers observe the conflict, not the work.
- **No leaks.** Consumers never see S2 stream names, fence tokens, or seq nums.

Scope guard (both directions): domain semantics stay out of the generic layer,
*and* the generic layer does not grow a KStreams operator algebra (joins,
windowing, repartitioning) ahead of a consumer that demands it. `Authority` is
the shared write-authority core (I5), not per-lane fence code. The duality
surface is: log (`create/append/seal/attach`), table (`fold/get/query/subscribe`,
Lane A), predicate-wait (the bridge). The **Mailbox** admission primitive (P3's
generalized `InboxFold`) and the actor **Processor** drive loop are *not* in
this surface — B1 supplies the write-authority protocol they compose; the
fence/claim *mechanics* are P3's `Foundation/Durable` port (coordinate, don't
duplicate).

Validation Gates (F#-zone, G6 — per
[`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)):

- **Native shapes, EffSharp-free.** `Async` + `Result` + DU errors + `Codec`
  records + pull-cursor reads — no EffSharp (per the 2026-07-06 deprecation), so
  the package-level EffSharp decision is "none, nothing new introduced."
- **Ergonomic sample** above passes the consumer test.
- **Emitted-TS sample + Fable build check** ride P4's Fable→TS facade for
  `Firegrid.Log`; deferred to implementation. The surface is Fable-safe by
  construction — it adds no BCL-only API beyond what `SubjectHistory` already
  emits.

Proof obligations:

- `session.turn-attach` — a reader attaching mid-flight observes a
  byte-identical prefix to a reader attached from the start, then the same live
  tail and terminal status.
- `session.turn-crash-terminal` — kill -9 the producer mid-turn; recovery drives
  the turn to a durable terminal state and closes the stream; an attached reader
  observes the terminal rather than hanging.
- `session.turn-idempotent-create` — duplicate turn creation with the same
  client-supplied ids yields one stream and one producer; the retry attaches
  rather than forking.

RFC invariants: turn-stream naming, terminal/closure semantics, attach contract.
Unblocks: MS-M1.

### MS-C3 — Wake Path

Tail-driven wakes replacing sweep/poll for latency-sensitive work: per-shard
wake streams, a `readSession`-tailed router with a durable cursor, and a folded
timer index in front of the existing sweep semantics. Sweeps remain as the
degraded-mode contract and their proofs stay green.

Proof obligations:

- `wake.tail-latency` — an appended wake reaches its claimed handler within a
  bound asserted from trace evidence (bound recorded in the proof, not prose).
- `wake.single-claim` — two routers tailing the same shard: exactly one claims
  a given wake; the loser observes the claim, not the work.
- `wake.timer-exactly-once` — a due timer fires exactly once across a router
  restart; a not-yet-due timer survives the restart unfired.

RFC invariants: wake-delivery liveness statement (at least one live host per
shard; no substrate push).
Unblocks: MS-M5 timer/approval-dependent features; not on the MS-M1–M4 path.

### MS-C4 — Session State + Projections

TypeScript-surface `StateView` per the KV-demo pattern (fold + `AppliedTail`;
eventual reads local; strong reads via check-tail barrier), session history as a
C1-checkpointed fold, and the thread-index projection.

Proof obligations:

- `state.stateview-strong-read` — a strong read issued after a second host's
  acknowledged append observes that append; an eventual read may not.
- `session.history-fold` — session history materialized from L1/L2 facts equals
  the same fold replayed from zero; checkpointing does not change the result.
- `session.projection-lag-observable` — projection staleness is exposed as data
  (applied tail vs. checked tail), not hidden.

RFC invariants: projection/read-model contract (projections are rebuildable,
never alternate truth).
Unblocks: MS-M3.

### MS-C5 — Turn Lifecycle Authority

Claim, fence, cancel, and idle/max-duration timeouts as a durable protocol over
C2 + C3, replacing agent-ui's in-memory `activeRuns` map. Cancel is a durable
control fact any process can append; the producer observes it and terminates the
turn; timeouts are kernel obligations, not process-local timers.

Target Surface (F# — WP B4; the *resume-artifact-store slice* of MS-C5; gate
G6):

MS-C5 has two WPs. This section is **WP B4's slice only** — the fenced
native-resume-artifact store and its `session.resume-artifact-fenced`
obligation. The lifecycle-authority slice (session single-writer, durable
cancel, idle/max-duration timeouts, and the three `session.lifecycle-*`
obligations) is **WP B3's** Target Surface, supplied separately at the same
altitude; the two are independent (both depend on B1's I5 only).

The store is a **per-session fenced register** of the harness-native resume
artifact — the "Native resume artifact" of the Vocabulary section: harness-owned
resume state (e.g. a Claude session id) durably kept by the kernel, *fenced per
session*. It is a domain binding of B1's `Authority` (I5, PR #98) in the
FencedOwner regime: only the live session writer stores or replaces the artifact,
and a different identity deposes the prior writer by epoch increment — so a
deposed writer's `store` fails `Deposed`, exactly the DurableLog `Deposed` /
`store.object-live-fencing` law applied to the resume register. This is the
executable close of agent-ui's last-writer-wins session-store race: a stale
process can no longer clobber the live writer's artifact.

**Read-side pairing with `firegrid/native` (cited, not redefined).** The artifact
is an opaque `Payload` plus metadata whose fields mirror D1's `firegrid/native`
L1 extension (I2) — `Harness`, `NativeType`, `Payload` — so a harness adapter
(WP D3) reads the fenced artifact to re-hydrate the harness and surfaces the same
native fact to the UI as a `firegrid/native` record. This store holds the durable,
fenced *source* of that state; it does **not** redefine the L1 vocabulary (see
[`../canon/architecture/fluent/l1-observation-vocabulary.md`](../canon/architecture/fluent/l1-observation-vocabulary.md),
§`firegrid/native`). The `Payload` is opaque to the store — never parsed.

**Fence binding.** The register's subject is session-derived
(`sessions/{sessionId}/resume`, never random) and its epoch rotates on takeover,
so the fence is genuinely *per session*. Coordinating this claim with B3's
session/lifecycle claim (one identity claiming both, deposed together on takeover)
is lifecycle policy layered above, **not a mechanism here** — mirroring
DurableLog's "AlreadyLive rejection is lifecycle policy (MS-C5), not a mechanism."

```fsharp
// namespace Firegrid.Store — domain binding of Authority (I5); native-resume
// concern, session-scoped. open Firegrid.Foundation; open Firegrid.Foundation.SubjectHistory
// Deps passed in (sans-IO shell): S2.Basin. No EffSharp.

/// The harness-native resume payload plus its metadata — the durable, fenced
/// source of the `firegrid/native` (I2) facts a harness adapter surfaces on the
/// read side. Holds the artifact; does NOT redefine the L1 vocabulary.
type ResumeArtifact =
    { Harness: string             // emitting harness id — pairs with firegrid/native.harness
      NativeType: string option   // pairs with firegrid/native.nativeType
      Payload: string             // opaque resume blob (e.g. a Claude session id); never parsed
      Turn: Turn.TurnId option }  // provenance: the turn whose completion produced it, if known

module ResumeArtifactStore =
    /// A fenced writer over one session's resume register — wraps an
    /// `Authority.Holder` (FencedOwner, I5). Only the live holder stores; a
    /// different identity deposes it (epoch increment). Abstract.
    type Writer                                 // abstract; wraps Authority.Holder<ResumeArtifact>

    [<RequireQualifiedAccess>]
    type OpenError = Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type StoreError =
        | Deposed                              // a newer epoch took the session's write authority
        | Failed of S2Errors.S2Failure
    [<RequireQualifiedAccess>]
    type ReadError = Failed of S2Errors.S2Failure

    /// Derived (never random) subject for a session's resume register:
    /// `sessions/{sessionId}/resume`. Consumers pass `SessionId`, not names.
    val subject : Turn.SessionId -> SubjectId

    /// Claim the fenced writer for a session under `holderId` (`Authority.claim`
    /// bound to the resume subject): the same identity re-attaches idempotently;
    /// a different identity rotates the fence to `epoch + 1` and deposes the prior
    /// writer. Claim is the sole deposal mechanism.
    val openWriter :
        S2.Basin -> Turn.SessionId -> Authority.HolderId -> Async<Result<Writer, OpenError>>

    /// Store (replace) the current artifact under the writer's fence
    /// (`Authority.commit`). Last-store-under-fence-wins. A deposed writer fails
    /// `Deposed` — it may compute but cannot commit.
    val store : Writer -> ResumeArtifact -> Async<Result<unit, StoreError>>

    /// Read the current artifact — authority-free (readers need no claim). `None`
    /// when the session has never stored one.
    val read : S2.Basin -> Turn.SessionId -> Async<Result<ResumeArtifact option, ReadError>>
```

Ergonomic sample (the consumer test — usable with no proof-harness knowledge):

```fsharp
// Writer (the session's live holder): claim the fenced register, store the
// harness's resume artifact after a turn. A deposed writer's store fails.
match! ResumeArtifactStore.openWriter basin session holderId with
| Ok writer ->
    let artifact =
        { Harness = "claude-agent-sdk"
          NativeType = Some "resume"
          Payload = claudeSessionId          // opaque to the store
          Turn = Some turnId }
    match! ResumeArtifactStore.store writer artifact with
    | Ok () -> ()
    | Error ResumeArtifactStore.StoreError.Deposed -> ()      // a newer writer took over
    | Error (ResumeArtifactStore.StoreError.Failed _) -> ()
| Error _ -> ()

// Resume side (authority-free): read the current artifact to re-hydrate the
// harness; `None` on a session that has never produced one.
match! ResumeArtifactStore.read basin session with
| Ok (Some artifact) -> resumeHarness artifact.Harness artifact.Payload
| Ok None -> startFresh ()
| Error _ -> ()
```

Laws (stated here; proven by WP B4's `session.resume-artifact-fenced` proof):

- **Fenced single-writer store** (`session.resume-artifact-fenced`). At most one
  live writer per session (per epoch); a deposed writer's `store` fails `Deposed`
  — it computes but cannot commit. Extends the `store.object-live-fencing`
  lineage (DurableLog's `Deposed` law) to the session resume register, closing
  agent-ui's last-writer-wins session-store race: a stale process cannot clobber
  the live writer's artifact.
- **Idempotent claim / takeover.** `openWriter session holderId` while `holderId`
  holds the current epoch re-attaches to the same writer (idempotent per epoch);
  a different identity rotates to `epoch + 1` and deposes the prior writer.
  Inherits `Authority`'s idempotent-claim law; `openWriter` (claim) is the sole
  deposal mechanism.
- **Last-store-wins read.** `read` returns the artifact of the latest successful
  `store`, or `None` before the first. Reads are authority-free — a reader needs
  no claim (matches DurableLog `attach`; per the canon, readers never need
  authority).
- **Opaque payload; native pairing, not redefinition.** The store never parses
  `Payload`; `Harness`/`NativeType`/`Payload` are the durable, fenced source of
  the `firegrid/native` (I2) facts a harness adapter (D3) surfaces on read — the
  store consumes that vocabulary's shape, it does not redefine it.
- **No leaks.** Consumers never see fence tokens, seq nums, or the interleaved
  fence records; the derived subject is deterministic by design.

Scope guard (both directions): the resume register is a *fenced last-write-wins
cell* (the register shape) — not a log the consumer tails, not a fold; it exposes
the current artifact, no history. Superseded-artifact compaction (trim/retention
behind the latest) is deferred, recorded, and out of B4 scope (mirrors the MS-C1
sidecar-compaction deferral). `Authority` (I5) is consumed verbatim — FencedOwner
`claim`/`commit`, no new methods, **no I5 shape change, so G1 is not re-opened**;
a resume operation `Authority` lacks would be gate G1/G6, not a method grown here.
Sans-IO split: subject derivation, the artifact JSON codec, and latest-record
selection are the pure core; `openWriter`/`store`/`read` are the I/O shells over
the injected `S2.Basin`.

Validation Gates (F#-zone, G6 — per
[`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)):

- **Native shapes, EffSharp-free.** `Async` + `Result` + DU errors + a
  `ResumeArtifact` record + a private JSON codec (`JsJson`, as `Turn` uses) — no
  EffSharp (per the 2026-07-06 deprecation), so the package-level EffSharp
  decision is "none, nothing new introduced."
- **Ergonomic sample** above passes the consumer test.
- **Emitted-TS sample + Fable build check** ride P4's Fable→TS facade; the surface
  is Fable-safe by construction — its JSON codec uses the same `JsJson` surface
  `Turn` already emits, adding no BCL-only API beyond `Authority`/`SubjectHistory`.

Proof obligations:

- `session.lifecycle-single-writer` — two concurrent starts for the same
  session: exactly one live turn; the second is rejected or queued by policy,
  never a second producer on the same session.
- `session.lifecycle-durable-cancel` — cancel appended by a process that is not
  the producer terminates the turn to a durable cancelled state; duplicate
  cancel is idempotent.
- `session.lifecycle-deposed-producer` — extends `store.object-live-fencing` to
  turns: a live deposed producer cannot append turn output after takeover.
- `session.resume-artifact-fenced` — the native resume artifact (e.g. Claude
  session id) is written under the session fence; a stale owner's write is
  rejected (closes agent-ui's last-writer-wins session-store race).

RFC invariants: turn single-writer, durable cancel, resume-artifact fencing.
Unblocks: MS-M2.

### MS-C6 — Harness Adapter Contract + Claude Agent SDK Adapter

The L1 observation vocabulary and the adapter interface: `drive` (prompt in),
observe → L1 facts, native resume artifact production, and a declared
**interception capability** (gateable vs. observe-only — the durability
guarantees differ and the contract must say so). First concrete adapter: Claude
Agent SDK, including subagent scoping (`parent_tool_use_id`) and usage/cost
facts.

Decision (G2, made 2026-07-06): the L1 observation vocabulary is an **ACP
session-update superset**. Base vocabulary = ACP session/update semantics
(message chunks, thought chunks, tool_call and tool_call_update, plan).
Firegrid extensions are namespaced and additive: `firegrid/usage` (token/cost
facts), `firegrid/subagent` (parent-scoped attribution), `firegrid/native`
(harness-specific passthrough, tagged with harness id). Rules: every extension
declares its fold behavior for consumers that do not understand it —
ignorable-by-default, never load-bearing for the base fold's correctness; the
schema is versioned; D2's fixture corpus is the compatibility gate. Rationale:
future ACP harnesses lower near-trivially, and every UI folds one format
regardless of harness. WP D1 turns this into the schema + decision-record
page; deviations re-open gate G2.

Proof obligations:

- `harness.fixture-replay` — replaying a recorded harness transcript fixture
  through the adapter reconstructs an identical L1 fact sequence and identical
  folded message state (adapter determinism).
- `harness.resume-suppression` — resuming a session does not re-emit
  already-observed L1 facts and does not re-execute observed side effects; new
  facts append after the recorded prefix.
- `harness.subagent-scoping` — subagent output is attributed to its parent tool
  call, never interleaved into the top-level turn text.

RFC invariants: L1 vocabulary schema; interception-capability declaration;
resume-suppression contract.
Unblocks: MS-M4.

## Consumer Milestones

Consumer repo: `home-observability-stack` (`apps/agent-ui`). Every milestone
ships flag-gated with a one-deploy revert path (this is a production home
system), links back to this SDD's ids, and treats the 2026-07-05 e2e list as the
standing regression suite:

> deterministic streamId; server materializes the assistant turn; full content +
> terminal status survive mid-stream client disconnect; user-turn backfill;
> resume during streaming; replay after finalize; active-stream cleared on
> finalize; assistant-history writes rejected; cancel reaches a terminal state.

### MS-M1 — Attach replaces the resumable store

Consumes: MS-C2. Deletes: `resumable-store.ts`, most of `stream-runtime.ts`
stream plumbing. Evidence: regression list green over the C2 attach path; no
`assistant-stream/resumable` S2 store writes remain.

### MS-M2 — Turn lifecycle on kernel authority

Consumes: MS-C5. Deletes: in-memory `activeRuns`, cancel plumbing,
session-store write path. Evidence: regression list green; double-POST produces
one live turn in production; cancel works against a production build from a
process other than the producer; the dev-mode cancel gotcha is obsolete.

### MS-M3 — History and threads as projections

Consumes: MS-C1 + MS-C4. Deletes: `history-store` event vocabulary,
`turn-writer.ts`; materialization becomes a kernel obligation (closes the
orphaned-turn crash gap: kill the container mid-turn, the turn still reaches
history). Evidence: regression list green; history load cost bounded by
checkpoint, verified on the largest real thread.

### MS-M4 — Event loop becomes an adapter

Consumes: MS-C6. Deletes: the `switch (m.type)` lowering in `route.ts`; the UI
folds the L1 vocabulary. Evidence: regression list green; usage/cost surfaced
per turn; subagent output correctly scoped in the live UI.

### MS-M5 — Exit criterion

One week of production traffic on the full stack, plus the second-harness
smoke: a codex-CLI or raw-SDK adapter drives one turn behind the unchanged UI.
Evidence: zero data-loss incidents; the smoke turn renders correctly with no UI
changes.

## Build Order

C2 → M1 → C5 → M2 → (C1 + C4) → M3 → C6 → M4 → C3 → M5.

Each milestone de-risks the next capability with production feedback. C3 gates
only M5-era temporal features and can proceed in parallel from any point.
Capability pairs in parentheses are independent of each other.

## Implemented Assets (living)

Update this section as work lands: proof green / module shipped / agent-ui PR
merged / LOC deleted.

- **A1 (MS-C1)** — `Firegrid.Foundation.Checkpoint` shipped
  (`src/Firegrid.Store/Foundation/Checkpoint.fs`): the checkpointed fold (table
  half). Derived-sidecar snapshots (`checkpointSubject`, I4 `Snapshot<'state>`),
  open-CAS `commit` grounded on `SubjectHistory.appendExpected` with the
  monotonic `Regressed` guard, `rebuild` = latest snapshot + suffix replay,
  `checkpoint` convenience, and `trim` behind the committed AsOf. Proof
  `state.checkpoint-rebuild-equivalence` green: a cold `Fold` and a fresh-client
  host restart both reconstruct `(state, Version)` identical to
  `SubjectHistory.foldTo` from `Seq 0`. Deferred to A2: sidecar compaction and
  driving `commit` through `Authority.admit` (I5).
- **B1 / MS-C2 (I5 + I1) — modules shipped (WP B1-IMPL).** `Authority`
  (`src/Firegrid.Store/Foundation/Authority.fs`), `DurableLog`
  (`src/Firegrid.Store/Foundation/DurableLog.fs`), and the `Turn` binding
  (`src/Firegrid.Store/Foundation/Turn.fs`) implement the MS-C2 Target Surface
  exactly: idempotent-per-holder `claim` with epoch-increment takeover, seal as
  a reserved fence sentinel, `next`/`terminal` blocking-with-wait per the
  `openCursorWithWait` idiom. F#-native, EffSharp-free, Fable-safe (JS + TS
  emit green). Composes the P3 fence primitives (PR #91), does not duplicate
  them. Proof obligations (`session.turn-attach`, `session.turn-crash-terminal`,
  `session.turn-idempotent-create`) land with WP B2.
- **MS-C6 / WP D1 — L1 observation vocabulary (I2).** Shipped
  `@firegrid/l1-vocabulary` (`packages/l1-vocabulary`): the ACP `session/update`
  superset schema, Effect-free decoder, and canonical `foldTurn` base fold, with
  the G2 decision record at
  [`../canon/architecture/fluent/l1-observation-vocabulary.md`](../canon/architecture/fluent/l1-observation-vocabulary.md)
  and the seed fixture corpus (`packages/l1-vocabulary/fixtures/*.json`) for D2's
  replay harness. Proof `l1-vocabulary.schema-conformance` green in `apps/proofs`
  (decode, JSON round-trip stability, schema version, ignorable-by-default,
  subagent scoping).

## Acceptance Criteria

1. All MS-C1–C6 proof obligations green in CI.
2. agent-ui's chat API is ingress + attach + adapter; the five bespoke modules
   named in MS-M1–M4 are deleted.
3. The regression list passes against the new stack in production.
4. The orphaned-turn and double-POST defects are impossible-by-proof
   (`session.turn-crash-terminal`, `session.lifecycle-single-writer`), not
   merely unobserved.
5. Second-harness smoke passes with zero UI changes.
6. Every RFC invariant added by this SDD appears in
   `rfc/agent-substrate/operating/conformance.md` mapped to a green proof.

## Risks / Open Questions

- **S2 append/read latency envelope on the chat hot path** — C2's design keeps
  folds off the token path, but the turn stream itself must sustain chat-rate
  appends with acceptable attach latency; measure early in C2, not at M1.
- **L1 vocabulary base** — the C6 architect gate; wrong choice here is the most
  expensive to unwind.
- **Wake liveness statement** — no substrate push means at least one live host
  per shard; the RFC must say this plainly (same operational posture as any
  partition-leader system, but it forecloses "fully serverless" readings).
- **`fireline` profile naming** — **resolved (WP F3)**: re-documented, not
  renamed. The `fireline` profile family is frozen (legacy Rust `crates/fireline-*`,
  reference-only); current `fluent-firegrid` profiles use the `.fluent.md`
  suffix and cite the S2 substrate canon. C6/D-lane harness-adapter profiles are
  authored as `.fluent.md` pages. Convention is authoritative in
  [`../rfc/agent-substrate/README.md`](../rfc/agent-substrate/README.md#implementation-profiles).
- **Fable/TS seam placement** — active substrate is `src/Firegrid.Log` /
  `src/Firegrid.Store` (F#→JS) with the product surface in `packages/`; each
  capability's module placement follows the package-boundary principle (stable
  seams first, packages later) and is proposed per work packet in the lanes doc.
