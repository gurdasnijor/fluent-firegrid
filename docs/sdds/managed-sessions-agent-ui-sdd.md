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

Decision (made here): a wake record is a **pointer, never a payload** — it names
the subject to drive and *why* (`WakeReason`), and nothing else. The triggering
message, timer payload, or child terminal already lives durably on the subject's
own mailbox/log; the shard stream carries no state a reader would otherwise miss.
Consequence: losing every wake on a shard degrades *latency* to the sweep floor,
never *correctness* — the router is an accelerator in front of the sweep, not a
new source of truth. Alternative (wake carries the message body, so a drive skips
re-reading the mailbox) rejected: it forks state across two logs and makes the
wake path correctness-load-bearing.

Decision (made here): leadership is the `Authority` **FencedOwner** regime (I5,
WP B1) applied to the shard subject — exactly one live router per shard, deposal
by epoch increment. C1 does **not** invent a shard-lease or a third fence idiom;
the router's `claim` is an `Authority.claim` instance, and the durable cursor is
the `Mailbox` fenced-checkpoint pattern (P3's `MailboxCheckpoint` lineage), not a
bespoke offset store.

Target Surface (F# — WP C1; gate G6 + G1 sign-off pending):

The wake path is the actor reading of MS-C3 from
[`../canon/architecture/fluent/authority-and-actors.md`](../canon/architecture/fluent/authority-and-actors.md):
**a wake shard is a shard-granularity mailbox, and the router is its actor — its
mailbox IS the shard stream.** It factors into two generic F# modules in the
durable-kernel package, co-located with and composed from the P3-ported
`Processor`/`Mailbox` and the B1 `Authority` protocol (I5), reusing the kernel's
one pointer vocabulary (`ActorAddress`, `WakeReason`) rather than minting a
parallel one:

- **`WakeShard`** — the frozen cross-lane interface **I3**: the wake-record
  schema (`{ Subject; Reason }`) and the deterministic shard naming
  (`subject → shard → stream`). Consumed by B3 (lifecycle wakes) and future
  temporal features; changing the record schema *or* the naming scheme is a G1
  gate.
- **`WakeRouter`** — the tail-driven actor: claim the shard (FencedOwner), tail
  the stream with wait, dispatch each fresh wake to its target exactly once, and
  advance a fenced durable cursor. A binding/combinator over
  `Authority` + `Mailbox` + `Processor`, not a new drive loop.

C1 depends on P1 only (ledger). This is a **sequencing** rule, not a fence
fallback: C1's implementation builds the **pure core** immediately (`WakeShard`,
`codec`, `shardOf`, `plan` — none of which touch `Authority`); the **shell**
(`tick`/`run`) compiles only once B1's `Authority` implementation is merged.
(B1-IMPL is now merged to `main`, so the shell is unblocked — but this remains
the documented rule.) The router **never** ships a raw-fence variant: if B1 were
ever unmerged when the shell is ready, C1 pauses and reports rather than
re-introducing the parallel fence idiom this surface bans. C2's
`wake.single-claim` election drives through `Authority.claim` (FencedOwner),
exactly as A1/A2 route checkpoint election through `Authority.admit`.

```fsharp
// namespace Firegrid.Store.Foundation.Durable — the actor kernel (Processor/Mailbox lineage).
// open Firegrid.Foundation                     // SubjectHistory (P2), Authority (I5, B1)
// Reuses the kernel's pointer vocabulary: ActorAddress, WakeReason (Processor.fs).
// Deps passed in (sans-IO shell): S2.Basin + Authority + the Mailbox cursor pattern. No EffSharp.

module WakeShard =
    /// I3 — a shard identifier: 0 <= id < Count. Shards partition the subject
    /// space so exactly one router tails each shard's stream.
    type ShardId = ShardId of int
    /// I3 — fixed deployment parameter: how many shard streams exist under a
    /// namespace. Resharding (changing Count) is a canon non-goal (no rebalancing
    /// protocol); a Count change is an operational migration, out of C1 scope.
    type ShardConfig = { Namespace: string; Count: int }

    /// I3 — the wake record: a POINTER, never a payload. `Subject` is the actor to
    /// drive; `Reason` is why. Reason reuses the kernel `WakeReason`
    /// (MailboxReady | TimerFired | ChildTerminal) so there is ONE wake
    /// vocabulary, not two. Changing this shape is a G1 gate.
    type WakeRecord = { Subject: ActorAddress; Reason: WakeReason }

    /// Codec for the shard stream. Records are open-appended by any poster. The
    /// shard stream NEVER seals — it is an open-append mailbox forever, with no
    /// terminal record; do not import B1 `DurableLog`'s seal semantics onto the
    /// wake path by analogy (a wake shard has no lifecycle end).
    val codec : SubjectHistory.Codec<WakeRecord>

    /// I3 — stable, deployment-independent mapping subject → shard. Pinned in full
    /// so any later change is a visible G1 reshard, never a silent one:
    ///   key   = UTF-8 bytes of `String.concat "/" subject.Segments`
    ///   hash  = FNV-1a, 32-bit, read UNSIGNED (offset basis 2166136261u,
    ///           prime 16777619u, each step `(h ^^^ byte) * prime` wrapping mod 2^32)
    ///   shard = ShardId (int (hash % uint32 Count))
    /// Branch-free, no BigInt, no BCL hashing (never `Object.GetHashCode`) — so it
    /// is bit-identical across the .NET host and P4's Fable→TS emit. Same subject →
    /// same shard for a fixed `Count`. No existing `src/` helper to cite; C1
    /// introduces this pinned function as part of I3. (Total-function guard: a
    /// `Count < 1` is clamped to 1 so the modulus is never zero; a valid
    /// deployment has `Count >= 1`, where the guard never changes a result.)
    val shardOf : ShardConfig -> ActorAddress -> ShardId
    /// I3 — derived (never random) shard-stream subject: `"{ns}/wake/{shardId}"`.
    val shardSubject : ShardConfig -> ShardId -> SubjectHistory.SubjectId
    /// Convenience: the shard stream a subject's wakes land on (`shardOf` then
    /// `shardSubject`).
    val subjectShard : ShardConfig -> ActorAddress -> SubjectHistory.SubjectId

    /// Post a wake for `subject`: open-append the pointer to its shard stream (the
    /// router's mailbox). ANY process may post — the shard stream is an
    /// open-append mailbox; delivery to the target is the router's job. Dedupe is
    /// the router's cursor + idempotent drive, not the poster's concern; N posts
    /// for one subject cost at most N idempotent drives.
    val post :
        S2.Basin -> ShardConfig -> subject: ActorAddress -> WakeReason
            -> Async<Result<unit, S2Errors.S2Failure>>

module WakeRouter =
    /// The consumed prefix of this router's shard stream as an EXCLUSIVE upper
    /// bound (the house `Version` convention): `NextSeq` is the next sequence to
    /// consume — every `Seq < NextSeq` is already consumed (dispatched or skipped),
    /// so a fresh router resumes AT `NextSeq` and re-dispatches nothing. Committed
    /// as a fenced record on the router's own cursor log (the `MailboxCheckpoint`
    /// pattern), recovered on claim from a bounded tail window — no resident
    /// memory, no re-drive of the prefix.
    type Cursor = { Shard: WakeShard.ShardId; NextSeq: SubjectHistory.Seq }

    /// The drive dependency (sans-IO seam): "ensure subject S is claimed and
    /// ticked for reason R." The router core decides WHICH subjects to dispatch
    /// and in what order; the shell performs the drive (a `Processor` tick on the
    /// target). Injected so the core stays pure/deterministic and C2's proofs can
    /// substitute a recording driver. The drive is idempotent by the target's own
    /// claim: driving an already-current subject is a no-op tick.
    type Drive = ActorAddress -> WakeReason -> Async<Result<unit, string>>

    [<RequireQualifiedAccess>]
    type RouterError =
        | ClaimFailed of Authority.ClaimError
        | Deposed of by: Authority.Epoch        // a newer epoch took the shard; step aside
        | ReadFailed of S2Errors.S2Failure
        | DriveFailed of ActorAddress * error: string
        | CursorCommitFailed of S2Errors.S2Failure

    /// Pure decision core (sans-IO): given the current cursor and the batch of
    /// records scanned off the shard tail (`None` = an undecodable pointer),
    /// produce the ordered dispatch list and the advanced cursor. Deterministic —
    /// no clock, no I/O. Dispatches only decodable records at `Seq >= NextSeq`
    /// (strict dedup); an undecodable record is *consumed and skipped* — its
    /// subject degrades to the sweep — so a single poison pointer (any process may
    /// post) cannot wedge the shard. The advanced cursor's `NextSeq` is the last
    /// SCANNED `Seq + 1` (exclusive upper bound over the whole consumed prefix,
    /// decodable or not). C2's `wake.timer-exactly-once` pins this boundary.
    val plan :
        Cursor -> scanned: (SubjectHistory.Seq * WakeRecord option) list
            -> dispatch: (ActorAddress * WakeReason) list * next: Cursor

    /// One claimed tick: claim the shard (FencedOwner), fold the cursor, read the
    /// shard tail with wait, `plan` the fresh dispatches, drive each, then commit
    /// the advanced cursor under the fence. Blocks-with-wait for the next wake (the
    /// `SubjectHistory.openCursorWithWait` idiom) so an appended wake reaches
    /// dispatch without a poll interval. Returns `Deposed` if a newer epoch rotated
    /// the fence at commit — the deposed router computed but could not advance.
    val tick :
        S2.Basin -> ShardConfig -> WakeShard.ShardId -> Authority.HolderId -> Drive
            -> Async<Result<Cursor, RouterError>>

    /// Run the router loop until deposed or the async is cancelled: claim once,
    /// then `tick` continuously, tailing the stream. Degraded mode: if no router
    /// runs a shard, the existing sweep still drives the work — wakes accelerate,
    /// sweeps guarantee (the liveness invariant: at least one live host per shard;
    /// no substrate push).
    val run :
        S2.Basin -> ShardConfig -> WakeShard.ShardId -> Authority.HolderId -> Drive
            -> Async<Result<unit, RouterError>>
```

Ergonomic sample (the consumer test — usable with no proof-harness knowledge):

```fsharp
// Producer side (e.g. B3 lifecycle): post a wake — a pointer, not a payload.
// Any process may post; the subject's mailbox/log already holds the real state.
let! _ = WakeShard.post basin shardCfg subject WakeReason.MailboxReady

// Router side: `drive` claims + ticks the target actor (supplied by the host;
// idempotent, so a replayed dispatch is a harmless no-op tick).
let drive : WakeRouter.Drive =
    fun target reason -> driveSubjectActor basin target reason

// Run the shard the subject maps to. One live router per shard (FencedOwner).
match! WakeRouter.run basin shardCfg (WakeShard.shardOf shardCfg subject) holderId drive with
| Ok () -> ()                                        // ran until cancelled
| Error (WakeRouter.RouterError.Deposed _) -> ()     // a newer epoch owns the shard; step aside
| Error _ -> ()                                      // transient; the sweep still covers the work
```

Laws (stated here; all three proofs land in **WP C2** — C1 ships the shard
stream + tailed router + durable cursor, not the timer index or these proofs):

- **Wake is a pointer, not a payload.** A `WakeRecord` carries only
  `Subject + Reason`; the triggering state stays on the subject's own
  mailbox/log. A drive re-reads that state, so a wake never duplicates truth and
  redundant wakes coalesce (N wakes → at most N idempotent drives, each observing
  all pending state).
- **Single live router per shard** (`wake.single-claim`, C2). Two routers tailing
  one shard: `Authority.claim` (FencedOwner) yields exactly one live holder; the
  loser is `Deposed` and can neither dispatch nor advance the cursor. It observes
  the claim, not the work — the `store-object-live-fencing` lineage applied to a
  shard.
- **Durable cursor / effectively-exactly-once dispatch**
  (`wake.timer-exactly-once`, C2). The cursor is a fenced checkpoint with
  `NextSeq` an exclusive upper bound; `plan` skips `Seq < NextSeq` strictly and
  advances `NextSeq` to the last *scanned* `Seq + 1` (the whole consumed prefix).
  A restart resumes from the committed cursor — unconsumed wakes are not dropped
  and consumed wakes are not re-flooded (bounded replay of at most the in-flight
  batch). Dispatch is at-least-once with an idempotent drive, so a due timer fires
  effectively exactly once across a router restart; a not-yet-posted timer
  survives unfired.
- **Poison tolerance** (`wake.timer-exactly-once`, C2). Because any process may
  post, an undecodable record is *consumed and skipped* — the cursor advances past
  it and its subject degrades to the sweep — so one malformed pointer cannot
  head-of-line-block a shard. This is the degraded-mode contract applied
  per-record: a lost wake costs latency, never correctness. The proof extends
  `wake.timer-exactly-once` to pin it: a poison record is consumed, the cursor
  passes it, subsequent wakes still dispatch, and a router restart does not
  re-wedge on it.
- **Tail latency, no poll** (`wake.tail-latency`, C2). `tick` blocks-with-wait on
  the shard tail (`openCursorWithWait`), so an appended wake reaches its claimed
  handler within a bound *asserted from trace evidence in the proof* (bound
  recorded there, not in prose).
- **Degraded mode is the sweep.** No live router for a shard ⇒ work still drives
  via the existing sweep (`store-runtime-timer-sweep`,
  `store-runtime-schedule-sweep` stay green). Wakes are a latency accelerator in
  front of the sweep, never its replacement; the RFC liveness statement (≥1 live
  host per shard; no substrate push) is the floor.
- **Deterministic sharding.** `shardOf` is the pinned FNV-1a-32 over the UTF-8
  address key, mod `Count` (above); same subject → same shard for a fixed
  `Count`, bit-identical on .NET and Fable→TS. Resharding is a canon non-goal.
- **No leaks.** Consumers see `ActorAddress` / `WakeReason` / `ShardId` — never S2
  stream names, fence tokens, seq nums, or the cursor subject.

Scope guard (both directions): C1 delivers the **shard wake stream + tailed
router + durable cursor** only. The **folded timer index** (what decides *when* a
`TimerFired` wake is posted) and the three named proofs are **WP C2** — C1 posts
and routes timer wakes but does not build the wheel. No shard manager, no
rebalancing, no resident actor processes, no location directory, no distributed
liveness clock (the canon non-goals) — `Count` is a fixed deployment parameter,
addressing is naming, activation is claiming. The router reuses `Authority` (I5)
leadership, the `Mailbox` fenced cursor, and the `Processor` drive; it is a
binding, not a new fence idiom or a second drive loop. Wakes never become the
sole correctness path — the sweep stays the floor. (Deferred, recorded: the shard
stream's consumed prefix grows unbounded; its trimming rides A-lane
checkpoint+trim behind the router's committed cursor later — the A1
sidecar-compaction precedent — and is out of C1 scope.) (Deferred, recorded:
poison skips are **silent** in C1 (`WakeRouter` discards the decode error and
maps the record to `None`), so systematic poison — e.g. codec version skew from a
newer poster — would degrade a whole shard to sweep-floor latency with no signal
beyond forensic trace reading. An observability seam (skip count / structured
log) is deferred, revisited at C2 or first operational need — the silent
degradation is a recorded decision, not an accident.)

Validation Gates (F#-zone, G6 — per
[`fsharp-fable-effsharp-evaluation-sdd.md`](./fsharp-fable-effsharp-evaluation-sdd.md)):

- **Native shapes, EffSharp-free.** `Async` + `Result` + DU errors + `Codec`
  records + a pure `plan`/`shardOf` core — no EffSharp (per the 2026-07-06
  deprecation), so the package-level EffSharp decision is "none, nothing new."
- **Ergonomic sample** above passes the consumer test — a host wires `post` and a
  `Drive`, with no proof-harness knowledge.
- **Works-without-EffSharp** is inherent (nothing introduced). **Emitted-TS
  sample + Fable build check** ride P4's Fable→TS facade; the surface is
  Fable-safe by construction — `shardOf` is a content hash (no BCL-only
  `GetHashCode`), and it adds no API beyond what `SubjectHistory`/`Mailbox`
  already emit.

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

MS-C5 is delivered by two WPs: **B3** (this surface — session claim /
single-writer start, durable cancel, and durable timeouts) and **B4** (the
fenced native-resume-artifact store, a sibling module whose
`session.resume-artifact-fenced` obligation and surface refinement land with B4).
B3 and B4 share the **MS-C5 claim convention**: One session holder identity
claims both the session log and the resume register; a takeover `start` claims
the resume register before any re-hydration read, so a stale writer's late
`store` is fenced out before it can fork harness state. (B4 binds its own
register fence at `sessions/{s}/resume`; the two fences share the holder
identity, not one fence — this convention, not "grounded on B3's fence.")

Target Surface (F# — WP B3; G6 approved with required changes folded; G1 fork
decided — keep I1 unchanged, 2026-07-07):

Lifecycle is a **session-actor policy** layered on B1's `Authority` (**I5**) and
`DurableLog`/`Turn` (**I1**), per
[`../canon/architecture/fluent/authority-and-actors.md`](../canon/architecture/fluent/authority-and-actors.md)
("Session (managed agent) = the actor; prompts and cancels are mailbox sends").
It invents **no second authority**: `start` is an `Authority.claim`, termination
is a `DurableLog.seal`, and cancel is an ordinary **mailbox send**. It is one
domain module, `SessionLifecycle`, in the F# `src/` zone
(`src/Firegrid.Store/SessionLifecycle.fs`), sans-IO: a pure decision core
(`fold`/`onCommand`/`onWake` — the session Handler, the Processor's pluggable
part) plus an `Async` shell (`start`/`cancel`/`drive`).

**Topology — three derived subjects per session** (all named from the
`SessionId` the client already holds, never random):

| Subject | Regime | Role |
| --- | --- | --- |
| `sessions/{s}/log` | FencedOwner (I5) | the session actor's own authoritative log; its fold is the **durable `activeRuns`** — which turn, if any, is live. The session outlives its turns, so it is never sealed per-turn. |
| `sessions/{s}/in` | Open-append (InboxFold lineage) | the session **inbox**: cancels (and later prompts) land here as mailbox sends; senders need no authority. |
| `sessions/{s}/turns/{t}` | FencedOwner + Sealed (I1 `Turn`) | the turn stream for one execution, created under the session holder's identity, sealed at terminal. |

**Single-writer via composition, not a new mechanism.** A `start` claims the
session log (`Authority.claim`) and reads the fold. Two concurrent starts are
resolved by *both* I5 and policy: the session claim fences the loser, so its
`TurnStarted` fails to commit; and the `AlreadyLive` policy rejects a start that
observes a *different* live turn. A same-`turnId` start re-attaches
(`DurableLog.create` re-claims, deposing any stale producer) — recovery, never a
fork. The `AlreadyLive` rejection is exactly the lifecycle policy that
`DurableLog.create` defers to MS-C5.

**Durable cancel is a mailbox send.** `cancel` is an open-append to
`sessions/{s}/in` carrying `(source, sourceSeq)` provenance — any process may
call it without holding the session, and it is **not** a bespoke control
channel. The holder admits it on its next `drive` (provenance-deduped) and seals
the live turn; a resend folds once.

**How the cancel reaches a running adapter** (the sentence D2's #99 R4 defers to
this surface). The holder seals the turn on admitting the cancel; a concurrently
running producer/adapter then observes cancellation as a `Deposed`/sealed append
failure on its **next emit** — D2's `EmitError.deposed|sealed` law: it computed
output but could not commit it. An in-process host **may** additionally interrupt
the adapter fiber as an *optimization* (to stop wasted work sooner), never as the
mechanism — the durable seal, not fiber interruption, is what terminates the turn.

**Durable timeouts, no baked-in clock.** A deadline is a function of the durable
fold (turn `startedAt`; last-chunk time on the turn stream), so any host
**re-arms** it on claim — it survives restart and is not a process-local timer.
The fire is delivered as a `WakeReason.TimerFired` by the kernel wake path, whose
concrete wake-record + shard naming is **I3** (C1, PR #95, under architect
review) — cited here and consumed abstractly; MS-C5 does **not** depend on I3's
shape. In MS-C3's degraded sweep/poll mode the same deadline is recomputed, so
correctness never hinges on tail wakes. Time enters the pure core only as
`now: Timestamp` data (the sans-IO rule).

```fsharp
// namespace Firegrid.Store — session-actor lifecycle POLICY over the generic
// Authority (I5), DurableLog/Turn (I1), and the kernel timer/wake path (I3, C1).
// open Firegrid.Foundation            // Authority, DurableLog, SubjectHistory
// open Firegrid.Store.Foundation.Durable  // WakeReason, TimerId, Timestamp — the
//   kernel wake vocabulary (I3/C1); consumed here, NOT re-minted (one vocabulary).
// Deps passed in (sans-IO shell): S2.Basin + the I1 Turn codec. No EffSharp.

module SessionLifecycle =
    // ---- Kernel wake vocabulary (consumed, NOT re-minted) -----------------
    // `Timestamp`, `TimerId`, and `WakeReason` are the kernel's, from
    // `Foundation/Durable/Processor.fs` — this surface consumes them so C1's
    // router (`Drive : ActorAddress -> WakeReason -> …`) drives a session with
    // no mapping shim between lanes. Time still enters the pure core only as
    // `now: Timestamp` data (the sans-IO rule); it is never read ambiently.
    //   type Timestamp   = int64
    //   type TimerId     = TimerId of string
    //   type WakeReason  = MailboxReady | TimerFired of TimerId * Timestamp
    //                    | ChildTerminal of SubjectId

    // ---- Derived addresses (never random) ---------------------------------
    /// Session authoritative log (FencedOwner, I5): the durable `activeRuns`.
    val logSubject   : Turn.SessionId -> SubjectHistory.SubjectId
    /// Session inbox (open-append, InboxFold lineage): mailbox sends land here.
    val inboxSubject : Turn.SessionId -> SubjectHistory.SubjectId

    // ---- Durable timeout policy (data, not a clock) -----------------------
    /// Idle + max-duration bounds (ms). Durable by construction: a deadline is a
    /// function of the fold, so any host re-arms it on claim. `None` = unbounded.
    type Timeouts =
        { Idle: int64 option            // max quiet gap since the last turn chunk
          MaxDuration: int64 option }   // absolute cap since turn start
    val noTimeouts : Timeouts

    // ---- Session-log schema (L2 coordination facts; holder-only) ----------
    /// Why a turn ended, recorded on the SESSION log. The turn stream's terminal
    /// stays the I1 `TurnTerminal` unchanged — cancel and both timeouts map it to
    /// `TurnTerminal.Cancelled`; the distinct CAUSE is this L2 fact. (G1 fork
    /// DECIDED — keep I1 unchanged, no first-class `TurnTerminal.TimedOut`; see the
    /// gate-boundary note below for the G6 rationale.)
    type EndCause =
        | Done                          // producer completed normally
        | Failed of reason: string
        | Cancelled                     // external cancel (mailbox send)
        | IdleTimeout
        | MaxDurationTimeout
    /// Exhaustive `EndCause → TurnTerminal` mapping (the turn-stream terminal),
    /// so the collapse is stated, not implied:
    ///   Done               → TurnTerminal.Completed
    ///   Failed reason      → TurnTerminal.Failed reason
    ///   Cancelled          → TurnTerminal.Cancelled
    ///   IdleTimeout        → TurnTerminal.Cancelled
    ///   MaxDurationTimeout → TurnTerminal.Cancelled
    /// The three abnormal-stop causes collapse to `Cancelled` on the turn stream;
    /// `EndCause` preserves the distinction losslessly on the session log.
    /// Session-log record schema (I5-fenced; only the holder appends).
    type LifecycleFact =
        | TurnStarted of Turn.TurnId * startedAt: Timestamp
        | TurnEnded   of Turn.TurnId * EndCause * endedAt: Timestamp

    // ---- Inbox schema (open-append mailbox sends) -------------------------
    /// A durable control message. Cancel is the only command in B3; prompts join
    /// later (MS-C6/E). `(Source, SourceSeq)` makes a resend fold once.
    type Command = Cancel of Turn.TurnId
    type Sent = { Source: string; SourceSeq: int64; Command: Command }

    // ---- Pure decision core (sans-IO; Fable-safe; deterministic) ----------
    type State                          // abstract; live turn + arming info
    val initial  : State
    val fold     : State -> SubjectHistory.StoredRecord<LifecycleFact> -> State
    /// The live turn, if any — the durable `activeRuns` answer for this session.
    val liveTurn : State -> Turn.TurnId option

    // A wake the holder must act on is the kernel `WakeReason` (above), NOT a
    // private DU: `MailboxReady` admits the inbox; `TimerFired` re-checks the
    // durable deadlines; `ChildTerminal` is `noop` here by policy (B3 has no
    // child subjects — reserved for later composition). `WakeReason.TimerFired`
    // carries the timer's delivery timestamp, but the processing-time fact
    // `onWake` decides on is the separate `now: Timestamp` (see below).
    /// The decision as data (not the effect): seal the live turn, append session
    /// facts, (re)arm / disarm durable timers. `Arm` lowers to the kernel
    /// `Intent.SetTimer`; `Disarm` is **advisory** — the kernel `Intent` is
    /// `SetTimer | Send | Execute` with no cancel-timer, so a stale/late fire is
    /// already `noop`-guarded by the pure core (`onWake` seals only when `now`
    /// passes the deadline). If a first-class cancel-timer intent is ever wanted,
    /// that is Lane-P coordination (like the InboxFold note), not a mechanism B3
    /// mints here.
    type Outcome =
        { Seal:   (Turn.TurnId * TurnTerminal) option
          Append: LifecycleFact list
          Arm:    (TimerId * dueAt: Timestamp) list
          Disarm: TimerId list }         // advisory — see note above
    val noop : Outcome

    /// Pure: state + admitted command + `now` → outcome. `Cancel` of the live
    /// turn seals it `Cancelled`; `Cancel` of a non-live / already-ended turn is
    /// `noop` — idempotent.
    val onCommand : Timeouts -> State -> now: Timestamp -> Sent -> Outcome
    /// Pure: state + `now` + kernel wake → outcome. `now: Timestamp` is the
    /// processing-time fact the law depends on (`WakeReason.TimerFired`'s own
    /// timestamp is only the delivery's claim, and `TurnEnded.endedAt` has no
    /// other source) — symmetric with `onCommand`. An idle/max timer whose
    /// deadline `now` has passed seals the live turn; a not-yet-due / stale
    /// `TimerFired`, a `MailboxReady` with nothing to admit, or a `ChildTerminal`
    /// is `noop`.
    val onWake    : Timeouts -> State -> now: Timestamp -> WakeReason -> Outcome

    // ---- Shell: start / cancel / drive (Async; composes I5 + I1) -----------
    [<RequireQualifiedAccess>]
    type StartError =
        | AlreadyLive of Turn.TurnId    // single-writer policy: a different live turn holds the session
        | Claim of Authority.ClaimError
        | Failed of S2Errors.S2Failure

    /// A started turn: the I1 turn `Producer` bound under the session claim, plus
    /// the session `Holder` and folded `State`. Only this holder appends output.
    type LiveTurn                       // abstract

    /// Start a turn on `session` as `holderId`: `Authority.claim` the session log,
    /// then the SINGLE-WRITER policy over the fold —
    ///   • no live turn        → record `TurnStarted`, `DurableLog.create` the
    ///                           turn stream under `holderId`, arm idle/max timers.
    ///   • live turn = `turnId`→ re-attach (create re-claims / deposes a stale
    ///                           producer): recovery, never a fork.
    ///   • live turn ≠ `turnId`→ reject `AlreadyLive`.
    /// A deposed racer's `TurnStarted` also fails under the session fence, so two
    /// concurrent starts yield exactly one live turn.
    val start :
        S2.Basin -> Timeouts -> Turn.SessionId -> Turn.TurnId -> Authority.HolderId
            -> now: Timestamp -> Async<Result<LiveTurn, StartError>>

    /// Append one turn chunk under the session fence (re-arms the idle timer). A
    /// live *deposed* producer fails `Deposed` — it computes but cannot commit.
    val append   : LiveTurn -> TurnChunk -> Async<Result<unit, DurableLog.AppendError>>
    /// Seal the turn normally (`TurnTerminal.Completed` + `TurnEnded Done`),
    /// disarm timers. First-valid-terminal-wins (I1).
    val complete : LiveTurn -> now: Timestamp -> Async<Result<unit, DurableLog.AppendError>>

    [<RequireQualifiedAccess>]
    type CancelError = Failed of S2Errors.S2Failure
    /// Durable cancel — a MAILBOX SEND (open-append to `inboxSubject`), NOT a
    /// control channel and NOT authority: any process may call it. Idempotent by
    /// `(source, sourceSeq)`; the holder observes it on its next `drive` and seals
    /// the turn `Cancelled`. Returns once the send is durable.
    val cancel :
        S2.Basin -> Turn.SessionId -> Turn.TurnId
            -> source: string -> sourceSeq: int64 -> Async<Result<unit, CancelError>>

    [<RequireQualifiedAccess>]
    type Progress =
        | Idle                          // nothing to admit / no deadline reached
        | Advanced                      // facts / timers committed, turn still live
        | Ended of EndCause             // the turn was sealed (cancel / timeout)
        | Deposed                       // a newer holder owns the session; step down
    [<RequireQualifiedAccess>]
    type DriveError =
        | Mailbox of S2Errors.S2Failure
        | Failed of S2Errors.S2Failure

    /// One holder drive tick: admit the inbox (provenance-deduped), apply the pure
    /// `onCommand`/`onWake` decisions (seal the turn, append facts, (re)arm timers)
    /// under the session fence. `wake` is the kernel `WakeReason` (`MailboxReady`
    /// admits the inbox; `TimerFired` re-checks deadlines; `ChildTerminal` is
    /// `noop`), so C1's router (`Drive : ActorAddress -> WakeReason -> …`) drives a
    /// session with no shim. Sealing under a rotated fence surfaces `Deposed`.
    val drive :
        LiveTurn -> WakeReason -> now: Timestamp -> Async<Result<Progress, DriveError>>
```

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

**Pairing with D2's `NativeResumeArtifact` (owned there) and D1's `firegrid/native`
(both cited, not redefined).** The artifact is the durable storage schema for D2's
ratified-pending `NativeResumeArtifact = { harness, version, payload }` (#99) — **D2
owns that shape**; this store is *not* a second definition. `Harness`/`Version`/
`Payload` round-trip D2's `harness`/`version`/`payload` field-for-field (D3
serializes `payload: unknown` into the opaque `Payload: string`), so D3 loses no
round-trip fidelity the day it stores its first artifact. `NativeType`/`Turn` are
additive kernel/adapter-side enrichment: they let a harness adapter (WP D3) read
the fenced artifact to re-hydrate the harness and surface the same native fact to
the UI as a D1 `firegrid/native` (I2) record — `Harness`/`NativeType`/`Payload`
mirror that L1 extension. This store holds the durable, fenced *source* of that
state; it redefines **neither** vocabulary (see
[`../canon/architecture/fluent/l1-observation-vocabulary.md`](../canon/architecture/fluent/l1-observation-vocabulary.md),
§`firegrid/native`). The `Payload` is opaque to the store — never parsed.

**Fence binding and the claim-then-read convention.** The register's subject is
session-derived (`sessions/{sessionId}/resume`, never random) and its epoch rotates
on takeover, so the fence is genuinely *per session* — that per-session fence is
the mechanism here. Coordinating the register claim with B3's session-log claim is
the shared lifecycle convention that makes re-hydration safe, stated **verbatim on
both sides** (B3 #103 R4 carries this identical sentence): *"One session holder
identity claims both the session log and the resume register; a takeover `start`
claims the resume register before any re-hydration read, so a stale writer's late
`store` is fenced out before it can fork harness state."* The freshness this buys
— that no later stale `store` can change what a resuming holder read — is pinned as
the **Claim-then-read re-hydration** law below, which the
`session.resume-artifact-fenced` proof enforces (interleaving included). (This is
lifecycle policy layered above the primitive — mirroring DurableLog's "AlreadyLive
rejection is lifecycle policy (MS-C5), not a mechanism" — but, unlike AlreadyLive,
it is a *stated, proven* convention because the takeover window it closes is real.)

```fsharp
// namespace Firegrid.Store — domain binding of Authority (I5); native-resume
// concern, session-scoped. open Firegrid.Foundation; open Firegrid.Foundation.SubjectHistory
// Deps passed in (sans-IO shell): S2.Basin. No EffSharp.

/// The harness-native resume payload plus its metadata — the durable, fenced
/// source of the `firegrid/native` (I2) facts a harness adapter surfaces on the
/// read side. Holds the artifact; does NOT redefine the L1 vocabulary.
type ResumeArtifact =
    { Harness: string             // emitting harness id — pairs D2 NativeResumeArtifact.harness / firegrid/native.harness
      Version: int64              // harness-owned artifact version — pairs D2 NativeResumeArtifact.version (round-trip fidelity)
      NativeType: string option   // additive: pairs firegrid/native.nativeType (adapter-side enrichment)
      Payload: string             // opaque resume blob (D2 payload: unknown, serialized; e.g. a Claude session id); never parsed
      Turn: Turn.TurnId option }  // additive provenance: the turn whose completion produced it, if known

module ResumeArtifactStore =
    /// A fenced writer over one session's resume register — wraps an
    /// `Authority.Holder` (FencedOwner, I5). Only the live holder stores; a
    /// different identity deposes it (epoch increment). Abstract.
    type Writer                                 // abstract; wraps Authority.Holder<ResumeArtifact>

    [<RequireQualifiedAccess>]
    type OpenError =
        | Claim of Authority.ClaimError        // pass-through (incl. `Sealed`); no narrowing — lane symmetry with B3 StartError
        | Failed of S2Errors.S2Failure
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

Ergonomic sample — WP B3 (the consumer test — usable with no proof-harness knowledge):

```fsharp
// Producer process: start (single-writer) → append → complete. `now` is data.
match! SessionLifecycle.start basin timeouts session turnId holderId now with
| Ok live ->
    let! _ = SessionLifecycle.append live (Text "hello")   // Error Deposed if fenced out
    let! _ = SessionLifecycle.complete live now
    ()
| Error (SessionLifecycle.StartError.AlreadyLive other) -> ()  // a turn is already live
| Error _ -> ()

// Any OTHER process cancels durably — a mailbox send, no session authority held:
let! _ = SessionLifecycle.cancel basin session turnId "api-pod-7" 0L

// The holder observes the cancel on its next drive and seals the turn.
// The wake is the kernel `WakeReason` (Foundation/Durable), not a private DU:
match! SessionLifecycle.drive live Durable.WakeReason.MailboxReady now with
| Ok (SessionLifecycle.Progress.Ended SessionLifecycle.EndCause.Cancelled) -> ()  // durable Cancelled
| Ok (SessionLifecycle.Progress.Deposed) -> ()                 // a newer host took over
| _ -> ()
```

Laws (stated here; proven by WP B3):

- **Single-writer start** (`session.lifecycle-single-writer`, B3). Two concurrent
  `start`s on one session yield exactly one live turn: the session claim fences
  the loser (its `TurnStarted` fails to commit) and the `AlreadyLive` policy
  rejects a start observing a different live turn — never two producers on one
  session.
- **Durable cancel is a mailbox send** (`session.lifecycle-durable-cancel`, B3).
  `cancel` from a process that is not the producer appends to the inbox; the
  holder's `drive` admits it and seals the turn to a durable
  `TurnTerminal.Cancelled` (`TurnEnded Cancelled` on the session log). A
  duplicate — same `(source, sourceSeq)`, or a cancel of an already-ended turn —
  folds once (idempotent): no second terminal.
- **Deposed producer cannot append** (`session.lifecycle-deposed-producer`, B3).
  Extends `store.object-live-fencing` / `session.turn-crash-terminal` to
  lifecycle: after a takeover `start` rotates the fence, the prior `LiveTurn`'s
  `append`/`complete` fails `Deposed` — it computes but cannot commit. Recovery
  drives the turn to a durable terminal; an attached reader observes it, never
  hangs.
- **Durable, host-independent timeouts.** Idle and max-duration deadlines are
  functions of the durable fold, re-armed on every claim — a timeout survives
  host restart and is not a process-local timer. `onWake` seals only when `now`
  is past the deadline; a stale/duplicate fire is `noop`. Fire delivery is the
  kernel wake path (I3, C1; cited, not depended on); the sweep/poll degraded mode
  (MS-C3) recomputes the same deadline.
- **No second authority.** `start` = `Authority.claim`, termination =
  `DurableLog.seal`, cancel = an open-append send — composing I5, never
  extending it (no I5 shape change → no G1 on I5).
- **No leaks.** Consumers never see S2 stream names, fence tokens, seq nums, or
  the inbox address.

Gate-boundary notes (the first is now **decided** by the G6 ruling and recorded
here for the record; the rest are raised for sign-off, **not** taken in this WP):

- **Turn terminal for timeouts — G1 fork DECIDED: keep I1 unchanged** (architect
  G6 ruling, 2026-07-07). This surface maps cancel *and* both timeouts onto the
  existing `TurnTerminal.Cancelled`, recording the distinct cause only as an L2
  `EndCause` on the session log — so I1 (the `Turn` schema, B1) is **unchanged**.
  The alternative, a first-class `TurnTerminal.TimedOut` on the turn stream, was
  weighed and declined. Rationale of record: (1) **no turn-stream reader behaves
  differently by cause** — at that altitude the operative fact is "no more output,
  not a normal completion"; (2) the **cause of record lives on the session log**,
  which A4's history fold projects, so history/UI get `IdleTimeout` vs `Cancelled`
  without touching I1; (3) D2's adapter contract **freezes `L1Terminal` as
  `completed | cancelled | failed`**, so a first-class `TimedOut` would force a
  *simultaneous* G1 on I1 **and** a change to that contract; (4) **I1 is ratified,
  implemented, and proven** (B2's three green proofs), and no consumer requires the
  distinction at turn altitude to reopen it; (5) the decision is **reversible
  additively later** — `EndCause` remains the durable source of truth, so a future
  first-class terminal would be a mechanical, additive migration.
- **I3 dependency (cited, not depended-on).** Timer-wake delivery is I3's
  wake-record + shard naming (C1, PR #95). MS-C5 consumes `WakeReason.TimerFired`
  abstractly and re-arms deadlines from the fold, so it neither depends on I3's
  concrete shape nor blocks on PR #95. If wiring lifecycle wakes later needs an
  I3 shape change, that is a G1 on I3 — escalated separately.
- **Inbox admission primitive.** The session inbox follows P3's
  `InboxFold`/`Mailbox` lineage (provenance-deduped admission). If B3 needs a
  generic mailbox surface P3 does not yet export publicly, that is Lane-P
  coordination (as A1↔B1 for `Authority.admit`), not a bespoke re-implementation.

Scope guard (both directions): lifecycle is **policy**, not a new primitive — it
adds no authority regime (I5 owns claim/seal) and no generic drive loop (P3's
Processor owns claim→admit→commit; this surface supplies only the session
Handler + domain entry points). The inbox `Command` schema stays minimal (Cancel
only in B3; prompts are MS-C6/E). The fenced native-resume-artifact store and its
`session.resume-artifact-fenced` proof are **WP B4's** sibling surface within
MS-C5, not B3.

Ergonomic sample — WP B4 (the consumer test — usable with no proof-harness knowledge):

```fsharp
// Writer (the session's live holder): claim the fenced register, store the
// harness's resume artifact after a turn. A deposed writer's store fails.
match! ResumeArtifactStore.openWriter basin session holderId with
| Ok writer ->
    let artifact =
        { Harness = "claude-agent-sdk"
          Version = 1L                       // D2 NativeResumeArtifact.version — round-trips as-is
          NativeType = Some "resume"
          Payload = claudeSessionId          // opaque to the store (D2 payload, serialized)
          Turn = Some turnId }
    match! ResumeArtifactStore.store writer artifact with
    | Ok () -> ()
    | Error ResumeArtifactStore.StoreError.Deposed -> ()      // a newer writer took over
    | Error (ResumeArtifactStore.StoreError.Failed _) -> ()
| Error _ -> ()

// Re-hydration (claim-then-read law): a resuming holder claims the register
// FIRST — rotating the epoch, deposing any stale writer — THEN reads, so no
// late stale `store` can fork the state it re-hydrates. (Observers/UI may read
// authority-free, without the claim; re-hydration reads are claim-first.)
match! ResumeArtifactStore.openWriter basin session holderId with
| Ok _writer ->
    match! ResumeArtifactStore.read basin session with
    | Ok (Some artifact) -> resumeHarness artifact.Harness artifact.Payload
    | Ok None -> startFresh ()
    | Error _ -> ()
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
  `store`, or `None` before the first. Because `Writer` wraps
  `Authority.Holder<ResumeArtifact>`, each `openWriter` takeover appends an
  `Authority` fence record onto the same subject stream the artifacts commit to;
  `read` selects the latest *artifact* by **record kind** — fence rotations are S2
  *command* records (`RFence`) and are skipped (`IgnoreCommandRecords`), while
  artifacts are `text` records decoded by the artifact codec — so a bare takeover
  (claim with no subsequent `store`) never shadows the current artifact. Reads are
  authority-free — a reader needs no claim (matches DurableLog `attach`; per the
  canon, readers never need authority). *Re-hydration* reads, however, are
  claim-first (next law).
- **Claim-then-read re-hydration** (`session.resume-artifact-fenced`). A resuming
  holder MUST `openWriter` — rotating the register epoch, deposing any stale writer
  — **before** its re-hydration `read`; then no later stale `store` can change what
  it observed. Without the claim-first order the takeover window is real: a new
  holder deposes the old on the session log (B3), reads the register authority-free,
  re-hydrates, and a stale writer's late `store` (its register epoch never rotated)
  then forks harness state. The authority-free `read` (prior law) stands for
  observers/UI only. The proof pins the exact interleaving: a stale `store` after a
  new holder's read-without-claim must be impossible. Shared convention (verbatim,
  matched on B3 #103 R4): *"One session holder identity claims both the session log
  and the resume register; a takeover `start` claims the resume register before any
  re-hydration read, so a stale writer's late `store` is fenced out before it can
  fork harness state."*
- **Opaque payload; round-trips D2, redefines nothing.** The store never parses
  `Payload`; `Harness`/`Version`/`Payload` round-trip D2's `NativeResumeArtifact`
  (#99) field-for-field — D2 owns the shape, this is its durable storage schema —
  and `Harness`/`NativeType`/`Payload` are the durable, fenced source of the
  `firegrid/native` (I2) facts a harness adapter (D3) surfaces on read. The store
  consumes both vocabularies' shapes; it redefines neither.
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

_WP B3:_

- **Native shapes, EffSharp-free.** `Async` + `Result` + DU errors + `Codec`
  records + a pure `fold`/`onCommand`/`onWake` core with time as `now: Timestamp`
  data — no EffSharp (2026-07-06 deprecation); package-level decision "none,
  nothing new."
- **Ergonomic sample** above passes the consumer test.
- **Emitted-TS sample + Fable build check** ride P4's Fable→TS facade; the surface
  is Fable-safe by construction — it adds no BCL-only API beyond what `Authority`
  / `DurableLog` / `SubjectHistory` already emit.

_WP B4:_

- **Native shapes, EffSharp-free.** `Async` + `Result` + DU errors + a
  `ResumeArtifact` record + a private JSON codec (`JsJson`, as `Turn` uses) — no
  EffSharp (per the 2026-07-06 deprecation), so the package-level EffSharp
  decision is "none, nothing new introduced."
- **Ergonomic sample** above passes the consumer test.
- **Emitted-TS sample + Fable build check** ride P4's Fable→TS facade; the surface
  is Fable-safe by construction — its JSON codec uses the same `JsJson` surface
  `Turn` already emits, adding no BCL-only API beyond `Authority`/`SubjectHistory`.

Proof obligations:

- `session.lifecycle-single-writer` (WP B3) — two concurrent starts for the same
  session: exactly one live turn; the second is rejected (`AlreadyLive`) or
  fenced by policy, never a second producer on the same session.
- `session.lifecycle-durable-cancel` (WP B3) — cancel appended by a process that
  is not the producer terminates the turn to a durable cancelled state; duplicate
  cancel is idempotent.
- `session.lifecycle-deposed-producer` (WP B3) — extends `store.object-live-fencing` to
  turns: a live deposed producer cannot append turn output after takeover.
- `session.resume-artifact-fenced` (WP B4) — the native resume artifact (e.g. Claude
  session id) is written under the session fence; a stale owner's write is
  rejected (closes agent-ui's last-writer-wins session-store race). The proof
  includes the **claim-then-read** interleaving: a new holder that `openWriter`s
  before its re-hydration `read` cannot then observe a stale writer's late `store`
  (that store is fenced `Deposed`) — the "stale-store-after-new-holder-read-
  without-claim" fork must be impossible.

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

D1 shipped that vocabulary as `@firegrid/l1-vocabulary` (interface I2) plus the
decision record
[`../canon/architecture/fluent/l1-observation-vocabulary.md`](../canon/architecture/fluent/l1-observation-vocabulary.md).
The adapter contract below **consumes** it: an adapter lowers harness traffic
into `L1StreamRecord`s and never mints a parallel vocabulary. If the contract
were to require a change to I2, that is gate **G1** — escalate; do not widen the
vocabulary here.

Target Surface (TS — WP D2; gate G6 + G1 sign-off pending):

The adapter is the reconstruction-model seam of
[`../canon/architecture/fluent/harness-io.md`](../canon/architecture/fluent/harness-io.md):
the raw harness owns the model loop and writes nothing durable; the adapter turns
its protocol traffic into L1 observation facts and hands them to the kernel, which
appends them under the turn's fence. Per
[`../canon/architecture/fluent/authority-and-actors.md`](../canon/architecture/fluent/authority-and-actors.md)
the adapter is *an effectful handler that writes only under the Processor's fence,
emitting L1 facts as its Append* — it owns **no** authority, durability,
wait/timer/child semantics, or projection schema.

It factors into a **pure lowering core** (sans-IO — the fixture-replay target)
and an **I/O shell** (the live `drive`), driving against two kernel-provided
seams (the fenced L1 sink and, for gateable adapters, the durable-tool gate) and
referencing one B4 seam (the fenced native-resume-artifact store — MS-C5, cited
not redefined).

```ts
// package @firegrid/harness-adapter — TS zone, Effect shapes per LLMS.md.
// Consumes @firegrid/l1-vocabulary (I2). Owns no durability.
import type { L1StreamRecord } from "@firegrid/l1-vocabulary"
import { Context, Data, Effect, type Scope } from "effect"

// ── Declared interception posture (the durability guarantee differs) ──────
// gateable    — can mediate Firegrid durable tools (transactional, replay-served).
// observe-only — cannot mediate any; harness-native effects are only suppressed.
export type InterceptionCapability = "gateable" | "observe-only"

export interface HarnessCapabilities {
  readonly harness: string                 // stable harness id (matches firegrid/native `harness`)
  readonly interception: InterceptionCapability
  readonly emitsUsage: boolean             // lowers firegrid/usage token/cost facts
  readonly emitsSubagents: boolean         // lowers subagent scoping (firegrid/subagent + parent tool content)
}

// ── Opaque harness resume state — produced here, stored fenced by B4 (MS-C5) ─
export interface NativeResumeArtifact {
  readonly harness: string
  readonly version: number                 // harness-owned artifact version
  readonly payload: unknown                // opaque; e.g. Claude session id + cursor
}

// ── Pure protocol → L1 lowering (sans-IO; deterministic; Effect-free) ─────
// A fold from recorded harness protocol events to L1 records: no I/O, no clock,
// no entropy. Replaying an event log reproduces identical L1 facts — the
// `harness.fixture-replay` target. Per-harness; D3 supplies Claude's, including
// parent_tool_use_id scoping and usage/cost lowering.
export interface HarnessLowering<Event, State> {
  readonly initial: State
  readonly lower: (
    state: State,
    event: Event
  ) => { readonly state: State; readonly records: ReadonlyArray<L1StreamRecord> }
}

// ── Kernel-provided seams the shell drives against ────────────────────────
// Fenced L1 append. `emit` is the adapter's only write; the kernel appends under
// the turn's fence (I1 DurableLog). A deposed/sealed turn surfaces here and the
// adapter must stop — the reconstruction analog of DurableLog.AppendError.
export class L1Sink extends Context.Service<L1Sink, {
  readonly emit: (record: L1StreamRecord) => Effect.Effect<void, EmitError>
}>()("@firegrid/harness-adapter/L1Sink") {}

// Durable-tool mediation. Provided at a *gateable* adapter's layer construction
// (a dependency of its `Layer`), never in `drive`'s R — so an observe-only
// adapter, whose layer has no `ToolGate`, literally cannot obtain one. Pause at a
// Firegrid-mediated tool call; the kernel commits the L2 intent/result and
// returns the committed result to feed back to the harness. Resume serves the
// recorded result, never re-executes (execution-models Model B). L2 authority is
// the runtime's (I5), not this contract's.
export class ToolGate extends Context.Service<ToolGate, {
  readonly mediate: (call: MediatedToolCall) => Effect.Effect<CommittedToolResult, GateError>
}>()("@firegrid/harness-adapter/ToolGate") {}

export interface MediatedToolCall {
  readonly toolCallId: string              // the L1 tool_call this gates
  readonly name: string
  readonly rawInput: unknown
}
export interface CommittedToolResult {
  readonly toolCallId: string
  readonly output: unknown                 // durable L2 result fed back to the harness
}

// ── The adapter service (I/O shell) ───────────────────────────────────────
export interface ResumePoint {
  readonly artifact: NativeResumeArtifact   // prior harness resume state
  // EXCLUSIVE upper bound (house convention — same boundary class as C1 R3 and
  // D1's fold): the I1 Version below which L1 facts are already durable. Resume
  // emits the suffix FROM `observedThrough` onward (facts at Version >=
  // observedThrough); the fact AT observedThrough is the first not-yet-durable one.
  readonly observedThrough: number
}
export interface DriveInput {
  // The user turn: a NON-EMPTY sequence of `user_message_chunk`s that share one
  // `messageId`. Plural because L1ChunkBody.content is a single content block per
  // chunk (ACP; see packages/l1-vocabulary), so a multi-block prompt
  // (text + image + resource) needs several records — a singular record cannot
  // express it, and widening the arity after ratification would be G1.
  readonly prompt: readonly [L1StreamRecord, ...ReadonlyArray<L1StreamRecord>]
  readonly resume?: ResumePoint             // present iff continuing an existing turn/session
}
export type L1Terminal =
  | { readonly _tag: "completed" }
  | { readonly _tag: "cancelled" }          // a cancel was observed and the turn ended
  | { readonly _tag: "failed"; readonly reason: string }
// This contract fixes only the terminal *representation*. How a cancel reaches a
// running `drive` is B3's MS-C5 surface to define (cancel is a mailbox send, per
// canon) — deliberately not pinned here.
export interface DriveOutcome {
  readonly artifact: NativeResumeArtifact    // updated resume state for the kernel to persist (B4-fenced)
  readonly terminal: L1Terminal
}

export class HarnessAdapter extends Context.Service<HarnessAdapter, {
  readonly capabilities: HarnessCapabilities
  // Drive one turn to terminal: lower harness traffic to L1 via `L1Sink.emit` and
  // produce the updated resume artifact. `ToolGate` is deliberately NOT in `drive`'s
  // R — a gateable adapter closes over it at layer construction; an observe-only
  // adapter's layer never depends on it, so it *cannot* mediate. The posture is
  // enforced by the types, not by prose. The harness process is a scoped resource.
  readonly drive: (
    input: DriveInput
  ) => Effect.Effect<DriveOutcome, DriveError, L1Sink | Scope.Scope>
}>()("@firegrid/harness-adapter/HarnessAdapter") {}

// ── Typed errors ──────────────────────────────────────────────────────────
export class DriveError extends Data.TaggedError("DriveError")<{
  readonly harness: string
  readonly message: string
  readonly cause?: unknown
}> {}
export class EmitError extends Data.TaggedError("EmitError")<{
  readonly kind: "deposed" | "sealed" | "failed"
  readonly message: string
}> {}
export class GateError extends Data.TaggedError("GateError")<{
  readonly toolCallId: string
  readonly message: string
}> {}
```

Ergonomic sample (the consumer test — the kernel drives an adapter; the proof
drives the pure core, with no live process):

```ts
// Kernel side (Processor): the claim gives the fenced L1Sink; drive to terminal;
// persist the returned artifact under the same fence (B4).
const runTurn = Effect.fn("runTurn")(function*(input: DriveInput) {
  const adapter = yield* HarnessAdapter
  const outcome = yield* adapter.drive(input)          // emits L1 under the fence
  // Illustrative only — `ResumeArtifactStore` names B4's API and is pending B4
  // ratification (MS-C5); shown here to place the seam, not to fix its shape.
  yield* ResumeArtifactStore.put(outcome.artifact)     // B4 (MS-C5), fenced per session
  return outcome.terminal
})

// Proof side (harness.fixture-replay): replay the PURE lowering over a recorded
// transcript and check against a D1 golden fixture and its fold.
const replay = <E, S>(l: HarnessLowering<E, S>, events: ReadonlyArray<E>) =>
  events.reduce(
    (acc, ev) => {
      const step = l.lower(acc.state, ev)
      return { state: step.state, out: [...acc.out, ...step.records] }
    },
    { state: l.initial, out: [] as ReadonlyArray<L1StreamRecord> }
  ).out
// expect: replay(lowering, events) deep-equals fixture.records; and
//         foldTurn(replay(...)) deep-equals foldTurn(fixture.records).
```

Laws (stated here; proven by WP D2/D3 after approval):

- **Emits only I2, never a parallel vocabulary.** Every fact is an
  `L1StreamRecord`; harness specifics ride `firegrid/native` or the documented
  `firegrid/*` extensions, never a new base variant. A need the base vocabulary
  cannot express is gate G1 (change I2), not an adapter escape hatch.
- **No authority.** The adapter's only write is `L1Sink.emit`; it never appends,
  fences, seals, decides wait/timer/child completion, or owns a projection schema
  (harness-io F-A constraints). `EmitError.deposed`/`sealed` stops the turn — it
  computed but could not commit.
- **Deterministic lowering.** `lower` is pure: the same recorded event sequence
  yields identical L1 records and identical `foldTurn` state
  (`harness.fixture-replay`). All nondeterminism lives in the harness process,
  behind the I/O shell.
- **Resume suppresses, never repeats.** Given a `ResumePoint`, `drive` re-enters
  via the native artifact and emits the suffix from `observedThrough` onward
  (facts at Version >= `observedThrough`, the exclusive-upper-bound convention):
  already-observed L1 facts are not re-emitted and already-observed side effects
  are not re-executed — gateable durable tools are served their recorded L2 result
  and fed back; observe-only harness-native side effects are suppressed by native
  resume (`harness.resume-suppression`).
- **Interception posture is declared and honored.** A `gateable` adapter may call
  `ToolGate.mediate` (durable, transactional results, replay-served on resume) for
  Firegrid-registered tools; an `observe-only` adapter *cannot* — its layer
  provides no `ToolGate`, so the prohibition is a type-level property, not prose.
  Its harness-native side effects are at-least-once-observed but not
  Firegrid-transactional, and the contract says so. The posture is a static
  per-adapter declaration, not a per-call surprise.
- **Subagent output is parent-scoped.** Subagent activity lowers to
  `tool_call_update` content on its `parent_tool_use_id` tool call plus an
  ignorable `firegrid/subagent` record, so `foldTurn` attributes it to the parent
  and never interleaves it into top-level turn text (`harness.subagent-scoping`) —
  exactly what D1's fold and subagent fixture already require.
- **Resume artifact is opaque and fenced elsewhere.** The adapter
  produces/consumes `NativeResumeArtifact` as an opaque, harness-versioned blob;
  its durable, fenced per-session storage is B4 (MS-C5), referenced here, never
  re-implemented. **D2 owns the TS-side `NativeResumeArtifact` shape; B4's F#
  store is generic over an opaque serialized payload plus `(harness, version)`
  metadata and must not redefine the artifact.**

Scope guard (both directions): the adapter owns protocol→L1 lowering and
resume-artifact production; it does not own durability, L2/authority,
wait/timer/child lifecycle, or queryable projections (those are B / A / runtime).
It is the neutral generalization of harness-io's `FiregridAcpClient` and the
native lowering adapter — a Claude Agent SDK adapter (D3) and a future ACP/codex
adapter both implement this one contract. The contract does not grow into a
general RPC or agent-framework surface ahead of a consumer that demands it.

TS-zone shape (G6): Effect service + layer, `Effect.fn` for effectful methods,
tagged errors for expected failures, a sans-IO Effect-free `lower` core, and
consuming `@firegrid/l1-vocabulary`'s public exports only (no deep imports). The
fixture-replay harness (recorded transcripts as fixtures, deterministic
reconstruction as the proof — the lane's main artifact) and any adapter
implementation land in follow-up commits **after** architect approval of this
surface.

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
- **A2 (MS-C1) — proofs green + election on I5 (WP A2).** `Checkpoint.commit`'s
  election now routes through B1's `Authority.admit` (the I5 **Open /
  bare-authority** regime, whose single-winner CAS *is* checkpoint election — not
  FencedOwner, not a private CAS path), with no `Checkpoint` surface change.
  Proofs `state.checkpoint-race` (two racers → exactly one commits; loser
  `Raced`; stale state `Regressed`; monotonic snapshots) and `state.trim-safety`
  (guard `AheadOfCheckpoint`; a reader from the trim floor rebuilds equivalent
  state) green. Trim-safety surfaced and fixed an A1 defect: S2 trim leaves a
  command record on the source, so `rebuild` now folds the source via bounded
  batch reads with `IgnoreCommandRecords` (public S2 API, no surface change)
  rather than `SubjectHistory.foldTo`, which would decode the trim marker. F2
  conformance rows INV-018/019/020 added. Still deferred: sidecar compaction.
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
- **B2 / MS-C2 — turn-stream proofs green (WP B2).** `session.turn-streams` in
  `Firegrid.Foundation.Proofs` (`FoundationTurnStreamProof.fs`) proves the three
  MS-C2 obligations over the public `DurableLog`/`Turn` surface on real `s2Lite`:
  `session.turn-attach` (byte-identical mid-flight vs. start prefix + same
  terminal), `session.turn-crash-terminal` (a live deposed producer cannot
  commit after a fresh-connection recovery host takes over; recovery reaches a
  durable terminal; the attached reader observes it — the `store.object-live-fencing`
  technique adapted to the foundation runner), `session.turn-idempotent-create`
  (same-identity re-attach → one stream never forked; different-identity → epoch
  takeover deposing prior producers). Conformance rows INV-015/016/017 added.
- **B3 / MS-C5 — turn lifecycle authority shipped + proofs green (WP B3).**
  `SessionLifecycle` (`src/Firegrid.Store/SessionLifecycle.fs`) implements the
  MS-C5 Target Surface exactly: a session-actor **policy** over B1's `Authority`
  (I5) and `DurableLog`/`Turn` (I1) — **no second authority**. `start` =
  `Authority.claim` the session log + the single-writer `AlreadyLive` policy over
  the fold; durable cancel = a `(source, sourceSeq)`-deduped **mailbox send**
  (open-append to `sessions/{s}/in`) the holder seals on its next `drive`; durable
  timeouts = deadlines re-derived from the fold, fired via the kernel
  `WakeReason.TimerFired` (the I3/C1 vocabulary consumed, not re-minted). Cancel
  and both timeouts map to the unchanged I1 `TurnTerminal.Cancelled`; the distinct
  cause is the L2 `EndCause` on the session log (the decided G1 posture — I1
  unchanged). Pure `fold`/`onCommand`/`onWake` core + `Async` shell, EffSharp-free,
  Fable-safe (JS + TS emit green). Proofs `session.lifecycle-{single-writer,
  durable-cancel,deposed-producer}` in `FoundationSessionLifecycleProof.fs` green
  on real `s2Lite`, driven through the public surface only; the deposed-producer
  proof extends B2's two-host live-fencing technique. Conformance rows
  INV-021/022/023 added. B4's `session.resume-artifact-fenced` is the sibling MS-C5
  obligation, still open.
- **MS-C6 / WP D1 — L1 observation vocabulary (I2).** Shipped
  `@firegrid/l1-vocabulary` (`packages/l1-vocabulary`): the ACP `session/update`
  superset schema, Effect-free decoder, and canonical `foldTurn` base fold, with
  the G2 decision record at
  [`../canon/architecture/fluent/l1-observation-vocabulary.md`](../canon/architecture/fluent/l1-observation-vocabulary.md)
  and the seed fixture corpus (`packages/l1-vocabulary/fixtures/*.json`) for D2's
  replay harness. Proof `l1-vocabulary.schema-conformance` green in `apps/proofs`
  (decode, JSON round-trip stability, schema version, ignorable-by-default,
  subagent scoping).
- **MS-C6 / WP D2 — harness adapter contract (implementation).** Shipped
  `@firegrid/harness-adapter` (`packages/harness-adapter`) implementing the
  architect-approved MS-C6 Target Surface: the ratified contract types + service
  seams (`HarnessAdapter`, `L1Sink`, `ToolGate`, tagged errors), the pure
  Effect-free `replay` lowering core, the generic `drive` reconstruction shell
  (`makeReconstructionAdapter` — prompt validation, resume-suppression by the
  exclusive-upper-bound `observedThrough`, fenced emit via `L1Sink`), and the
  ACP-native `referenceLowering` (deliberately not the Claude adapter — D3). The
  interception posture is type-enforced: `ToolGate` is a gateable adapter's layer
  dependency, absent from `drive`'s `R`, so an observe-only adapter cannot obtain
  one. Proofs `harness.fixture-replay` and `harness.resume-suppression` green in
  `apps/proofs` over the D1 seed corpus. Conformance rows INV-021/022/023 added.
  The side-effect-non-re-execution half of resume-suppression, subagent scoping,
  and usage/cost lowering land with the WP D3 Claude adapter.
- **C1 / MS-C3 (I3) — wake path shipped (WP C1).** `WakeShard`
  (`src/Firegrid.Store/Foundation/Durable/WakeShard.fs`) and `WakeRouter`
  (`.../WakeRouter.fs`) implement the MS-C3 Target Surface: the I3 wake-record
  schema (`WakeRecord = { Subject; Reason }`, a pointer reusing the kernel
  `WakeReason` — not a parallel vocabulary), pinned deterministic sharding
  (`shardOf` = FNV-1a-32 over UTF-8 of `String.concat "/" Segments`, read
  unsigned `mod Count`; verified against canonical vectors, Fable-safe via
  `Math.imul`/`TextEncoder`), `shardSubject`/`subjectShard`/`post` (open-append
  shard mailbox that never seals), and the tail-driven router: leadership through
  `Authority.claim` (FencedOwner, I5 — never a private fence), a fenced durable
  cursor on the router's own log recovered from a bounded tail window (`NextSeq`
  exclusive upper bound; pure `plan` skips `Seq < NextSeq` strictly and advances
  over the whole consumed prefix, so an undecodable poison pointer is skipped —
  degraded to the sweep — instead of wedging the shard), `openCursorWithWait`
  tailing with safe cursor close, drive-before-commit so a deposal leaves only
  idempotent re-drives, and an injected sans-IO `Drive` seam. F#-native,
  EffSharp-free, Fable-safe (JS + TS emit green). Degraded mode preserved: the
  sweeps stay the correctness floor. Deferred to WP C2: the folded timer index
  and the three `wake.*` proofs (`wake.tail-latency`, `wake.single-claim`,
  `wake.timer-exactly-once`); shard-stream retention rides A-lane
  checkpoint+trim.

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
