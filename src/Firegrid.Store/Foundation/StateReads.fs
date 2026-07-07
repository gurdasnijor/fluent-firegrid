namespace Firegrid.Foundation

open Firegrid.Log
open Firegrid.Foundation.SubjectHistory

/// MS-C4 (A3) — the *read half* of MS-C4: the P2-ported `StateView` fold exposed
/// as a linearizable **strong** vs lagging **eventual** read model at the
/// `@firegrid/store` P4 seam (re-exported through `Exports.fs`), so a consumer
/// reads session state Promise-first without touching S2.
///
/// Generic and domain-free. It **consumes P2's `StateView`
/// (`start`/`read Eventual|Strong`/`stop`) and `SubjectHistory` as-is** — no
/// shape change to either P2 module. It invents no schema and no new S2 access;
/// the version convention is P2's.
///
/// **Version is the exclusive upper bound** (the P2 convention): `AppliedTail`
/// is the source `Version` the local fold has applied *through* — the snapshot
/// reflects every record with `Seq < AppliedTail`, so a strong read *through*
/// `v` reflects every record with `Seq < v`. `AppliedTail` makes staleness data.
///
/// **Reads need no authority.** A reader only folds; strong-read linearizability
/// comes from the S2 check-tail barrier, not a lease.
module StateReads =
    /// A live read handle over a subject's fold — the P2 `StateView`, resident
    /// and tailing. Abstract at the seam: the consumer never sees the pump,
    /// cursor, stream name, or seq nums.
    type Reader<'record, 'state> = StateView<'record, 'state>

    /// Start a reader over `source`, folding from `recoverFrom` with `initial` +
    /// `apply`. Thin over `StateView.start`.
    let start
        (basin: S2.Basin)
        (codec: Codec<'record>)
        (source: SubjectId)
        (recoverFrom: Seq)
        (initial: 'state)
        (apply: 'state -> StoredRecord<'record> -> 'state)
        : Async<Reader<'record, 'state>> =
        StateView.start basin codec source recoverFrom initial apply

    /// Eventual read: the local applied snapshot — a monotonic prefix that never
    /// regresses across successive reads and may lag the committed tail.
    /// `ViewState.AppliedTail` exposes exactly how far it has folded.
    let readEventual (reader: Reader<'record, 'state>) : Async<ViewState<'state>> =
        StateView.read Eventual reader

    /// Strong read *through a requested version* (linearizable, read-your-writes):
    /// resolves once the reader has applied through `through`, returning a
    /// snapshot reflecting every commit with `Seq < through`. Fast path: return
    /// the local snapshot when it has already applied through `through`; else one
    /// fallback `read Strong` (a check-tail barrier that waits through the current
    /// tail, which is `>= through` for a committed version). `through` must be a
    /// committed version (out of contract beyond the committed tail).
    let readThrough (through: Version) (reader: Reader<'record, 'state>) : Async<ViewState<'state>> =
        async {
            let! snapshot = StateView.read Eventual reader

            if versionNumber snapshot.AppliedTail >= versionNumber through then
                return snapshot
            else
                return! StateView.read Strong reader
        }

    /// Strong read at the current committed tail: a check-tail barrier that
    /// observes every commit acknowledged before the read — including a *second
    /// host's* acknowledged append. Thin over `StateView.read Strong`.
    let readLatest (reader: Reader<'record, 'state>) : Async<ViewState<'state>> =
        StateView.read Strong reader

    /// Stop the reader and release its cursor. Thin over `StateView.stop`.
    let stop (reader: Reader<'record, 'state>) : Async<unit> = StateView.stop reader
