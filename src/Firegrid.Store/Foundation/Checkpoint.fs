namespace Firegrid.Foundation

open Firegrid.Log
open Firegrid.Foundation.SubjectHistory

/// MS-C1 — Checkpointed fold (the *table half* of the table/stream duality).
///
/// A durable fold whose state is periodically *snapshotted* so a cold host
/// reconstructs it as `latest snapshot + suffix replay` instead of replaying
/// from `Seq 0`. Generic and domain-free; grounded in the P2-ported
/// `SubjectHistory`. Snapshots live on a derived sidecar subject
/// (`checkpointSubject source`, never a random name) so trimming the source
/// never drops a snapshot, and the snapshot log is itself an ordinary open-CAS
/// log. Deps are passed in (sans-IO shell): an `S2.Basin` and codecs; no
/// EffSharp.
///
/// **Version is the exclusive upper bound** (the P2 convention): a
/// `Snapshot.AsOf` is the source `Version` its `State` has folded *through* —
/// the `State` reflects every source record with `Seq < AsOf`, and `rebuild`
/// resumes the fold from `Seq AsOf`. `AsOf = Version 0` is the empty fold.
///
/// **Checkpoint election is open-CAS, not fenced-owner.** `commit` routes
/// through `Authority.admit` — the I5 (B1) Open (bare-authority) regime, whose
/// single-winner CAS at the observed sidecar tail *is* checkpoint election — not
/// a private CAS path and not the FencedOwner regime. Two writers race and the
/// conditional append decides one winner; the loser gets `Raced`.
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
    type Fold<'record, 'state> =
        private
            { Basin: S2.Basin
              Source: SubjectId
              Sidecar: SubjectId
              RecordCodec: Codec<'record>
              SnapshotCodec: Codec<Snapshot<'state>>
              Initial: 'state
              Apply: 'state -> StoredRecord<'record> -> 'state }

    [<RequireQualifiedAccess>]
    type CommitFailure<'state> =
        | Raced of AppendConflict<Snapshot<'state>> // open-CAS: another checkpointer took the slot
        | Regressed of requested: Version * latest: Version // AsOf <= latest committed AsOf (stale state)
        | Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type TrimFailure =
        | AheadOfCheckpoint of requested: Version * committed: Version // would trim past the snapshot
        | Failed of S2Errors.S2Failure

    /// Derived (never random) sidecar address for a source subject's snapshots.
    let checkpointSubject (SubjectId source) : SubjectId = SubjectId(source + "/checkpoints")

    /// Frame a snapshot as `AsOf|state`. `AsOf` is a pure integer with no `|`,
    /// so splitting on the first separator recovers the state verbatim even when
    /// the encoded state itself contains `|`.
    let private snapshotCodec (stateCodec: StateCodec<'state>) : Codec<Snapshot<'state>> =
        { Encode = fun snapshot -> sprintf "%d|%s" (versionNumber snapshot.AsOf) (stateCodec.Encode snapshot.State)
          Decode =
            fun body ->
                let separator = body.IndexOf('|')

                if separator < 0 then
                    Error(sprintf "malformed snapshot record (no separator): %s" body)
                else
                    let asOfText = body.Substring(0, separator)
                    let stateText = body.Substring(separator + 1)

                    match System.Int64.TryParse asOfText with
                    | false, _ -> Error(sprintf "malformed snapshot AsOf: %s" asOfText)
                    | true, asOf ->
                        match stateCodec.Decode stateText with
                        | Ok state -> Ok { AsOf = Version asOf; State = state }
                        | Error error -> Error error }

    /// Bind a fold over `source` (its sidecar is derived).
    let make
        (basin: S2.Basin)
        (recordCodec: Codec<'record>)
        (stateCodec: StateCodec<'state>)
        (source: SubjectId)
        (initial: 'state)
        (apply: 'state -> StoredRecord<'record> -> 'state)
        : Fold<'record, 'state> =
        { Basin = basin
          Source = source
          Sidecar = checkpointSubject source
          RecordCodec = recordCodec
          SnapshotCodec = snapshotCodec stateCodec
          Initial = initial
          Apply = apply }

    /// Pure rebuild plan (sans-IO core): from a latest snapshot (or none) decide
    /// the resume Seq and the seed state. `None -> (Seq 0, initial)`.
    let resumeFrom (snapshot: Snapshot<'state> option) (initial: 'state) : Seq * 'state =
        match snapshot with
        | None -> (Seq 0L, initial)
        | Some snapshot -> (Seq(versionNumber snapshot.AsOf), snapshot.State)

    /// Read exactly one record at `seqN` on `subject`, or None if the slot is
    /// empty. Cursor lifetime is managed like `SubjectHistory.foldTo` — closed
    /// on both the success and failure paths, never twice.
    let private readOneAt basin codec subject seqN =
        async {
            let! cursor = SubjectHistory.openCursor basin codec subject (Seq seqN)
            let mutable closing = false

            try
                let! item = SubjectHistory.tryNext cursor
                closing <- true
                do! SubjectHistory.closeCursor cursor

                match item with
                | Ok value -> return value
                | Error error -> return failwith error
            with error ->
                if not closing then
                    do! SubjectHistory.closeCursor cursor

                return raise error
        }

    /// Latest committed snapshot on the sidecar, or None when never checkpointed.
    let latest (fold: Fold<'record, 'state>) : Async<Snapshot<'state> option> =
        async {
            let! sidecarTail = SubjectHistory.tail fold.Basin fold.Sidecar
            let count = versionNumber sidecarTail

            if count <= 0L then
                return None
            else
                let! record = readOneAt fold.Basin fold.SnapshotCodec fold.Sidecar (count - 1L)
                return record |> Option.map (fun stored -> stored.Body)
        }

    /// Fold the source from `fromSeq` (inclusive) up to `untilVer` (the
    /// exclusive as-of upper bound), decoding each *data* record via the record
    /// codec and applying it. Command records are filtered with
    /// `IgnoreCommandRecords`: `trim` appends a trim marker to the source, and a
    /// rebuild that resumes across it must skip the marker rather than hand its
    /// binary body to the record codec. This is why the source fold reads S2
    /// directly (bounded batch reads that end at the tail) instead of
    /// `SubjectHistory.foldTo`, which surfaces command records and would decode
    /// the trim marker as a source record.
    let private foldSourceData (fold: Fold<'record, 'state>) (Seq fromSeq) (Version untilVer) (seed: 'state) : Async<'state> =
        async {
            let (SubjectId sourceName) = fold.Source
            let stream = fold.Basin |> S2.stream sourceName
            let mutable state = seed
            let mutable pos = fromSeq
            let mutable go = pos < untilVer

            while go do
                let! batch =
                    stream
                    |> S2.readWith
                        { S2.ReadOptions.empty with
                            Start = Some(S2.FromSeqNum pos)
                            IgnoreCommandRecords = true }

                match batch with
                | [] -> go <- false
                | records ->
                    let mutable lastSeq = pos - 1L

                    for record in records do
                        if record.SeqNum > lastSeq then
                            lastSeq <- record.SeqNum

                        if record.SeqNum < untilVer then
                            match fold.RecordCodec.Decode record.Body with
                            | Ok body -> state <- fold.Apply state { Seq = Seq record.SeqNum; Body = body }
                            | Error error ->
                                failwithf "checkpoint rebuild: decode failed at seq %d: %s" record.SeqNum error

                    let next = lastSeq + 1L
                    // Stop at the as-of bound or when a batch makes no progress
                    // (only command records remained ahead of `pos`).
                    if next >= untilVer || next <= pos then go <- false else pos <- next

            return state
        }

    /// Rebuild = latest snapshot + suffix replay to the source tail. With no
    /// snapshot this is a fold-from-zero. Returns the folded state and the
    /// source Version it is as-of. No resident memory — a cold Fold rebuilds.
    let rebuild (fold: Fold<'record, 'state>) : Async<'state * Version> =
        async {
            let! snapshot = latest fold
            let resumeSeq, seed = resumeFrom snapshot fold.Initial
            let! sourceTail = SubjectHistory.tail fold.Basin fold.Source
            let! state = foldSourceData fold resumeSeq sourceTail seed
            return state, sourceTail
        }

    /// Commit a snapshot to the sidecar under open-CAS at its observed tail.
    /// The election routes through `Authority.admit` — the I5 Open (bare-authority)
    /// regime, whose single-winner CAS *is* checkpoint election, per the MS-C1
    /// surface — never a private CAS path. Two racing writers: exactly one wins;
    /// the loser gets `Raced` (`Authority.AdmitError.Lost`). Snapshots are
    /// monotonic: rejects `Regressed` when `snapshot.AsOf <= latest.AsOf` (a slow
    /// checkpointer cannot overwrite `latest` with stale state).
    let commit
        (fold: Fold<'record, 'state>)
        (snapshot: Snapshot<'state>)
        : Async<Result<Version, CommitFailure<'state>>> =
        async {
            let! sidecarTail = SubjectHistory.tail fold.Basin fold.Sidecar
            let count = versionNumber sidecarTail

            let! latestAsOf =
                if count <= 0L then
                    async { return Version 0L }
                else
                    async {
                        let! record = readOneAt fold.Basin fold.SnapshotCodec fold.Sidecar (count - 1L)

                        return
                            match record with
                            | Some stored -> stored.Body.AsOf
                            | None -> Version 0L
                    }

            if versionNumber snapshot.AsOf <= versionNumber latestAsOf then
                return Error(CommitFailure.Regressed(snapshot.AsOf, latestAsOf))
            else
                let! admitted =
                    Authority.admit fold.Basin fold.SnapshotCodec fold.Sidecar sidecarTail [ snapshot ]

                match admitted with
                | Ok version -> return Ok version
                | Error(Authority.AdmitError.Lost conflict) -> return Error(CommitFailure.Raced conflict)
                | Error(Authority.AdmitError.Failed failure) -> return Error(CommitFailure.Failed failure)
        }

    /// Convenience: rebuild to the current source tail, then commit that snapshot.
    let checkpoint (fold: Fold<'record, 'state>) : Async<Result<Snapshot<'state>, CommitFailure<'state>>> =
        async {
            let! state, asOf = rebuild fold
            let snapshot = { AsOf = asOf; State = state }
            let! appended = commit fold snapshot

            match appended with
            | Ok _ -> return Ok snapshot
            | Error failure -> return Error failure
        }

    /// Trim the source behind a committed snapshot. `upTo` must be <= the latest
    /// committed snapshot's AsOf (`AheadOfCheckpoint` otherwise); a reader
    /// starting at the trim floor still rebuilds equivalent state.
    let trim (fold: Fold<'record, 'state>) (upTo: Version) : Async<Result<unit, TrimFailure>> =
        async {
            let! snapshot = latest fold

            let committed =
                match snapshot with
                | Some snapshot -> snapshot.AsOf
                | None -> Version 0L

            if versionNumber upTo > versionNumber committed then
                return Error(TrimFailure.AheadOfCheckpoint(upTo, committed))
            else
                try
                    // Trim records before `upTo` on the source. `rebuild` resumes
                    // from `Seq AsOf` (>= the trim floor), so the trimmed prefix is
                    // never read — A2's `state.trim-safety` proof drives this.
                    let (SubjectId sourceName) = fold.Source

                    let! _ =
                        fold.Basin
                        |> S2.stream sourceName
                        |> S2.append [ S2.Record.trim (versionNumber upTo) ]

                    return Ok()
                with error ->
                    return Error(TrimFailure.Failed(S2Errors.classify error))
        }
