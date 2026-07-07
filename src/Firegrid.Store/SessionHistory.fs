namespace Firegrid.Store

open Firegrid.Log
open Firegrid.Foundation

/// MS-C4 (A4) — the *projection half* of MS-C4: a durable **session-history
/// fold** plus a **thread-index projection**, completing the table side of the
/// duality.
///
/// The generic `Projection<'fact,'history>` core composes I4 `Checkpoint`
/// (long-lived cold rebuild from `latest snapshot + suffix`) with the A3
/// `StateReads` seam (a resident reader *seeded from the latest checkpoint*, so
/// even live reads skip `Seq 0`; `AppliedTail` makes projection lag data) over
/// one subject with one `apply` — all three consumed AS-IS (no shape change).
///
/// The `Turns` binding folds B3's session log **(I6, `SessionLifecycle.LifecycleFact`
/// on `sessions/{s}/log`)** directly — no new schema, no second write — and
/// preserves `EndCause` losslessly (never the `TurnTerminal` collapse, per the
/// #103 TimedOut ruling). `History = { Order; ByTurn }`, where `ByTurn` is the
/// thread-index. Projections are rebuildable, never alternate truth.
///
/// EffSharp-free: `Async` + `Result` + DU facts + `Map`/list history + `Codec`
/// records over the P2/I4/A3 primitives.
module SessionHistory =
    // ---- Generic core: a checkpointed (I4) + readable (A3) fold -----------

    /// A durable, long-lived projection over a session-fact subject. Abstract —
    /// the consumer never sees the checkpoint sidecar, the reader's cursor, or
    /// seq nums.
    type Projection<'fact, 'history> =
        private
            { Basin: S2.Basin
              Source: SubjectHistory.SubjectId
              FactCodec: SubjectHistory.Codec<'fact>
              Initial: 'history
              Apply: 'history -> SubjectHistory.StoredRecord<'fact> -> 'history
              Fold: Checkpoint.Fold<'fact, 'history> }

    /// Bind a projection over `source` (its checkpoint sidecar is derived). The
    /// `StateCodec` snapshots `'history` for the sidecar; the record `Codec`
    /// decodes facts.
    let make
        (basin: S2.Basin)
        (factCodec: SubjectHistory.Codec<'fact>)
        (stateCodec: Checkpoint.StateCodec<'history>)
        (source: SubjectHistory.SubjectId)
        (initial: 'history)
        (apply: 'history -> SubjectHistory.StoredRecord<'fact> -> 'history)
        : Projection<'fact, 'history> =
        { Basin = basin
          Source = source
          FactCodec = factCodec
          Initial = initial
          Apply = apply
          Fold = Checkpoint.make basin factCodec stateCodec source initial apply }

    /// Cold rebuild via I4: `latest snapshot + suffix` to the source tail (a
    /// fold-from-zero when never checkpointed). No resident memory.
    let rebuild (projection: Projection<'fact, 'history>) : Async<'history * SubjectHistory.Version> =
        Checkpoint.rebuild projection.Fold

    /// Snapshot the current history to the sidecar (I4 open-CAS `checkpoint`).
    let checkpoint
        (projection: Projection<'fact, 'history>)
        : Async<Result<Checkpoint.Snapshot<'history>, Checkpoint.CommitFailure<'history>>> =
        Checkpoint.checkpoint projection.Fold

    /// Latest committed checkpoint, or None (I4 `latest`).
    let latest (projection: Projection<'fact, 'history>) : Async<Checkpoint.Snapshot<'history> option> =
        Checkpoint.latest projection.Fold

    /// A resident, live reader over the history — the A3 `StateReads.Reader`.
    type Reader<'fact, 'history> = StateReads.Reader<'fact, 'history>

    /// Start a reader seeded from the latest checkpoint (`Checkpoint.resumeFrom`),
    /// so it live-tails the suffix only rather than folding from `Seq 0`.
    let startReader (projection: Projection<'fact, 'history>) : Async<Reader<'fact, 'history>> =
        async {
            let! snapshot = Checkpoint.latest projection.Fold
            let resumeSeq, seed = Checkpoint.resumeFrom snapshot projection.Initial
            return! StateReads.start projection.Basin projection.FactCodec projection.Source resumeSeq seed projection.Apply
        }

    /// Eventual read: the local applied history (a monotonic prefix; `AppliedTail`
    /// exposes the projection's lag as data).
    let readEventual (reader: Reader<'fact, 'history>) : Async<ViewState<'history>> = StateReads.readEventual reader

    /// Strong read at the checked tail (linearizable).
    let readLatest (reader: Reader<'fact, 'history>) : Async<ViewState<'history>> = StateReads.readLatest reader

    let stopReader (reader: Reader<'fact, 'history>) : Async<unit> = StateReads.stop reader

    // ---- Turn binding: session turn-history + thread-index over I6 --------

    /// The Turn-aware binding: A4's history is the L2 turn index folded directly
    /// from B3's session log (I6). It preserves `EndCause` losslessly.
    module Turns =
        /// A turn's status. `Ended` keeps the L2 `EndCause`
        /// (`Done | Failed | Cancelled | IdleTimeout | MaxDurationTimeout`) — never
        /// the `TurnTerminal` collapse, so timeouts never read as cancels.
        type TurnStatus =
            | Live
            | Ended of SessionLifecycle.EndCause

        type TurnEntry =
            { Turn: Turn.TurnId
              Status: TurnStatus
              OpenedAt: int64
              ClosedAt: int64 option }

        /// The folded session history AND its thread-index. `Order` is the turns
        /// newest-first (prepended, O(1) per fact — not a naive append-to-tail);
        /// `ByTurn` is the derived index, keyed by the turn-id *string* so the
        /// checkpoint sidecar's `StateCodec` can JSON-serialize the `Map`.
        type History =
            { Order: Turn.TurnId list
              ByTurn: Map<string, TurnEntry> }

        /// Fold source = B3's session log (I6). No new subject, no second write.
        let subject (session: Turn.SessionId) : SubjectHistory.SubjectId = SessionLifecycle.logSubject session

        let initial: History = { Order = []; ByTurn = Map.empty }

        /// Fold `LifecycleFact` (I6) directly: `TurnStarted` opens a `Live` entry;
        /// `TurnEnded` closes it, recording the `EndCause`. A re-seen `TurnStarted`
        /// (same-identity re-attach) folds once.
        let apply (history: History) (record: SubjectHistory.StoredRecord<SessionLifecycle.LifecycleFact>) : History =
            match record.Body with
            | SessionLifecycle.TurnStarted(turnId, startedAt) ->
                let (Turn.TurnId key) = turnId

                match history.ByTurn.TryFind key with
                | Some _ -> history
                | None ->
                    { Order = turnId :: history.Order
                      ByTurn =
                        history.ByTurn
                        |> Map.add
                            key
                            { Turn = turnId
                              Status = Live
                              OpenedAt = startedAt
                              ClosedAt = None } }
            | SessionLifecycle.TurnEnded(turnId, cause, endedAt) ->
                let (Turn.TurnId key) = turnId

                match history.ByTurn.TryFind key with
                | Some entry ->
                    { history with
                        ByTurn =
                            history.ByTurn
                            |> Map.add
                                key
                                { entry with
                                    Status = Ended cause
                                    ClosedAt = Some endedAt } }
                | None ->
                    { Order = turnId :: history.Order
                      ByTurn =
                        history.ByTurn
                        |> Map.add
                            key
                            { Turn = turnId
                              Status = Ended cause
                              OpenedAt = endedAt
                              ClosedAt = Some endedAt } }

        /// The thread-index projection: turns indexed by id — a derived view.
        let threadIndex (history: History) : Map<string, TurnEntry> = history.ByTurn

        // ---- History StateCodec for the I4 checkpoint sidecar ----------------

        let private causeTag (cause: SessionLifecycle.EndCause) : string =
            match cause with
            | SessionLifecycle.Done -> "done"
            | SessionLifecycle.Failed _ -> "failed"
            | SessionLifecycle.Cancelled -> "cancelled"
            | SessionLifecycle.IdleTimeout -> "idle"
            | SessionLifecycle.MaxDurationTimeout -> "max"

        let private causeReason (cause: SessionLifecycle.EndCause) : string =
            match cause with
            | SessionLifecycle.Failed reason -> reason
            | _ -> ""

        let private decodeCause (tag: string) (reason: string) : Result<SessionLifecycle.EndCause, string> =
            match tag with
            | "done" -> Ok SessionLifecycle.Done
            | "failed" -> Ok(SessionLifecycle.Failed reason)
            | "cancelled" -> Ok SessionLifecycle.Cancelled
            | "idle" -> Ok SessionLifecycle.IdleTimeout
            | "max" -> Ok SessionLifecycle.MaxDurationTimeout
            | other -> Error(sprintf "unknown end cause '%s'" other)

        let private encodeEntry (entry: TurnEntry) =
            let (Turn.TurnId turn) = entry.Turn

            let status, cause, reason =
                match entry.Status with
                | Live -> "live", "", ""
                | Ended endCause -> "ended", causeTag endCause, causeReason endCause

            {| turn = turn
               status = status
               cause = cause
               reason = reason
               openedAt = string entry.OpenedAt
               closed = entry.ClosedAt.IsSome
               closedAt = string (defaultArg entry.ClosedAt 0L) |}

        let private decodeEntry (raw: obj) : Result<TurnEntry, string> =
            try
                let turnId = Turn.TurnId(JsJson.stringProp "turn" raw)
                let openedAt = System.Int64.Parse(JsJson.stringProp "openedAt" raw)

                let closedAt =
                    if JsJson.stringProp "closed" raw = "true" then
                        Some(System.Int64.Parse(JsJson.stringProp "closedAt" raw))
                    else
                        None

                match JsJson.stringProp "status" raw with
                | "live" ->
                    Ok
                        { Turn = turnId
                          Status = Live
                          OpenedAt = openedAt
                          ClosedAt = closedAt }
                | "ended" ->
                    decodeCause (JsJson.stringProp "cause" raw) (JsJson.stringProp "reason" raw)
                    |> Result.map (fun cause ->
                        { Turn = turnId
                          Status = Ended cause
                          OpenedAt = openedAt
                          ClosedAt = closedAt })
                | other -> Error(sprintf "unknown turn status '%s'" other)
            with error ->
                Error error.Message

        /// JSON codec for the `History` snapshot on the checkpoint sidecar. Encodes
        /// the ordered entries (both `Order` and `ByTurn` are reconstructed from
        /// them, so the round-trip is exact).
        let stateCodec: Checkpoint.StateCodec<History> =
            { Encode =
                fun history ->
                    let entries =
                        history.Order
                        |> List.choose (fun turnId ->
                            let (Turn.TurnId key) = turnId
                            history.ByTurn.TryFind key |> Option.map encodeEntry)
                        |> List.toArray

                    JsJson.stringify {| entries = entries |}
              Decode =
                fun body ->
                    try
                        let parsed = JsJson.parse<obj> body
                        let raws = JsJson.prop<obj[]> parsed "entries" |> Array.toList
                        let decoded = raws |> List.map decodeEntry

                        match decoded |> List.tryPick (function
                                                       | Error message -> Some message
                                                       | Ok _ -> None) with
                        | Some message -> Error message
                        | None ->
                            let entries =
                                decoded
                                |> List.choose (function
                                    | Ok entry -> Some entry
                                    | Error _ -> None)

                            Ok
                                { Order = entries |> List.map (fun entry -> entry.Turn)
                                  ByTurn =
                                    entries
                                    |> List.map (fun entry ->
                                        let (Turn.TurnId key) = entry.Turn
                                        key, entry)
                                    |> Map.ofList }
                    with error ->
                        Error error.Message }

        /// Bind the projection for a session over `SessionLifecycle.logSubject`
        /// (I6), folding `LifecycleFact` with `SessionLifecycle.lifecycleCodec`.
        let make (basin: S2.Basin) (session: Turn.SessionId) : Projection<SessionLifecycle.LifecycleFact, History> =
            make basin SessionLifecycle.lifecycleCodec stateCodec (subject session) initial apply
