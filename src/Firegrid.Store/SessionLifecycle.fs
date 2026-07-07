namespace Firegrid.Store

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store.Foundation.Durable

/// MS-C5 — turn lifecycle authority as a **session-actor policy** layered on B1's
/// `Authority` (I5) and `DurableLog`/`Turn` (I1), per
/// `docs/canon/architecture/fluent/authority-and-actors.md`
/// ("Session (managed agent) = the actor; prompts and cancels are mailbox sends").
///
/// It invents **no second authority**: `start` is an `Authority.claim`,
/// termination is a `DurableLog.seal`, and cancel is an ordinary **mailbox send**
/// (an open-append to the session inbox). A pure decision core
/// (`fold`/`onCommand`/`onWake` — the session Handler) plus an `Async` shell
/// (`start`/`append`/`complete`/`cancel`/`drive`). Sans-IO: time enters the pure
/// core only as `now: Timestamp` data. EffSharp-free, Fable-safe.
///
/// Three derived subjects per session (all named from the `SessionId` the client
/// already holds, never random):
///   `sessions/{s}/log`  — FencedOwner (I5): the session's authoritative log; its
///                         fold is the durable `activeRuns` (which turn is live).
///   `sessions/{s}/in`   — open-append (InboxFold lineage): cancels land here as
///                         mailbox sends; senders need no authority.
///   `sessions/{s}/turns/{t}` — FencedOwner + Sealed (I1 `Turn`): the turn stream.
///
/// The kernel wake vocabulary (`Timestamp`, `TimerId`, `WakeReason`) is the
/// kernel's own (`Foundation/Durable/Processor.fs`, I3/C1) — consumed here, NOT
/// re-minted, so C1's router (`Drive : ActorAddress -> WakeReason -> …`) drives a
/// session with no mapping shim.
module SessionLifecycle =

    // ---- Derived addresses (never random) ---------------------------------

    let private subjectName (SubjectHistory.SubjectId value) = value

    /// Session authoritative log (FencedOwner, I5): the durable `activeRuns`.
    let logSubject (Turn.SessionId session) : SubjectHistory.SubjectId =
        SubjectHistory.SubjectId(String.concat "/" [ "sessions"; session; "log" ])

    /// Session inbox (open-append, InboxFold lineage): mailbox sends land here.
    let inboxSubject (Turn.SessionId session) : SubjectHistory.SubjectId =
        SubjectHistory.SubjectId(String.concat "/" [ "sessions"; session; "in" ])

    // ---- Durable timeout policy (data, not a clock) -----------------------

    /// Idle + max-duration bounds (ms). Durable by construction: a deadline is a
    /// function of the fold, so any host re-arms it on claim. `None` = unbounded.
    type Timeouts =
        { Idle: int64 option
          MaxDuration: int64 option }

    let noTimeouts: Timeouts = { Idle = None; MaxDuration = None }

    // ---- Session-log schema (L2 coordination facts; holder-only) ----------

    /// Why a turn ended, recorded on the SESSION log. The turn stream's terminal
    /// stays the I1 `TurnTerminal` unchanged — cancel and both timeouts map it to
    /// `TurnTerminal.Cancelled`; the distinct CAUSE is this L2 fact. (G1 fork
    /// DECIDED, 2026-07-07 — keep I1 unchanged, no first-class `TurnTerminal.TimedOut`.)
    type EndCause =
        | Done
        | Failed of reason: string
        | Cancelled
        | IdleTimeout
        | MaxDurationTimeout

    /// Exhaustive `EndCause -> TurnTerminal` mapping (the turn-stream terminal), so
    /// the collapse is stated, not implied. The three abnormal-stop causes collapse
    /// to `Cancelled` on the turn stream; `EndCause` preserves the distinction
    /// losslessly on the session log.
    let terminalOf (cause: EndCause) : TurnTerminal =
        match cause with
        | Done -> TurnTerminal.Completed
        | Failed reason -> TurnTerminal.Failed reason
        | Cancelled -> TurnTerminal.Cancelled
        | IdleTimeout -> TurnTerminal.Cancelled
        | MaxDurationTimeout -> TurnTerminal.Cancelled

    /// Session-log record schema (I5-fenced; only the holder appends).
    type LifecycleFact =
        | TurnStarted of Turn.TurnId * startedAt: Timestamp
        | TurnEnded of Turn.TurnId * EndCause * endedAt: Timestamp

    // ---- Inbox schema (open-append mailbox sends) -------------------------

    /// A durable control message. Cancel is the only command in B3; prompts join
    /// later (MS-C6/E). `(Source, SourceSeq)` makes a resend fold once.
    type Command = Cancel of Turn.TurnId

    type Sent =
        { Source: string
          SourceSeq: int64
          Command: Command }

    // ---- Pure decision core (sans-IO; Fable-safe; deterministic) ----------

    /// Abstract: the live turn (with its `startedAt`, the deadline basis) — the
    /// durable `activeRuns` answer for this session.
    type State = private { Live: (Turn.TurnId * Timestamp) option }

    let initial: State = { Live = None }

    let fold (state: State) (record: SubjectHistory.StoredRecord<LifecycleFact>) : State =
        match record.Body with
        | TurnStarted(turnId, startedAt) -> { Live = Some(turnId, startedAt) }
        | TurnEnded(turnId, _, _) ->
            match state.Live with
            | Some(live, _) when live = turnId -> { Live = None }
            | _ -> state

    /// The live turn, if any — the durable `activeRuns` answer for this session.
    let liveTurn (state: State) : Turn.TurnId option =
        state.Live |> Option.map fst

    // ---- Durable timer identities (derived from the turn, never random) ----
    // Two well-known timers per turn let `onWake` recompute each deadline from the
    // fold: max-duration from `startedAt`; idle from the last durable activity the
    // session log records (turn start — precise per-chunk re-arm on claim is the
    // kernel wake path, I3/C1, cited not depended-on here).

    let private idleTimerId (Turn.TurnId turn) : TimerId = TimerId("idle:" + turn)
    let private maxTimerId (Turn.TurnId turn) : TimerId = TimerId("max:" + turn)

    let private disarmsFor (turnId: Turn.TurnId) : TimerId list =
        [ idleTimerId turnId; maxTimerId turnId ]

    let private armsFor (timeouts: Timeouts) (turnId: Turn.TurnId) (startedAt: Timestamp) : (TimerId * Timestamp) list =
        [ match timeouts.Idle with
          | Some idle -> yield (idleTimerId turnId, startedAt + idle)
          | None -> ()
          match timeouts.MaxDuration with
          | Some maxDuration -> yield (maxTimerId turnId, startedAt + maxDuration)
          | None -> () ]

    /// The decision as data (not the effect): seal the live turn, append session
    /// facts, (re)arm / disarm durable timers. `Arm` lowers to the kernel
    /// `Intent.SetTimer`; `Disarm` is **advisory** — the kernel `Intent` has no
    /// cancel-timer, so a stale/late fire is already `noop`-guarded by the pure core
    /// (`onWake` seals only when `now` passes the deadline).
    type Outcome =
        { Seal: (Turn.TurnId * TurnTerminal) option
          Append: LifecycleFact list
          Arm: (TimerId * Timestamp) list
          Disarm: TimerId list }

    let noop: Outcome =
        { Seal = None
          Append = []
          Arm = []
          Disarm = [] }

    let private sealWith (turnId: Turn.TurnId) (cause: EndCause) (now: Timestamp) : Outcome =
        { Seal = Some(turnId, terminalOf cause)
          Append = [ TurnEnded(turnId, cause, now) ]
          Arm = []
          Disarm = disarmsFor turnId }

    /// Pure: state + admitted command + `now` -> outcome. `Cancel` of the live turn
    /// seals it `Cancelled`; `Cancel` of a non-live / already-ended turn is `noop`
    /// (idempotent).
    let onCommand (_timeouts: Timeouts) (state: State) (now: Timestamp) (sent: Sent) : Outcome =
        match sent.Command with
        | Cancel turnId ->
            match state.Live with
            | Some(live, _) when live = turnId -> sealWith turnId EndCause.Cancelled now
            | _ -> noop

    /// Pure: state + `now` + kernel wake -> outcome. An idle/max timer whose deadline
    /// `now` has passed seals the live turn; a not-yet-due / stale `TimerFired`, a
    /// `MailboxReady` with nothing to admit, or a `ChildTerminal` is `noop`.
    let onWake (timeouts: Timeouts) (state: State) (now: Timestamp) (wake: WakeReason) : Outcome =
        match wake with
        | WakeReason.MailboxReady -> noop
        | WakeReason.ChildTerminal _ -> noop
        | WakeReason.TimerFired(timerId, _) ->
            match state.Live with
            | None -> noop
            | Some(turnId, startedAt) ->
                let fireIf (cause: EndCause) (bound: int64 option) (dueAt: Timestamp) =
                    match bound with
                    | Some _ when now >= dueAt -> sealWith turnId cause now
                    | _ -> noop

                if timerId = maxTimerId turnId then
                    fireIf EndCause.MaxDurationTimeout timeouts.MaxDuration (startedAt + defaultArg timeouts.MaxDuration 0L)
                elif timerId = idleTimerId turnId then
                    fireIf EndCause.IdleTimeout timeouts.Idle (startedAt + defaultArg timeouts.Idle 0L)
                else
                    noop

    // ---- Codecs (private; JSON, string-encoded int64 for Fable safety) -----

    let private causeTag (cause: EndCause) : string =
        match cause with
        | Done -> "done"
        | Failed _ -> "failed"
        | Cancelled -> "cancelled"
        | IdleTimeout -> "idle"
        | MaxDurationTimeout -> "max"

    let private causeReason (cause: EndCause) : string =
        match cause with
        | Failed reason -> reason
        | _ -> ""

    let private decodeCause (tag: string) (reason: string) : Result<EndCause, string> =
        match tag with
        | "done" -> Ok Done
        | "failed" -> Ok(Failed reason)
        | "cancelled" -> Ok Cancelled
        | "idle" -> Ok IdleTimeout
        | "max" -> Ok MaxDurationTimeout
        | other -> Error(sprintf "unknown end cause '%s'" other)

    let private lifecycleCodec: SubjectHistory.Codec<LifecycleFact> =
        { Encode =
            fun fact ->
                match fact with
                | TurnStarted(Turn.TurnId turn, startedAt) ->
                    JsJson.stringify
                        {| kind = "started"
                           turn = turn
                           at = string startedAt |}
                | TurnEnded(Turn.TurnId turn, cause, endedAt) ->
                    JsJson.stringify
                        {| kind = "ended"
                           turn = turn
                           cause = causeTag cause
                           reason = causeReason cause
                           at = string endedAt |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body

                    match JsJson.stringProp "kind" parsed with
                    | "started" ->
                        Ok(
                            TurnStarted(
                                Turn.TurnId(JsJson.stringProp "turn" parsed),
                                System.Int64.Parse(JsJson.stringProp "at" parsed)
                            )
                        )
                    | "ended" ->
                        decodeCause (JsJson.stringProp "cause" parsed) (JsJson.stringProp "reason" parsed)
                        |> Result.map (fun cause ->
                            TurnEnded(
                                Turn.TurnId(JsJson.stringProp "turn" parsed),
                                cause,
                                System.Int64.Parse(JsJson.stringProp "at" parsed)
                            ))
                    | other -> Error(sprintf "unknown lifecycle fact kind '%s'" other)
                with error ->
                    Error error.Message }

    let private inboxCodec: SubjectHistory.Codec<Sent> =
        { Encode =
            fun sent ->
                match sent.Command with
                | Cancel(Turn.TurnId turn) ->
                    JsJson.stringify
                        {| cmd = "cancel"
                           turn = turn
                           source = sent.Source
                           seq = string sent.SourceSeq |}
          Decode =
            fun body ->
                try
                    let parsed = JsJson.parse<obj> body

                    match JsJson.stringProp "cmd" parsed with
                    | "cancel" ->
                        Ok
                            { Source = JsJson.stringProp "source" parsed
                              SourceSeq = System.Int64.Parse(JsJson.stringProp "seq" parsed)
                              Command = Cancel(Turn.TurnId(JsJson.stringProp "turn" parsed)) }
                    | other -> Error(sprintf "unknown inbox command '%s'" other)
                with error ->
                    Error error.Message }

    // ---- Durable read helpers (authority-free tailing reads) --------------

    /// Drain every domain record on a subject (open-append or FencedOwner),
    /// filtering out command/fence records. Paginated and bounded (`WaitSecs = 0`
    /// drains to the current tail rather than tailing indefinitely). An empty /
    /// not-yet-created stream reads as no records.
    let private readDomain
        (basin: S2.Basin)
        (subject: SubjectHistory.SubjectId)
        (codec: SubjectHistory.Codec<'r>)
        : Async<'r list> =
        async {
            let name = subjectName subject
            do! S2.ensureStream name basin
            let stream = basin |> S2.stream name

            let rec loop (from: int64) (acc: 'r list) =
                async {
                    let! batch =
                        async {
                            try
                                return!
                                    stream
                                    |> S2.readWith
                                        { S2.ReadOptions.empty with
                                            Start = Some(S2.FromSeqNum from)
                                            WaitSecs = Some 0
                                            IgnoreCommandRecords = true }
                            with error ->
                                match S2Errors.classify error with
                                | S2Errors.RangeNotSatisfiable _ -> return []
                                | _ -> return raise error
                        }

                    match batch with
                    | [] -> return acc
                    | records ->
                        let decoded =
                            records
                            |> List.map (fun (record: S2.ReadRecord) ->
                                match codec.Decode record.Body with
                                | Ok value -> value
                                | Error message -> failwithf "decode failed at seq %d: %s" record.SeqNum message)

                        let last = (List.last records).SeqNum
                        return! loop (last + 1L) (acc @ decoded)
                }

            return! loop 0L []
        }

    /// Fold the session log to the durable `activeRuns` state.
    let private foldSessionLog (basin: S2.Basin) (session: Turn.SessionId) : Async<State> =
        async {
            let! facts = readDomain basin (logSubject session) lifecycleCodec

            return
                facts
                |> List.fold
                    (fun state fact ->
                        // The Seq is not load-bearing for this fold (activeRuns is
                        // last-writer per turn id); a placeholder keeps it pure.
                        let stored: SubjectHistory.StoredRecord<LifecycleFact> =
                            { Seq = SubjectHistory.Seq 0L; Body = fact }

                        fold state stored)
                    initial
        }

    let private endedState (state: State) (turnId: Turn.TurnId) : State =
        let stored: SubjectHistory.StoredRecord<LifecycleFact> =
            { Seq = SubjectHistory.Seq 0L
              Body = TurnEnded(turnId, EndCause.Cancelled, 0L) }

        fold state stored

    /// First occurrence per `(Source, SourceSeq)` — provenance dedup, so a resend
    /// folds once.
    let private dedupeBySource (sents: Sent list) : Sent list =
        sents
        |> List.fold
            (fun (seen: Set<string>, acc) sent ->
                let key = sent.Source + " " + string sent.SourceSeq

                if Set.contains key seen then
                    (seen, acc)
                else
                    (Set.add key seen, sent :: acc))
            (Set.empty, [])
        |> snd
        |> List.rev

    let private causeOf (facts: LifecycleFact list) : EndCause =
        facts
        |> List.tryPick (function
            | TurnEnded(_, cause, _) -> Some cause
            | _ -> None)
        |> Option.defaultValue EndCause.Done

    // ---- Shell errors + handles -------------------------------------------

    [<RequireQualifiedAccess>]
    type StartError =
        /// Single-writer policy: a different live turn holds the session.
        | AlreadyLive of Turn.TurnId
        | Claim of Authority.ClaimError
        | Failed of S2Errors.S2Failure

    /// A started turn: the I1 turn `Producer` bound under the session claim, plus
    /// the session `Holder` and folded `State`. Only this holder appends output.
    type LiveTurn =
        private
            { Basin: S2.Basin
              Timeouts: Timeouts
              Session: Turn.SessionId
              Turn: Turn.TurnId
              SessionHolder: Authority.Holder<LifecycleFact>
              Producer: DurableLog.Producer<TurnChunk, TurnTerminal>
              mutable State: State }

    [<RequireQualifiedAccess>]
    type CancelError = Failed of S2Errors.S2Failure

    [<RequireQualifiedAccess>]
    type Progress =
        | Idle
        | Advanced
        | Ended of EndCause
        | Deposed

    [<RequireQualifiedAccess>]
    type DriveError =
        | Mailbox of S2Errors.S2Failure
        | Failed of S2Errors.S2Failure

    // ---- Shell: start / append / complete / cancel / drive ----------------

    let private mapCommitToStart (error: Authority.CommitError) : StartError =
        match error with
        // The fence loser of a concurrent start: its `TurnStarted` fails to commit.
        | Authority.CommitError.Deposed _ ->
            StartError.Failed(S2Errors.Other "session start lost the fence to a concurrent start")
        | Authority.CommitError.Sealed -> StartError.Failed(S2Errors.Other "session log unexpectedly sealed")
        | Authority.CommitError.Failed failure -> StartError.Failed failure

    /// Start a turn on `session` as `holderId`: `Authority.claim` the session log,
    /// then the SINGLE-WRITER policy over the fold —
    ///   • no live turn        -> record `TurnStarted`, `DurableLog.create` the turn
    ///                            stream under `holderId`, arm idle/max timers.
    ///   • live turn = `turnId`-> re-attach (create re-claims / deposes a stale
    ///                            producer): recovery, never a fork.
    ///   • live turn ≠ `turnId`-> reject `AlreadyLive`.
    /// A deposed racer's `TurnStarted` also fails under the session fence, so two
    /// concurrent starts yield exactly one live turn.
    let start
        (basin: S2.Basin)
        (timeouts: Timeouts)
        (session: Turn.SessionId)
        (turnId: Turn.TurnId)
        (holderId: Authority.HolderId)
        (now: Timestamp)
        : Async<Result<LiveTurn, StartError>> =
        async {
            match! Authority.claim basin lifecycleCodec (logSubject session) holderId with
            | Error claimError -> return Error(StartError.Claim claimError)
            | Ok sessionHolder ->
                let! state = foldSessionLog basin session

                match liveTurn state with
                | Some live when live <> turnId -> return Error(StartError.AlreadyLive live)
                | existing ->
                    // Fresh start records `TurnStarted`; a same-`turnId` re-attach does
                    // not (the turn already started) — recovery, never a fork.
                    let! startedResult =
                        async {
                            match existing with
                            | Some _ -> return Ok()
                            | None ->
                                match! Authority.commit sessionHolder [ TurnStarted(turnId, now) ] with
                                | Ok _ -> return Ok()
                                | Error commitError -> return Error(mapCommitToStart commitError)
                        }

                    match startedResult with
                    | Error startError -> return Error startError
                    | Ok() ->
                        // `DurableLog.create` re-claims the turn stream under `holderId`
                        // (deposing any stale producer). The armed timers (`armsFor`) lower
                        // to kernel `Intent.SetTimer` via the I3 wake path (cited, not wired
                        // here); the deadlines survive restart because they are re-derived
                        // from the fold on the next claim.
                        let _armed = armsFor timeouts turnId now

                        match! DurableLog.create basin Turn.codec (Turn.address session turnId) holderId with
                        | Error DurableLog.CreateError.Sealed ->
                            return Error(StartError.Failed(S2Errors.Other "turn stream already sealed"))
                        | Error(DurableLog.CreateError.Failed failure) -> return Error(StartError.Failed failure)
                        | Ok producer ->
                            let! refreshed = foldSessionLog basin session

                            return
                                Ok
                                    { Basin = basin
                                      Timeouts = timeouts
                                      Session = session
                                      Turn = turnId
                                      SessionHolder = sessionHolder
                                      Producer = producer
                                      State = refreshed }
        }

    /// Append one turn chunk under the turn fence (a fresh chunk re-arms the idle
    /// timer on the next drive/claim). A live *deposed* producer fails `Deposed` —
    /// it computes but cannot commit.
    let append (live: LiveTurn) (chunk: TurnChunk) : Async<Result<unit, DurableLog.AppendError>> =
        DurableLog.append live.Producer chunk

    /// Seal the turn normally (`TurnTerminal.Completed` + `TurnEnded Done`), disarm
    /// timers. First-valid-terminal-wins (I1).
    let complete (live: LiveTurn) (now: Timestamp) : Async<Result<unit, DurableLog.AppendError>> =
        async {
            match! DurableLog.seal live.Producer (terminalOf EndCause.Done) with
            | Error error -> return Error error
            | Ok() ->
                match! Authority.commit live.SessionHolder [ TurnEnded(live.Turn, EndCause.Done, now) ] with
                | Ok _ ->
                    live.State <- endedState live.State live.Turn
                    return Ok()
                | Error(Authority.CommitError.Deposed _) -> return Error DurableLog.AppendError.Deposed
                | Error Authority.CommitError.Sealed -> return Error DurableLog.AppendError.Sealed
                | Error(Authority.CommitError.Failed failure) -> return Error(DurableLog.AppendError.Failed failure)
        }

    /// Durable cancel — a MAILBOX SEND (open-append to `inboxSubject`), NOT a control
    /// channel and NOT authority: any process may call it. Idempotent by
    /// `(source, sourceSeq)`; the holder observes it on its next `drive` and seals
    /// the turn `Cancelled`. Returns once the send is durable.
    let cancel
        (basin: S2.Basin)
        (session: Turn.SessionId)
        (turnId: Turn.TurnId)
        (source: string)
        (sourceSeq: int64)
        : Async<Result<unit, CancelError>> =
        async {
            let subject = inboxSubject session

            try
                do! S2.ensureStream (subjectName subject) basin

                let sent =
                    { Source = source
                      SourceSeq = sourceSeq
                      Command = Cancel turnId }

                let! _ = SubjectHistory.append basin inboxCodec subject [ sent ]
                return Ok()
            with error ->
                return Error(CancelError.Failed(S2Errors.classify error))
        }

    /// Apply a pure `Outcome` under the session fence: seal the turn stream, then
    /// record the L2 cause on the session log. Sealing under a rotated fence
    /// surfaces `Deposed`. Timer `Arm`/`Disarm` intents lower to the kernel wake
    /// path (I3/C1) — computed by the pure core, dispatched by the kernel, not wired
    /// in B3.
    let private applyOutcome (live: LiveTurn) (outcome: Outcome) : Async<Result<Progress, DriveError>> =
        async {
            match outcome.Seal with
            | None ->
                if List.isEmpty outcome.Append then
                    return Ok Progress.Idle
                else
                    match! Authority.commit live.SessionHolder outcome.Append with
                    | Ok _ -> return Ok Progress.Advanced
                    | Error(Authority.CommitError.Deposed _) -> return Ok Progress.Deposed
                    | Error Authority.CommitError.Sealed -> return Ok Progress.Idle
                    | Error(Authority.CommitError.Failed failure) -> return Error(DriveError.Failed failure)
            | Some(turnId, terminal) ->
                match! DurableLog.seal live.Producer terminal with
                | Error DurableLog.AppendError.Deposed -> return Ok Progress.Deposed
                | Error DurableLog.AppendError.Sealed ->
                    // The turn is already terminal — idempotent; nothing new to seal.
                    return Ok Progress.Idle
                | Error(DurableLog.AppendError.Failed failure) -> return Error(DriveError.Failed failure)
                | Ok() ->
                    let ended = causeOf outcome.Append

                    match! Authority.commit live.SessionHolder outcome.Append with
                    | Ok _ ->
                        live.State <- endedState live.State turnId
                        return Ok(Progress.Ended ended)
                    | Error(Authority.CommitError.Deposed _) -> return Ok Progress.Deposed
                    // The turn stream sealed but the session log could not record the
                    // cause — the turn is durably terminal regardless.
                    | Error Authority.CommitError.Sealed -> return Ok(Progress.Ended ended)
                    | Error(Authority.CommitError.Failed failure) -> return Error(DriveError.Failed failure)
        }

    /// Admit the inbox (provenance-deduped): apply `onCommand` to each fresh command
    /// in order, threading the folded state so the first `Cancel` of the live turn
    /// seals it and any resend / stale cancel folds to `noop`.
    let private admitInbox (live: LiveTurn) (state: State) (now: Timestamp) (sents: Sent list) : Outcome =
        dedupeBySource sents
        |> List.fold
            (fun (st, chosen: Outcome) sent ->
                let outcome = onCommand live.Timeouts st now sent

                let chosen' =
                    if chosen.Seal.IsNone && outcome.Seal.IsSome then
                        outcome
                    else
                        chosen

                let st' =
                    match outcome.Seal with
                    | Some(sealedTurn, _) -> endedState st sealedTurn
                    | None -> st

                (st', chosen'))
            (state, noop)
        |> snd

    /// One holder drive tick: re-fold the session log, admit the inbox
    /// (provenance-deduped), and apply the pure `onCommand`/`onWake` decisions (seal
    /// the turn, append facts) under the session fence. `wake` is the kernel
    /// `WakeReason` (`MailboxReady` admits the inbox; `TimerFired` re-checks
    /// deadlines; `ChildTerminal` is `noop`), so C1's router drives a session with no
    /// shim. Sealing under a rotated fence surfaces `Deposed`.
    let drive (live: LiveTurn) (wake: WakeReason) (now: Timestamp) : Async<Result<Progress, DriveError>> =
        async {
            try
                let! state = foldSessionLog live.Basin live.Session
                live.State <- state

                match wake with
                | WakeReason.ChildTerminal _ -> return Ok Progress.Idle
                | WakeReason.TimerFired _ -> return! applyOutcome live (onWake live.Timeouts state now wake)
                | WakeReason.MailboxReady ->
                    let! sentsResult =
                        async {
                            try
                                let! sents = readDomain live.Basin (inboxSubject live.Session) inboxCodec
                                return Ok sents
                            with error ->
                                return Error(S2Errors.classify error)
                        }

                    match sentsResult with
                    | Error failure -> return Error(DriveError.Mailbox failure)
                    | Ok sents -> return! applyOutcome live (admitInbox live state now sents)
            with error ->
                return Error(DriveError.Failed(S2Errors.classify error))
        }
