namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log
open Firegrid.Foundation

/// MS-C3 — the tail-driven wake router: the actor whose mailbox IS its shard
/// stream (per `docs/canon/architecture/fluent/authority-and-actors.md`).
/// Leadership is the `Authority` **FencedOwner** regime (I5) applied to the
/// router's own cursor log — exactly one live router per shard; deposal is epoch
/// increment, never a private fence. The durable cursor is a fenced checkpoint
/// (the `Mailbox` `MailboxCheckpoint` lineage): a restart resumes from the
/// committed cursor, re-driving nothing already consumed.
///
/// A binding/combinator over `Authority` + `SubjectHistory` + `WakeShard`, not a
/// new drive loop. The pure decision core is `plan`; the shell (`tick`/`run`)
/// claims, tails with wait, dispatches via the injected `Drive`, and commits the
/// advanced cursor under the fence.
///
/// EffSharp-free: `Async` + `Result` + DU errors + `Codec` records + pull-cursor
/// reads.
[<RequireQualifiedAccess>]
module WakeRouter =

    /// The consumed prefix of this router's shard stream as an EXCLUSIVE upper
    /// bound (the house `Version` convention): `NextSeq` is the next sequence to
    /// consume — every `Seq < NextSeq` is already consumed (dispatched or skipped),
    /// so a fresh router resumes AT `NextSeq` and re-dispatches nothing.
    type Cursor = { Shard: WakeShard.ShardId; NextSeq: SubjectHistory.Seq }

    /// The drive dependency (sans-IO seam): "ensure subject S is claimed and
    /// ticked for reason R." The core decides WHICH subjects to dispatch and in
    /// what order; the shell performs the drive. Injected so the core stays
    /// pure/deterministic and C2's proofs can substitute a recording driver. The
    /// drive is idempotent by the target's own claim: driving an already-current
    /// subject is a no-op tick.
    type Drive = ActorAddress -> WakeReason -> Async<Result<unit, string>>

    [<RequireQualifiedAccess>]
    type RouterError =
        /// Leadership claim on the shard's cursor log failed.
        | ClaimFailed of Authority.ClaimError
        /// A newer epoch rotated the fence at cursor commit; step aside.
        | Deposed of by: Authority.Epoch
        | ReadFailed of S2Errors.S2Failure
        | DriveFailed of ActorAddress * error: string
        | CursorCommitFailed of S2Errors.S2Failure

    // ---- Durable cursor log (fenced own-log, I5 FencedOwner) --------------

    /// The cursor is committed as an `int64` (next-seq) record on the router's own
    /// authority log; decimal-string encoded (Fable-safe, no int64 JSON).
    let private cursorCodec: SubjectHistory.Codec<int64> =
        { Encode = fun value -> string value
          Decode =
            fun body ->
                try
                    Ok(System.Int64.Parse body)
                with _ ->
                    Error(sprintf "invalid cursor seq '%s'" body) }

    /// Derived (never random) authority/cursor log for a shard's router:
    /// `"{ns}/wake/{shardId}/router"` — distinct from the open-append shard
    /// mailbox `"{ns}/wake/{shardId}"`.
    let private routerSubject (config: WakeShard.ShardConfig) (WakeShard.ShardId id) : SubjectHistory.SubjectId =
        SubjectHistory.SubjectId(sprintf "%s/wake/%d/router" config.Namespace id)

    /// How many records near the cursor-log tail to scan when recovering the
    /// committed cursor. Cursor commits are frequent and monotonic and fence
    /// (command) records are rare, so the latest commit is always within this
    /// window — bounding recovery read cost independent of log length.
    [<Literal>]
    let private recoverWindow = 64L

    /// Claim shard leadership through `Authority.claim` (FencedOwner) — never a
    /// private fence.
    let private claimRouter
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (holderId: Authority.HolderId)
        : Async<Result<Authority.Holder<int64>, RouterError>> =
        async {
            let! claimed = Authority.claim basin cursorCodec (routerSubject config shard) holderId

            match claimed with
            | Ok holder -> return Ok holder
            | Error error -> return Error(RouterError.ClaimFailed error)
        }

    /// Recover the committed `NextSeq` from the router's own cursor log. Reads a
    /// bounded window at the tail (fence records ignored) and takes the max
    /// committed value — cursor commits are monotonic, so the latest is the
    /// answer; never checkpointed ⇒ `Seq 0`.
    let private recoverCursor
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        : Async<Result<Cursor, RouterError>> =
        async {
            let (SubjectHistory.SubjectId name) = routerSubject config shard
            let stream = basin |> S2.stream name

            try
                let! tail = stream |> S2.checkTail
                let tailSeq = tail.SeqNum

                if tailSeq <= 0L then
                    return Ok { Shard = shard; NextSeq = SubjectHistory.Seq 0L }
                else
                    let! records =
                        stream
                        |> S2.readWith
                            { S2.ReadOptions.empty with
                                Start = Some(S2.FromSeqNum(max 0L (tailSeq - recoverWindow)))
                                IgnoreCommandRecords = true }

                    let latest =
                        records
                        |> List.choose (fun (record: S2.ReadRecord) ->
                            match cursorCodec.Decode record.Body with
                            | Ok value -> Some value
                            | Error _ -> None)
                        |> fun values -> if List.isEmpty values then 0L else List.max values

                    return Ok { Shard = shard; NextSeq = SubjectHistory.Seq latest }
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return Ok { Shard = shard; NextSeq = SubjectHistory.Seq 0L }
                | failure -> return Error(RouterError.ReadFailed failure)
        }

    /// Claim the shard, ensure its mailbox exists, and recover the cursor — the
    /// shared preamble of `tick` and `run`.
    let private claimAndRecover
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (holderId: Authority.HolderId)
        : Async<Result<Authority.Holder<int64> * Cursor, RouterError>> =
        async {
            let! claimed = claimRouter basin config shard holderId

            match claimed with
            | Error error -> return Error error
            | Ok holder ->
                let (SubjectHistory.SubjectId shardName) = WakeShard.shardSubject config shard

                try
                    // Ensure the shard mailbox once (before the loop), not per cycle.
                    do! S2.ensureStream shardName basin
                    let! recovered = recoverCursor basin config shard

                    match recovered with
                    | Error error -> return Error error
                    | Ok cursor -> return Ok(holder, cursor)
                with error ->
                    return Error(RouterError.ReadFailed(S2Errors.classify error))
        }

    // ---- Pure decision core ----------------------------------------------

    /// Pure decision core (sans-IO): given the current cursor and the batch of
    /// records scanned off the shard tail (`None` = an undecodable pointer),
    /// produce the ordered dispatch list and the advanced cursor. Deterministic —
    /// no clock, no I/O. Dispatches only decodable records at `Seq >= NextSeq`
    /// (strict dedup); an undecodable record is *consumed and skipped* — the
    /// subject degrades to the sweep — so a single poison pointer cannot wedge the
    /// shard. The advanced cursor's `NextSeq` is the last SCANNED `Seq + 1`
    /// (exclusive upper bound over the whole consumed prefix, decodable or not).
    let plan
        (cursor: Cursor)
        (scanned: (SubjectHistory.Seq * WakeShard.WakeRecord option) list)
        : (ActorAddress * WakeReason) list * Cursor =
        let floor = SubjectHistory.seqNumber cursor.NextSeq

        let dispatch =
            scanned
            |> List.choose (fun (seq, record) ->
                match record with
                | Some wake when SubjectHistory.seqNumber seq >= floor -> Some(wake.Subject, wake.Reason)
                | _ -> None)

        let advanced =
            match scanned with
            | [] -> cursor.NextSeq
            | _ ->
                let lastSeq = scanned |> List.map (fst >> SubjectHistory.seqNumber) |> List.max
                SubjectHistory.Seq(max (lastSeq + 1L) floor)

        dispatch, { cursor with NextSeq = advanced }

    // ---- Tail read + dispatch --------------------------------------------

    /// Poll window for the tailing cursor (the `openCursorWithWait` idiom): `tick`
    /// blocks up to this long for the next wake, so appends need no external poll.
    [<Literal>]
    let private waitSecs = 1

    /// Bound on records drained per cycle; a backlog larger than this is caught up
    /// across successive cycles (cursor advances each time).
    [<Literal>]
    let private maxBatch = 256

    /// Read a bounded batch off the shard mailbox from `fromSeq`, blocking with
    /// wait for the first record (empty on a quiet shard). Reads raw records and
    /// decodes each so an undecodable pointer is carried through as `None` (skipped
    /// by `plan`) with its real seq — never a halting error. The read cursor is
    /// closed on both the normal and exception paths.
    let private drainShard
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (fromSeq: SubjectHistory.Seq)
        : Async<Result<(SubjectHistory.Seq * WakeShard.WakeRecord option) list, RouterError>> =
        async {
            let (SubjectHistory.SubjectId name) = WakeShard.shardSubject config shard

            try
                let! cursor =
                    basin
                    |> S2.stream name
                    |> S2.readCursor
                        { S2.ReadOptions.empty with
                            Start = Some(S2.FromSeqNum(SubjectHistory.seqNumber fromSeq))
                            WaitSecs = Some waitSecs }

                let mutable closing = false

                try
                    let mutable collected = []
                    let mutable count = 0
                    let mutable go = true

                    while go && count < maxBatch do
                        let! raw = S2.tryNext cursor

                        match raw with
                        | Some record ->
                            let decoded =
                                match WakeShard.codec.Decode record.Body with
                                | Ok wake -> Some wake
                                | Error _ -> None // poison pointer: consume + skip (sweep covers the subject)

                            collected <- (SubjectHistory.Seq record.SeqNum, decoded) :: collected
                            count <- count + 1
                        | None -> go <- false

                    let result = List.rev collected
                    closing <- true
                    do! S2.closeReadCursor cursor
                    return Ok result
                with error ->
                    if not closing then
                        do! S2.closeReadCursor cursor

                    return raise error
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return Ok []
                | failure -> return Error(RouterError.ReadFailed failure)
        }

    /// One dispatch/commit cycle over an already-claimed holder: drain the shard
    /// tail, `plan`, drive each fresh wake in order, then commit the advanced
    /// cursor under the fence. Drive happens BEFORE the commit, so a deposal at
    /// commit leaves only idempotent re-drives for the next holder.
    let private cycle
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (holder: Authority.Holder<int64>)
        (drive: Drive)
        (cursor: Cursor)
        : Async<Result<Cursor, RouterError>> =
        async {
            let! drained = drainShard basin config shard cursor.NextSeq

            match drained with
            | Error error -> return Error error
            | Ok scanned ->
                let dispatch, advanced = plan cursor scanned

                let rec driveAll pending =
                    async {
                        match pending with
                        | [] -> return Ok()
                        | (subject, reason) :: rest ->
                            let! result = drive subject reason

                            match result with
                            | Ok() -> return! driveAll rest
                            | Error message -> return Error(RouterError.DriveFailed(subject, message))
                    }

                let! driven = driveAll dispatch

                match driven with
                | Error error -> return Error error
                | Ok() ->
                    if SubjectHistory.seqNumber advanced.NextSeq = SubjectHistory.seqNumber cursor.NextSeq then
                        // Nothing consumed this cycle: cursor unchanged, no fenced write.
                        return Ok advanced
                    else
                        let! committed = Authority.commit holder [ SubjectHistory.seqNumber advanced.NextSeq ]

                        match committed with
                        | Ok _ -> return Ok advanced
                        | Error(Authority.CommitError.Deposed by) -> return Error(RouterError.Deposed by)
                        | Error Authority.CommitError.Sealed ->
                            return Error(RouterError.CursorCommitFailed(S2Errors.Other "router cursor log unexpectedly sealed"))
                        | Error(Authority.CommitError.Failed failure) -> return Error(RouterError.CursorCommitFailed failure)
        }

    // ---- Public shell -----------------------------------------------------

    /// One claimed tick: claim the shard (FencedOwner), recover the cursor, read
    /// the shard tail with wait, dispatch each fresh wake exactly once, then commit
    /// the advanced cursor under the fence. Returns `Deposed` if a newer epoch
    /// rotated the fence at commit — the deposed router computed but did not advance.
    let tick
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (holderId: Authority.HolderId)
        (drive: Drive)
        : Async<Result<Cursor, RouterError>> =
        async {
            let! prepared = claimAndRecover basin config shard holderId

            match prepared with
            | Error error -> return Error error
            | Ok(holder, cursor) -> return! cycle basin config shard holder drive cursor
        }

    /// Run the router loop until deposed or the async is cancelled: claim once,
    /// then cycle continuously, tailing the stream. Degraded mode: if no router
    /// runs a shard, the existing sweep still drives the work — wakes accelerate,
    /// sweeps guarantee.
    let run
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (holderId: Authority.HolderId)
        (drive: Drive)
        : Async<Result<unit, RouterError>> =
        async {
            let! prepared = claimAndRecover basin config shard holderId

            match prepared with
            | Error error -> return Error error
            | Ok(holder, initial) ->
                let rec loop cursor =
                    async {
                        let! stepped = cycle basin config shard holder drive cursor

                        match stepped with
                        | Ok next -> return! loop next
                        | Error error -> return Error error
                    }

                return! loop initial
        }
