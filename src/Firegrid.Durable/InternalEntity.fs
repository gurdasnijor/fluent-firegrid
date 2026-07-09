/// ═══════════════════════════════════════════════════════════════════════
/// Firegrid.Durable — entity (virtual object) runtime (G2 green-making).
///
/// Extends the minimal entity runtime G1 built in `Internal.fs` (which this
/// file REUSES: `EntityRun.journalName`/`inboxName`/`readAllRecords` and the
/// journal layout the golden-wire fixture pins) into the full virtual-object
/// contract:
///
///   EXCLUSIVE  at most one Decide per key at any moment across all hosts —
///              durable FIFO inbox admission (provenance-deduped) + journal
///              fence deposal at commit. The critical ordering fix over the
///              G1 minimal drive: FENCE FIRST, THEN READ. A holder folds the
///              journal only after rotating the fence, so its snapshot is
///              complete (no earlier holder can commit past the new fence)
///              and its dedupe set cannot miss a racing commit. G1's
///              fold-then-fence ordering double-applied under two hosts.
///   ZOMBIE     a deposed writer computes but cannot commit: every reply +
///              event batch is ONE fenced append; a stale token surfaces
///              `FencingTokenMismatch` and the pass abandons. Same technique
///              as the kernel's owned-stream commits and `Authority`'s
///              epoch fences (`store.object-live-fencing` lineage) — the
///              fence token here is per-claim entropy rather than an
///              epoch/holder encoding because the entity journal's records
///              are header-tagged (`t=e` events / `t=m` markers) with
///              fixture-pinned raw bodies, which `Authority.commit`
///              (Codec-encoded, header-less text records) cannot produce.
///   SHARED     graded state reads fold the same journal and never touch the
///              fence or the inbox — they cannot block the writer. Latest is
///              a check-tail-barriered fold (the `StateReads`/`StateView`
///              semantics); the machinery is not composed directly because
///              `StateView`'s `StoredRecord` drops record headers and its
///              codec decodes every record, while the entity journal
///              interleaves marker and event records distinguished only by
///              header.
///
/// Delayed delivery (`SendAfter`): command envelopes carry a deliver-at
/// timestamp (`At`, ms epoch; 0 = immediate). The admission pass processes
/// only due envelopes and never advances the exclusive inbox cursor past the
/// first not-yet-due one (the barrier); because dedupe is by (Src, Seq)
/// provenance, re-reading records beyond the barrier folds once.
///
/// Nothing here is public surface except `DurableReservedSegment` (typed
/// admission error, architect ruling on PR #118).
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable

/// Raised at Start/Send admission when a user-chosen identity (workflow
/// instance id, entity key) contains a reserved path segment. K1 reserved
/// `…/gen/<n>` (generation rollover) and `…/child/<opId>` (child workflows)
/// as kernel identity conventions; admitting user ids that embed them could
/// alias another instance's journal. (Architect ruling on PR #118.)
exception DurableReservedSegment of id: string * segment: string

namespace Firegrid.Durable.Internal

open Fable.Core
open Firegrid.Log

// ── Reserved-segment admission validation ──────────────────────────────────

[<RequireQualifiedAccess>]
module internal Reserved =
    let private reservedSegments = [ "gen"; "child" ]

    /// Reject user-chosen identities that embed a reserved identity segment.
    /// Enforced at admission: workflow `Start` (instance ids) and entity
    /// `Call`/`Send`/`SendAfter` (keys).
    let check (id: string) : unit =
        for segment in id.Split('/') do
            if List.contains segment reservedSegments then
                raise (Firegrid.Durable.DurableReservedSegment(id, segment))

// ── Entity wire types ───────────────────────────────────────────────────────
//
// Inbox command envelope and journal reply marker. The marker commits in the
// SAME fenced append as the command's events (reply + events atomic under
// the key's fence); (Src, Seq) is the provenance dedupe key; `Cur` is the
// exclusive inbox cursor after this command (clamped at the delayed-command
// barrier, see `EntityHost.drive`).

type internal EntityCmd =
    { Src: string
      Seq: float
      At: float // deliver-at (ms since epoch); 0 = immediate
      Cmd: string }

type internal EntityMark =
    { Src: string
      Seq: float
      Cur: float
      Reply: string }

[<RequireQualifiedAccess>]
module internal EntityInterop =
    [<Emit("Date.now()")>]
    let nowMs () : float = jsNative

    [<Emit("Math.floor(Math.random() * $0)")>]
    let randomBelow (_bound: int) : int = jsNative

// ── The entity host: admission, fenced commit, graded reads ───────────────

[<RequireQualifiedAccess>]
module internal EntityHost =
    let private cmdTy = typeof<EntityCmd>
    let private markTy = typeof<EntityMark>

    let private encodeCmd (cmd: EntityCmd) : string = Codec.encodeWith cmdTy (box cmd)
    let private decodeCmd (body: string) : EntityCmd = Codec.decodeWith cmdTy body |> unbox
    let private encodeMark (mark: EntityMark) : string = Codec.encodeWith markTy (box mark)

    let private hasHeader (key: string) (value: string) (record: S2.ReadRecord) =
        record.Headers |> List.exists (fun (hk, hv) -> hk = key && hv = value)

    let private marksOf (records: S2.ReadRecord list) : EntityMark list =
        records
        |> List.filter (hasHeader "t" "m")
        |> List.map (fun record -> Codec.decodeWith markTy record.Body |> unbox<EntityMark>)

    let private provenanceOf (marks: EntityMark list) =
        marks |> List.map (fun mark -> mark.Src, mark.Seq) |> Set.ofList

    let private cursorOf (marks: EntityMark list) =
        marks |> List.fold (fun acc mark -> max acc (int64 mark.Cur)) 0L

    let private foldEvents (spec: EntityRuntimeSpec) (records: S2.ReadRecord list) : obj =
        records
        |> List.filter (hasHeader "t" "e")
        |> List.fold (fun state record -> spec.Evolve state record.Body) spec.Initial

    /// Journal read tolerating the substrate's create-visibility window:
    /// under concurrent `ensure`s of one stream (racing reader/writer/worker,
    /// exactly the shared-read law's shape) s2-lite surfaces a transient
    /// not-found even after an ensure resolved. Bounded: re-ensure + retry,
    /// then surface the error.
    let private readJournalRecords (basin: S2.Basin) (streamName: string) : Async<S2.ReadRecord list> =
        let stream = basin |> S2.stream streamName

        let rec attempt (remaining: int) =
            async {
                let! outcome = Async.Catch(EntityRun.readAllRecords stream)

                match outcome with
                | Choice1Of2 records -> return records
                | Choice2Of2 _ when remaining > 0 ->
                    do! basin |> S2.ensureStream streamName
                    do! Interop.sleepUnref 60
                    return! attempt (remaining - 1)
                | Choice2Of2 error -> return raise error
            }

        attempt 5

    /// Unfenced append with the same bounded ensure-retry (the visibility
    /// window also hits appends under concurrent submits). Only used where a
    /// duplicate append is absorbed downstream: command envelopes dedupe by
    /// (Src, Seq) provenance; a repeated fence rotation to the same token is
    /// a no-op takeover by the same claim.
    let private appendWithRetry (basin: S2.Basin) (streamName: string) (records: S2.Record list) : Async<unit> =
        let stream = basin |> S2.stream streamName

        let rec attempt (remaining: int) =
            async {
                let! outcome = Async.Catch(stream |> S2.append records)

                match outcome with
                | Choice1Of2 _ -> return ()
                | Choice2Of2 _ when remaining > 0 ->
                    do! basin |> S2.ensureStream streamName
                    do! Interop.sleepUnref 60
                    return! attempt (remaining - 1)
                | Choice2Of2 error -> return raise error
            }

        attempt 5

    /// Inbox records from the exclusive cursor onward (one batch; the next
    /// pass picks up anything beyond it — dedupe makes re-reads safe).
    let private readInboxFrom (inbox: S2.Stream) (cursor: int64) : Async<S2.ReadRecord list> =
        async {
            try
                let! tail = inbox |> S2.checkTail

                if cursor >= tail.SeqNum then
                    return []
                else
                    return!
                        inbox
                        |> S2.readWith
                            { S2.ReadOptions.empty with
                                Start = Some(S2.FromSeqNum cursor)
                                Clamp = true }
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return []
                | _ -> return raise error
        }

    // ── Client side: submit + reply pickup ─────────────────────────────────

    /// Append a command envelope to the key's durable FIFO inbox. Any
    /// process may send — no locks, no ownership needed.
    let submit
        (basin: S2.Basin)
        (entityName: string)
        (key: string)
        (source: string)
        (seq: float)
        (at: float)
        (encodedCmd: string)
        : Async<unit> =
        async {
            do! basin |> S2.ensureStream (EntityRun.journalName entityName key)
            do! basin |> S2.ensureStream (EntityRun.inboxName entityName key)

            let envelope = { Src = source; Seq = seq; At = at; Cmd = encodedCmd }

            do!
                appendWithRetry
                    basin
                    (EntityRun.inboxName entityName key)
                    [ S2.Record.text (encodeCmd envelope) ]
        }

    /// Wait for the reply marker carrying this command's provenance.
    let awaitReply (basin: S2.Basin) (entityName: string) (key: string) (source: string) (seq: float) : Async<string> =
        let journalStream = EntityRun.journalName entityName key

        let rec wait () =
            async {
                let! records = readJournalRecords basin journalStream

                let reply =
                    marksOf records
                    |> List.tryPick (fun mark ->
                        if mark.Src = source && mark.Seq = seq then
                            Some mark.Reply
                        else
                            None)

                match reply with
                | Some body -> return body
                | None ->
                    do! Interop.sleepUnref 80
                    return! wait ()
            }

        wait ()

    // ── SHARED reads: graded, fence-free, writer-non-blocking ──────────────

    /// Fold the key's journal at the current committed tail. `readAllRecords`
    /// is check-tail-barriered and paginates through that tail, so this is
    /// linearizable at the checked tail (serves `Latest`; `Eventual` accepts
    /// anything up to this, and is served with the same fold — a stronger
    /// answer than promised is within grade).
    let readState (spec: EntityRuntimeSpec) (basin: S2.Basin) (key: string) : Async<obj> =
        async {
            do! basin |> S2.ensureStream (EntityRun.journalName spec.Name key)
            let! records = readJournalRecords basin (EntityRun.journalName spec.Name key)
            return foldEvents spec records
        }

    /// Read-your-writes: wait until the journal tail covers `version`, then
    /// fold (for a caller holding an append ack).
    let readStateThrough (spec: EntityRuntimeSpec) (basin: S2.Basin) (key: string) (version: float) : Async<obj> =
        async {
            do! basin |> S2.ensureStream (EntityRun.journalName spec.Name key)
            let journal = basin |> S2.stream (EntityRun.journalName spec.Name key)

            let rec wait () =
                async {
                    let! tail = journal |> S2.checkTail

                    if float tail.SeqNum >= version then
                        let! records = readJournalRecords basin (EntityRun.journalName spec.Name key)
                        return foldEvents spec records
                    else
                        do! Interop.sleepUnref 80
                        return! wait ()
                }

            return! wait ()
        }

    // ── EXCLUSIVE drive: one admission pass for one key ────────────────────
    //
    // Protocol (per pass, per key):
    //   1. UNFENCED pre-check — is there any due, unprocessed command? If
    //      not, do nothing (an idle or all-delayed key never bloats its
    //      journal with fence rotations).
    //   2. CLAIM — rotate the journal fence to fresh entropy. Last fencer
    //      wins; every earlier holder's next fenced append is rejected.
    //   3. RE-READ — fold journal + read inbox AFTER the fence. The
    //      post-fence snapshot is complete: any commit that could ever
    //      succeed before ours is already visible (older tokens are dead),
    //      so the dedupe set and the state fold cannot miss anything.
    //   4. DECIDE + COMMIT — for each due, unprocessed envelope in FIFO
    //      order: run Decide, append events + reply marker as ONE fenced
    //      batch. `FencingTokenMismatch` ⇒ deposed ⇒ abandon the pass (the
    //      new holder re-reads and continues from the committed prefix).
    let drive (spec: EntityRuntimeSpec) (basin: S2.Basin) (key: string) : Async<bool> =
        async {
            let journal = basin |> S2.stream (EntityRun.journalName spec.Name key)
            let inbox = basin |> S2.stream (EntityRun.inboxName spec.Name key)

            // 1. Unfenced pre-check.
            let! preRecords = readJournalRecords basin (EntityRun.journalName spec.Name key)
            let preMarks = marksOf preRecords
            let preProcessed = provenanceOf preMarks
            let! prePending = readInboxFrom inbox (cursorOf preMarks)
            let preNow = EntityInterop.nowMs ()

            let hasDueWork =
                prePending
                |> List.exists (fun record ->
                    let cmd = decodeCmd record.Body
                    not (preProcessed.Contains(cmd.Src, cmd.Seq)) && not (cmd.At > preNow))

            if not hasDueWork then
                return false
            else
                // 2. Claim the key: rotate the journal fence.
                let fence = "l2e/" + Interop.entropy ()
                do! appendWithRetry basin (EntityRun.journalName spec.Name key) [ S2.Record.fence fence ]
                let fenced = S2.AppendOptions.none |> S2.AppendOptions.fencingToken fence

                // 3. Post-fence snapshot: complete by construction.
                let! records = EntityRun.readAllRecords journal
                let marks = marksOf records
                let mutable processed = provenanceOf marks
                let mutable state = foldEvents spec records
                let! pendingRecords = readInboxFrom inbox (cursorOf marks)

                let pending =
                    pendingRecords
                    |> List.map (fun record -> record.SeqNum, decodeCmd record.Body)

                let now = EntityInterop.nowMs ()

                // The exclusive cursor may not advance past the first
                // not-yet-due command (FIFO position preserved for delayed
                // delivery; (Src, Seq) dedupe absorbs the re-reads beyond it).
                let barrier =
                    pending
                    |> List.tryPick (fun (seqNum, cmd) ->
                        if cmd.At > now && not (processed.Contains(cmd.Src, cmd.Seq)) then
                            Some seqNum
                        else
                            None)

                // 4. Decide + fenced atomic commit, FIFO.
                let mutable deposed = false

                for seqNum, cmd in pending do
                    if
                        not deposed
                        && not (processed.Contains(cmd.Src, cmd.Seq))
                        && not (cmd.At > now)
                    then
                        let replyBody, eventBodies = spec.Decide key cmd.Cmd state

                        // Cursor after this command: past it, then past any
                        // already-processed records contiguously behind it,
                        // clamped at the barrier.
                        let mutable cursorAfter = seqNum + 1L

                        for laterSeq, laterCmd in pending do
                            if laterSeq = cursorAfter && processed.Contains(laterCmd.Src, laterCmd.Seq) then
                                cursorAfter <- cursorAfter + 1L

                        let cursorAfter =
                            match barrier with
                            | Some barrierSeq -> min cursorAfter barrierSeq
                            | None -> cursorAfter

                        let mark =
                            { Src = cmd.Src
                              Seq = cmd.Seq
                              Cur = float cursorAfter
                              Reply = replyBody }

                        let batch =
                            [ for eventBody in eventBodies -> S2.Record.textWith [ "t", "e" ] eventBody ]
                            @ [ S2.Record.textWith [ "t", "m" ] (encodeMark mark) ]

                        let! commit = journal |> S2.tryAppendWith fenced batch

                        match commit with
                        | Ok _ ->
                            state <- eventBodies |> List.fold spec.Evolve state
                            processed <- processed.Add(cmd.Src, cmd.Seq)
                        | Error(S2Errors.FencingTokenMismatch _) ->
                            // Deposed: a newer holder owns the key. Nothing
                            // from this pass leaks — the batch was atomic.
                            deposed <- true
                        | Error failure ->
                            deposed <- true
                            Interop.consoleError ("Firegrid.Durable entity commit failed: " + string failure)

                if deposed then
                    // Damp fence ping-pong between racing hosts.
                    do! Interop.sleepUnref (10 + EntityInterop.randomBelow 60)

                return true
        }
