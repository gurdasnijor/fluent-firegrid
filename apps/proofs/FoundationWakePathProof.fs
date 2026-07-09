namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Foundation
open Firegrid.Store
open Firegrid.Store.Foundation.Durable

/// C2 — the MS-C3 wake-path proof obligations, driven entirely through the public
/// C1 surface (`WakeShard` / `WakeRouter`) plus the C2 `TimerIndex`. No deep
/// imports, no proof-only branches in production code. The injected `Drive` seam
/// is the observability hook (a recording driver captures dispatches).
///
/// - `wake.tail-latency` — an appended wake reaches its claimed handler within a
///   recorded bound (measured from trace evidence).
/// - `wake.timer-exactly-once` — a due timer fires exactly once across a router
///   restart; a not-yet-due timer survives unfired; a poison (undecodable) record
///   is consumed and skipped (cursor passes it, later wakes still dispatch, a
///   restart does not re-wedge); a wake at the committed cursor survives a restart
///   undropped and a poison at that boundary does not wedge recovery.
///
/// The `wake.single-claim` obligation moved to the `foundation.fencing`
/// invariant family (FoundationFencingProof.fs) in Packet 0.3b.
module FoundationWakePathProof =

    // ---- shared helpers over the public surface --------------------------

    let private address segments : ActorAddress = { Segments = segments }

    /// A recording drive: captures each dispatch (subject, reason) and succeeds.
    let private recordingDrive (sink: ResizeArray<ActorAddress * WakeReason>) : WakeRouter.Drive =
        fun subject reason ->
            async {
                sink.Add(subject, reason)
                return Ok()
            }

    let private tickOk
        (basin: S2.Basin)
        (config: WakeShard.ShardConfig)
        (shard: WakeShard.ShardId)
        (holder: string)
        (drive: WakeRouter.Drive)
        : Async<WakeRouter.Cursor> =
        async {
            match! WakeRouter.tick basin config shard (Authority.HolderId holder) drive with
            | Ok cursor -> return cursor
            | Error _ -> return failwithf "wake-router: tick '%s' failed unexpectedly" holder
        }

    let private cursorSeq (cursor: WakeRouter.Cursor) : int64 = SubjectHistory.seqNumber cursor.NextSeq

    let private dispatchedSubjects (sink: ResizeArray<ActorAddress * WakeReason>) : ActorAddress list =
        sink |> Seq.map fst |> List.ofSeq

    /// Append a raw, undecodable record straight onto a shard's mailbox — models a
    /// newer/skewed external poster the router's codec cannot decode. Uses only the
    /// public `WakeShard.shardSubject` naming + the public S2 append API.
    let private postPoison (basin: S2.Basin) (config: WakeShard.ShardConfig) (shard: WakeShard.ShardId) : Async<unit> =
        async {
            let (SubjectHistory.SubjectId name) = WakeShard.shardSubject config shard
            let! _ = basin |> S2.stream name |> S2.append [ S2.Record.text "poison::not-a-wake-record" ]
            return ()
        }

    // ---- wake.tail-latency ------------------------------------------------

    type TailLatencyResult =
        { Dispatched: bool
          DispatchedReasonIsMailboxReady: bool
          LatencyMillis: float
          BoundMillis: float
          WithinBound: bool
          NonNegativeLatency: bool }

    /// Recorded latency bound (ms). The C1 router tails with a 1s
    /// `openCursorWithWait` poll window, so an appended wake reaches its handler
    /// within roughly that window; this bound (poll window + generous CI overhead)
    /// is the value the assertion pins.
    [<Literal>]
    let private tailLatencyBoundMillis = 3000.0

    let private tailLatencyWorkload ctx =
        ProofOperation.run
            ctx
            "wake.tail-latency"
            "wake-tail-latency"
            { ProofOperationOptions.empty with
                Key = Some "wake-tail-latency" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "wake-lat-" + suffix
                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let config: WakeShard.ShardConfig = { Namespace = "lat-" + suffix; Count = 1 }
                let subject = address [ "sessions"; "lat-" + suffix ]
                let shard = WakeShard.shardOf config subject

                let dispatched = ResizeArray<ActorAddress * WakeReason>()
                let mutable dispatchMillis = 0.0

                let drive: WakeRouter.Drive =
                    fun s r ->
                        async {
                            if dispatched.Count = 0 then
                                dispatchMillis <- Reports.nowMillis ()

                            dispatched.Add(s, r)
                            return Ok()
                        }

                // Append the wake, then run one claimed tick that tails it.
                let postMillis = Reports.nowMillis ()

                match! WakeShard.post basin config subject WakeReason.MailboxReady with
                | Error _ -> return failwith "wake-shard: post failed unexpectedly"
                | Ok() ->
                    let! _ = tickOk basin config shard "router-lat" drive
                    let latency = dispatchMillis - postMillis

                    let result =
                        { Dispatched = (dispatched.Count = 1)
                          DispatchedReasonIsMailboxReady =
                            (dispatched.Count = 1 && snd dispatched.[0] = WakeReason.MailboxReady)
                          LatencyMillis = latency
                          BoundMillis = tailLatencyBoundMillis
                          WithinBound = (latency >= 0.0 && latency <= tailLatencyBoundMillis)
                          NonNegativeLatency = (latency >= 0.0) }

                    do!
                        ctx.EmitSpan
                            "proof.wake.tail-latency.completed"
                            [ "proof.property", "wake.tail-latency"
                              "wake.latency_millis", string result.LatencyMillis
                              "wake.bound_millis", string result.BoundMillis
                              "wake.within_bound", string result.WithinBound ]

                    return result
            })

    let private tailLatencyProperty =
        property "wake.tail-latency" {
            s2Lite ""
            workload tailLatencyWorkload

            verify (fun v ->
                [ v.Expect.Workload "the appended wake reached its claimed handler exactly once" (fun r -> r.Dispatched)
                  v.Expect.Workload "the dispatched wake carried the posted MailboxReady reason" (fun r ->
                      r.DispatchedReasonIsMailboxReady)
                  v.Expect.Workload "the post→dispatch latency is non-negative" (fun r -> r.NonNegativeLatency)
                  v.Expect.Workload "the wake reached the handler within the recorded bound (<=3000ms)" (fun r ->
                      r.WithinBound)
                  v.Trace.SpanExists
                      "tail-latency completion span carries the measured latency"
                      "proof.wake.tail-latency.completed"
                      [ "proof.property", "wake.tail-latency" ]
                  v.Trace.Operation
                      "tail-latency operation recorded"
                      ({ TraceOperationMatch.named "wake.tail-latency" with
                          Status = Some "ok"
                          OutputContains = [ "WithinBound"; "LatencyMillis" ]
                          Count = Some 1 }) ])
        }

    // ---- wake.timer-exactly-once (+ poison + boundary) -------------------

    type TimerExactlyOnceResult =
        { DueTimerFiredExactlyOnce: bool
          NotYetDueSurvivesUnfired: bool
          BoundaryWakeSurvivesRestart: bool
          PoisonConsumedNotWedged: bool
          RestartAfterPoisonNoReDispatch: bool
          PoisonAtBoundaryRecoversNoWedge: bool
          CursorMonotonic: bool }

    let private timerExactlyOnceWorkload ctx =
        ProofOperation.run
            ctx
            "wake.timer-exactly-once"
            "wake-timer-exactly-once"
            { ProofOperationOptions.empty with
                Key = Some "wake-timer-exactly-once" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "wake-timer-" + suffix
                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                // Count = 1 → every subject maps to shard 0; one router handles all.
                let config: WakeShard.ShardConfig = { Namespace = "timer-" + suffix; Count = 1 }
                let s1 = address [ "sessions"; "s1" ]
                let s2subj = address [ "sessions"; "s2" ]
                let s3 = address [ "sessions"; "s3" ]
                let s4 = address [ "sessions"; "s4" ]
                let shard = WakeShard.shardOf config s1
                let t1 = TimerId "t1"
                let t2 = TimerId "t2"

                let armOk target timer dueAt =
                    async {
                        match! TimerIndex.arm basin config target timer dueAt with
                        | Ok() -> return ()
                        | Error _ -> return failwith "timer-index: arm failed unexpectedly"
                    }

                let loadOk () =
                    async {
                        match! TimerIndex.load basin config with
                        | Ok pending -> return pending
                        | Error _ -> return failwith "timer-index: load failed unexpectedly"
                    }

                let postOk target reason =
                    async {
                        match! WakeShard.post basin config target reason with
                        | Ok() -> return ()
                        | Error _ -> return failwith "wake-shard: post failed unexpectedly"
                    }

                let dispatched = ResizeArray<ActorAddress * WakeReason>()
                let drive = recordingDrive dispatched
                let countFor target = dispatched |> Seq.filter (fun (s, _) -> s = target) |> Seq.length

                // --- Phase 1: a due timer fires once; a not-yet-due one survives.
                do! armOk s1 t1 100L
                do! armOk s2subj t2 200L
                let! pendingBefore = loadOk ()
                let dueAt150 = TimerIndex.due 150L pendingBefore

                match! TimerIndex.fireDue basin config 150L with
                | Error _ -> return failwith "timer-index: fireDue failed unexpectedly"
                | Ok fired ->
                    // fireDue posts s1/t1's wake (due), not s2/t2 (dueAt > now).
                    let! c1 = tickOk basin config shard "router-1" drive // dispatches t1 → cursor 0→1
                    let t1AfterFirst = countFor s1
                    let! c2 = tickOk basin config shard "router-2" drive // restart: no re-dispatch
                    let t1AfterRestart = countFor s1
                    let! pendingAfter = loadOk ()

                    // --- Phase 2: a wake exactly at the committed cursor survives a restart.
                    do! postOk s3 WakeReason.MailboxReady // lands at seq == cursor (1)
                    let! c3 = tickOk basin config shard "router-3" drive // restart dispatches s3 → 1→2
                    let s3Dispatched = countFor s3

                    // --- Phase 3: a poison record mid-batch is consumed & skipped.
                    do! postPoison basin config shard // seq 2 (undecodable)
                    do! postOk s4 WakeReason.MailboxReady // seq 3 (valid)
                    let! c4 = tickOk basin config shard "router-4" drive // dispatch s4, skip poison → 2→4
                    let s4Dispatched = countFor s4
                    let! c5 = tickOk basin config shard "router-5" drive // restart: no re-dispatch, no wedge
                    let s4AfterRestart = countFor s4

                    // --- Phase 4: a poison at the committed-cursor boundary recovers without wedging.
                    do! postPoison basin config shard // seq 4 == cursor (4)
                    let! c6 = tickOk basin config shard "router-6" drive // recovery skips it → 4→5

                    let seqs = [ c1; c2; c3; c4; c5; c6 ] |> List.map cursorSeq

                    let result =
                        { DueTimerFiredExactlyOnce = (t1AfterFirst = 1 && t1AfterRestart = 1)
                          NotYetDueSurvivesUnfired =
                            (fired = [ (s1, t1) ]
                             && (dueAt150 |> List.map (fun (s, t, _) -> s, t)) = [ (s1, t1) ]
                             && (TimerIndex.pending pendingAfter |> List.map (fun (s, t, _) -> s, t)) = [ (s2subj, t2) ]
                             && countFor s2subj = 0)
                          BoundaryWakeSurvivesRestart = (s3Dispatched = 1 && cursorSeq c3 = 2L)
                          PoisonConsumedNotWedged = (s4Dispatched = 1 && cursorSeq c4 = 4L)
                          RestartAfterPoisonNoReDispatch = (s4AfterRestart = 1 && cursorSeq c5 = 4L)
                          PoisonAtBoundaryRecoversNoWedge = (cursorSeq c6 = 5L)
                          // cursor is monotonic non-decreasing across every claimed tick.
                          CursorMonotonic = (seqs = List.sort seqs && seqs = [ 1L; 1L; 2L; 4L; 4L; 5L ]) }

                    do!
                        ctx.EmitSpan
                            "proof.wake.timer-exactly-once.completed"
                            [ "proof.property", "wake.timer-exactly-once"
                              "wake.timer_fired_once", string result.DueTimerFiredExactlyOnce
                              "wake.poison_consumed", string result.PoisonConsumedNotWedged
                              "wake.boundary_survives", string result.BoundaryWakeSurvivesRestart ]

                    return result
            })

    let private timerExactlyOnceProperty =
        property "wake.timer-exactly-once" {
            s2Lite ""
            workload timerExactlyOnceWorkload

            verify (fun v ->
                [ v.Expect.Workload "a due timer fires exactly once across a router restart" (fun r ->
                      r.DueTimerFiredExactlyOnce)
                  v.Expect.Workload "a not-yet-due timer survives unfired (still pending, no wake posted)" (fun r ->
                      r.NotYetDueSurvivesUnfired)
                  v.Expect.Workload "a wake exactly at the committed cursor survives a restart undropped" (fun r ->
                      r.BoundaryWakeSurvivesRestart)
                  v.Expect.Workload "a poison record is consumed and skipped; the cursor passes it and the valid wake still dispatches" (fun r ->
                      r.PoisonConsumedNotWedged)
                  v.Expect.Workload "a restart after the poison re-dispatches nothing and does not re-wedge" (fun r ->
                      r.RestartAfterPoisonNoReDispatch)
                  v.Expect.Workload "a poison at the committed-cursor boundary recovers without wedging" (fun r ->
                      r.PoisonAtBoundaryRecoversNoWedge)
                  v.Expect.Workload "the durable cursor advances monotonically across every claimed tick" (fun r ->
                      r.CursorMonotonic)
                  v.Trace.SpanExists
                      "timer-exactly-once completion span emitted"
                      "proof.wake.timer-exactly-once.completed"
                      [ "proof.property", "wake.timer-exactly-once" ]
                  v.Trace.Operation
                      "timer-exactly-once operation recorded"
                      ({ TraceOperationMatch.named "wake.timer-exactly-once" with
                          Status = Some "ok"
                          OutputContains = [ "DueTimerFiredExactlyOnce"; "PoisonConsumedNotWedged" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "wake.path" {
            describedAs
                "MS-C3 wake path over the public WakeShard/WakeRouter surface + the C2 TimerIndex: tail-latency within a recorded bound, and timer exactly-once across restart with poison tolerance and the last-scanned+1 cursor boundary."

            property tailLatencyProperty
            property timerExactlyOnceProperty
        }
