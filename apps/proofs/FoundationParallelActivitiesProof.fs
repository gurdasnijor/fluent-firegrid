namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Store.Foundation.Durable

/// K2 — concurrent activity-batch execution, proven over the public kernel
/// surface only (DurableClient / DurableHost / Mailbox / ActivityCommandAdapter).
/// A tick's due CallActivity commands execute concurrently, so a fast fanned
/// branch completes (its completion durably published) while a slow branch is
/// still running. Host kills are modeled the K1 way: the "killed" host's fence
/// is abandoned mid-flight and a fresh host (new fence) resumes from the
/// journal, with the kill window placed after a completion is durably
/// published but before the dispatcher checkpoint.
///
/// - `durable.parallel-overlap` — two activities fanned in one tick overlap:
///   the fast one's completion is durably published while the slow one is
///   still mid-flight; completions fold in completion order (not declaration
///   order) and replay still binds each value to its OpId.
/// - `durable.parallel-fault-isolation` — a throwing batch-mate fails the tick
///   but its mates' completions are already durable; the checkpoint holds; the
///   retry skips the mates and heals only the thrower.
///
/// The `durable.parallel-kill-window` obligation moved to the
/// `foundation.crash-window` invariant family (FoundationCrashWindowProof.fs)
/// in Packet 0.3b.
module FoundationParallelActivitiesProof =

    // ---- shared helpers over the public surface --------------------------

    let private registered result =
        match result with
        | Ok registry -> registry
        | Error error -> failwithf "proof registry assembly failed: %s" (string error)

    let private startOk started =
        match started with
        | DurableClientStartStatus.Accepted _ -> ()
        | DurableClientStartStatus.Failed failure -> failwithf "start failed unexpectedly: %s" (string failure)

    let private inspectOwned (pair: StreamPair) : OwnedKey =
        { Key = pair.Key
          Fence = FenceToken "proof:inspect"
          Log = pair.Log
          Inbox = pair.Inbox }

    let private tickInstance basin workflows activities hostId instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! S2Substrate.ensureStreams basin key
            let pair = S2Substrate.streams basin key
            let options = DurableHostTickOptions.create hostId (int64 (Reports.nowMillis ()))
            return! DurableHost.claimAndRunWorkflowTick options workflows activities basin pair
        }

    let private journalRecords basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! S2Substrate.ensureStreams basin key
            let owned = inspectOwned (S2Substrate.streams basin key)
            let! decoded = S2Substrate.readLogText StepRecordCodec.decode owned
            return decoded
        }

    let private countRecords predicate decoded =
        decoded
        |> List.sumBy (fun (_, entry) ->
            match entry with
            | Ok record when predicate record -> 1
            | _ -> 0)

    let private inboxMessages basin instanceId =
        async {
            let key = DurableClient.instanceKey instanceId
            do! S2Substrate.ensureStreams basin key
            let owned = inspectOwned (S2Substrate.streams basin key)
            let! records = S2Substrate.readMailbox 0L 1000 owned

            return
                records
                |> List.choose (fun (record: S2.ReadRecord) ->
                    match MailboxEnvelopeCodec.decode record.Body with
                    | Ok envelope -> Some envelope
                    | Error _ -> None)
        }

    let private completionsFor opId envelopes =
        envelopes
        |> List.filter (fun (envelope: MailboxEnvelope) ->
            match envelope.Message with
            | CompleteActivity(id, _) -> id = opId
            | _ -> false)
        |> List.length

    let private completionEventCount opId decoded =
        decoded
        |> countRecords (function
            | Incoming(HistoryEvent(ActivityCompleted(id, _))) -> id = opId
            | _ -> false)

    let private completionEventSeq opId decoded =
        decoded
        |> List.tryPick (fun (seqNum, entry) ->
            match entry with
            | Ok(Incoming(HistoryEvent(ActivityCompleted(id, _)))) when id = opId -> Some seqNum
            | _ -> None)

    let private callCommandSeq opId decoded =
        decoded
        |> List.tryPick (fun (seqNum, entry) ->
            match entry with
            | Ok(Outgoing(Command(CallActivity(id, _)))) when id = opId -> Some seqNum
            | _ -> None)

    let private activityCheckpointCount decoded =
        decoded
        |> countRecords (function
            | Incoming(CommandDispatchCheckpoint(name, _)) -> name = ActivityCommandAdapter.dispatcher
            | _ -> false)

    let private driveToCompletion tick status hostId instanceId remaining =
        let rec loop remaining =
            async {
                if remaining = 0 then
                    return failwith "parallel-activities: workflow never completed"
                else
                    let! (_: DurableWorkflowHostStatus) = tick hostId instanceId
                    let! current = status instanceId

                    match current with
                    | DurableClientStatusRead.Succeeded(InstanceCompleted(_, value)) -> return value
                    | _ -> return! loop (remaining - 1)
            }

        loop remaining

    let private admitAndStep owned factory input =
        async {
            let now = int64 (Reports.nowMillis ())

            let! (_: MailboxStatus) =
                Mailbox.runOnce StepRecordCodec.encode StepRecordCodec.decode MailboxEnvelopeCodec.decode 100 owned

            let! (_: DurableHostStatus<Payload>) =
                DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now owned (factory input)

            return ()
        }

    // ---- durable.parallel-overlap ------------------------------------------

    type ParallelOverlapResult =
        { FastCompletionPublishedDuringSlow: bool
          WorkflowCompletedWithBothValues: bool
          EachHandlerExecutedOnce: bool
          SingleCompletionEventPerOp: bool
          CompletionsFoldedInCompletionOrder: bool }

    let private overlapWorkload ctx =
        ProofOperation.run
            ctx
            "durable.parallel-overlap"
            "durable-parallel-overlap"
            { ProofOperationOptions.empty with
                Key = Some "durable-parallel-overlap" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "par-overlap-" + suffix
                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let instance = InstanceId.create ("overlap-" + suffix)

                let slowExecutions = ref 0
                let fastExecutions = ref 0
                let overlapObserved = ref false

                // The slow branch does not sleep for a fixed time: it polls the
                // instance's own inbox until the FAST branch's completion
                // (OpId 1) is durably published, then returns. Under
                // sequential execution this observation is impossible (the
                // fast handler cannot run before the slow one returns), so the
                // poll times out and the expectation fails.
                let activities =
                    ActivityRegistry.empty
                    |> ActivityRegistry.register "fan/slow" (fun input ->
                        async {
                            slowExecutions.Value <- slowExecutions.Value + 1

                            let rec poll remaining =
                                async {
                                    if remaining = 0 then
                                        return false
                                    else
                                        let! envelopes = inboxMessages basin instance

                                        if completionsFor (OpId 1) envelopes > 0 then
                                            return true
                                        else
                                            do! Async.Sleep 25
                                            return! poll (remaining - 1)
                                }

                            let! observed = poll 200
                            overlapObserved.Value <- overlapObserved.Value || observed
                            return "slow:" + input
                        })
                    |> Result.bind (
                        ActivityRegistry.register "fan/fast" (fun input ->
                            async {
                                fastExecutions.Value <- fastExecutions.Value + 1
                                return "fast:" + input
                            })
                    )
                    |> registered

                // Declaration order is slow first (OpId 0), fast second
                // (OpId 1): the fast branch completes and folds FIRST even
                // though it was declared second.
                let factory (input: Payload) : Durable<Payload> =
                    durable {
                        let! values =
                            Workflow.all
                                [ Activities.create "fan/slow" input
                                  Activities.create "fan/fast" input ]

                        match values with
                        | [ slowValue; fastValue ] -> return "join<" + slowValue + "|" + fastValue + ">"
                        | other -> return "join-arity:" + string (List.length other)
                    }

                let workflows =
                    WorkflowRegistry.empty
                    |> WorkflowRegistry.register "fan/join" factory
                    |> registered

                let tick = tickInstance basin workflows activities
                let status = DurableClient.getStatusWith basin workflows

                let! started = DurableClient.startWith basin instance (WorkflowName "fan/join") "job"
                startOk started

                let! value = driveToCompletion tick status "ohost-a" instance 20

                // Redundant restart tick: dedupe must absorb it.
                let! _ = tick "ohost-b" instance

                let! records = journalRecords basin instance

                let fastSeq = completionEventSeq (OpId 1) records
                let slowSeq = completionEventSeq (OpId 0) records

                let foldedInCompletionOrder =
                    match fastSeq, slowSeq with
                    | Some fast, Some slow -> fast < slow
                    | _ -> false

                let result =
                    { FastCompletionPublishedDuringSlow = overlapObserved.Value
                      WorkflowCompletedWithBothValues = value = "join<slow:job|fast:job>"
                      EachHandlerExecutedOnce = slowExecutions.Value = 1 && fastExecutions.Value = 1
                      SingleCompletionEventPerOp =
                        completionEventCount (OpId 0) records = 1
                        && completionEventCount (OpId 1) records = 1
                      CompletionsFoldedInCompletionOrder = foldedInCompletionOrder }

                do!
                    ctx.EmitSpan
                        "proof.durable.parallel-overlap.completed"
                        [ "proof.property", "durable.parallel-overlap"
                          "overlap.observed", string overlapObserved.Value
                          "overlap.value", value
                          "overlap.slow_executions", string slowExecutions.Value
                          "overlap.fast_executions", string fastExecutions.Value ]

                return result
            })

    let private overlapProperty =
        property "durable.parallel-overlap" {
            s2Lite ""
            workload overlapWorkload

            verify (fun v ->
                [ v.Expect.Workload
                      "the fast branch's completion is durably published while the slow branch is still running"
                      (fun r -> r.FastCompletionPublishedDuringSlow)
                  v.Expect.Workload
                      "the workflow completes with both values bound to their declared positions"
                      (fun r -> r.WorkflowCompletedWithBothValues)
                  v.Expect.Workload "each fanned handler executed exactly once despite a redundant restart" (fun r ->
                      r.EachHandlerExecutedOnce)
                  v.Expect.Workload "the journal holds exactly one completion event per op" (fun r ->
                      r.SingleCompletionEventPerOp)
                  v.Expect.Workload
                      "completions fold in completion order, not declaration order (replay matches by OpId)"
                      (fun r -> r.CompletionsFoldedInCompletionOrder)
                  v.Trace.Operation
                      "parallel-overlap operation recorded"
                      ({ TraceOperationMatch.named "durable.parallel-overlap" with
                          Status = Some "ok"
                          OutputContains = [ "FastCompletionPublishedDuringSlow"; "EachHandlerExecutedOnce" ]
                          Count = Some 1 }) ])
        }

    // ---- durable.parallel-fault-isolation -----------------------------------

    type ParallelFaultIsolationResult =
        { ThrowingMateFailedTheTick: bool
          MatesCompletionsDurableAtFailure: bool
          CheckpointHeldAtFailure: bool
          RetrySkippedMatesAndHealedThrower: bool
          MatesExecutedOnce: bool
          WorkflowCompletedWithAllValues: bool
          SingleCompletionEventPerOp: bool }

    let private faultIsolationWorkload ctx =
        ProofOperation.run
            ctx
            "durable.parallel-fault-isolation"
            "durable-parallel-fault-isolation"
            { ProofOperationOptions.empty with
                Key = Some "durable-parallel-fault-isolation" }
            (async {
                let s2 = WorkloadContext.requireS2 ctx
                let suffix = string (int64 (Reports.nowMillis ()))
                let basinName = "par-fault-" + suffix
                let! _ = s2.Client |> S2.createBasin basinName
                let basin = s2.Client |> S2.basin basinName

                let ok1Executions = ref 0
                let ok2Executions = ref 0
                let boomExecutions = ref 0

                let ok1Handler input =
                    async {
                        ok1Executions.Value <- ok1Executions.Value + 1
                        return "ok1:" + input
                    }

                let ok2Handler input =
                    async {
                        ok2Executions.Value <- ok2Executions.Value + 1
                        return "ok2:" + input
                    }

                // Same registry shape twice: first with a throwing middle
                // handler, then with a healed one. The ok handlers are the
                // SAME closures in both, so their execution counters prove the
                // retry never re-ran them.
                let faultyActivities =
                    ActivityRegistry.empty
                    |> ActivityRegistry.register "iso/ok1" ok1Handler
                    |> Result.bind (
                        ActivityRegistry.register "iso/boom" (fun _ ->
                            async {
                                boomExecutions.Value <- boomExecutions.Value + 1
                                return failwith "boom"
                            })
                    )
                    |> Result.bind (ActivityRegistry.register "iso/ok2" ok2Handler)
                    |> registered

                let healedActivities =
                    ActivityRegistry.empty
                    |> ActivityRegistry.register "iso/ok1" ok1Handler
                    |> Result.bind (
                        ActivityRegistry.register "iso/boom" (fun input ->
                            async {
                                boomExecutions.Value <- boomExecutions.Value + 1
                                return "boom2:" + input
                            })
                    )
                    |> Result.bind (ActivityRegistry.register "iso/ok2" ok2Handler)
                    |> registered

                let factory (input: Payload) : Durable<Payload> =
                    durable {
                        let! values =
                            Workflow.all
                                [ Activities.create "iso/ok1" input
                                  Activities.create "iso/boom" input
                                  Activities.create "iso/ok2" input ]

                        match values with
                        | [ first; second; third ] -> return "iso<" + first + "|" + second + "|" + third + ">"
                        | other -> return "iso-arity:" + string (List.length other)
                    }

                let workflows =
                    WorkflowRegistry.empty
                    |> WorkflowRegistry.register "iso/flow" factory
                    |> registered

                let tick = tickInstance basin workflows healedActivities
                let status = DurableClient.getStatusWith basin workflows

                let instance = InstanceId.create ("fault-iso-" + suffix)
                let! started = DurableClient.startWith basin instance (WorkflowName "iso/flow") "job"
                startOk started

                // Host A commits the fanned journal pair, then runs the batch
                // with the throwing middle handler: the tick fails, but the
                // mates' completions must already be durable.
                let key = DurableClient.instanceKey instance
                do! S2Substrate.ensureStreams basin key
                let pair = S2Substrate.streams basin key
                let! ownedA = S2Substrate.claim "ihost-a" pair
                do! admitAndStep ownedA factory "job"

                let! faultyRun =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        100
                        100
                        faultyActivities
                        ownedA

                let throwingMateFailedTheTick =
                    match faultyRun with
                    | ActivityCommandAdapterStatus.Failed(ActivityCommandAdapterFailure.HandlerFailed(ActivityName "iso/boom",
                                                                                                      _)) -> true
                    | _ -> false

                let! inboxAtFailure = inboxMessages basin instance

                let matesCompletionsDurableAtFailure =
                    completionsFor (OpId 0) inboxAtFailure = 1
                    && completionsFor (OpId 2) inboxAtFailure = 1
                    && completionsFor (OpId 1) inboxAtFailure = 0
                    && ok1Executions.Value = 1
                    && ok2Executions.Value = 1

                let! recordsAtFailure = journalRecords basin instance
                let checkpointHeldAtFailure = activityCheckpointCount recordsAtFailure = 0

                // Fresh host B retries with the healed registry: the durable
                // mates are skipped (AlreadyPublished), only the thrower
                // executes, and the checkpoint finally advances.
                let! ownedB = S2Substrate.claim "ihost-b" pair

                let! healedRun =
                    ActivityCommandAdapter.runOnce
                        StepRecordCodec.encode
                        StepRecordCodec.decode
                        100
                        100
                        healedActivities
                        ownedB

                let retrySkippedMatesAndHealedThrower =
                    match healedRun with
                    | ActivityCommandAdapterStatus.Processed report ->
                        report.AlreadyPublished = 2
                        && (report.Completed |> List.map (fun completion -> completion.OpId)) = [ OpId 1 ]
                        && boomExecutions.Value = 2
                        && (match report.Checkpoint with
                            | CommandDispatchCheckpointResult.Checkpointed _ -> true
                            | _ -> false)
                    | _ -> false

                let! value = driveToCompletion tick status "ihost-c" instance 20

                let! recordsFinal = journalRecords basin instance

                let result =
                    { ThrowingMateFailedTheTick = throwingMateFailedTheTick
                      MatesCompletionsDurableAtFailure = matesCompletionsDurableAtFailure
                      CheckpointHeldAtFailure = checkpointHeldAtFailure
                      RetrySkippedMatesAndHealedThrower = retrySkippedMatesAndHealedThrower
                      MatesExecutedOnce = ok1Executions.Value = 1 && ok2Executions.Value = 1
                      WorkflowCompletedWithAllValues = value = "iso<ok1:job|boom2:job|ok2:job>"
                      SingleCompletionEventPerOp =
                        completionEventCount (OpId 0) recordsFinal = 1
                        && completionEventCount (OpId 1) recordsFinal = 1
                        && completionEventCount (OpId 2) recordsFinal = 1 }

                do!
                    ctx.EmitSpan
                        "proof.durable.parallel-fault-isolation.completed"
                        [ "proof.property", "durable.parallel-fault-isolation"
                          "fault.value", value
                          "fault.ok1_executions", string ok1Executions.Value
                          "fault.ok2_executions", string ok2Executions.Value
                          "fault.boom_executions", string boomExecutions.Value ]

                return result
            })

    let private faultIsolationProperty =
        property "durable.parallel-fault-isolation" {
            s2Lite ""
            workload faultIsolationWorkload

            verify (fun v ->
                [ v.Expect.Workload "a throwing batch-mate fails the tick with its own HandlerFailed" (fun r ->
                      r.ThrowingMateFailedTheTick)
                  v.Expect.Workload
                      "the throwing mate's batch-mates' completions are already durable when the tick fails"
                      (fun r -> r.MatesCompletionsDurableAtFailure)
                  v.Expect.Workload
                      "the dispatcher checkpoint does not advance past a command without a published completion"
                      (fun r -> r.CheckpointHeldAtFailure)
                  v.Expect.Workload
                      "the retry skips the durable mates (AlreadyPublished) and executes only the healed thrower"
                      (fun r -> r.RetrySkippedMatesAndHealedThrower)
                  v.Expect.Workload "the batch-mates executed exactly once across failure and retry (no duplicates)" (fun r ->
                      r.MatesExecutedOnce)
                  v.Expect.Workload "the workflow completes with all three values" (fun r ->
                      r.WorkflowCompletedWithAllValues)
                  v.Expect.Workload "the journal holds exactly one completion event per op" (fun r ->
                      r.SingleCompletionEventPerOp)
                  v.Trace.Operation
                      "parallel-fault-isolation operation recorded"
                      ({ TraceOperationMatch.named "durable.parallel-fault-isolation" with
                          Status = Some "ok"
                          OutputContains = [ "MatesCompletionsDurableAtFailure"; "RetrySkippedMatesAndHealedThrower" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.parallel-activities" {
            describedAs
                "K2 concurrent activity-batch execution over the public kernel surface: a tick's due activity commands execute concurrently (fast completions durably published while slow batch-mates still run; completions fold in completion order and replay binds by OpId), and a throwing batch-mate isolates (mates' completions durable, checkpoint held, retry heals only the thrower)."

            property overlapProperty
            property faultIsolationProperty
        }
