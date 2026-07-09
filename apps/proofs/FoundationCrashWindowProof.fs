namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Store.Foundation.Durable

/// Packet 0.3b — `foundation.crash-window`: the at-most-once-under-crash
/// invariant over the committed-but-not-dispatched window, stated ONCE
/// (CrashWindowLaw.fs) and instantiated over four kernel scenarios. Retires
/// the four bespoke restatements:
///
///   1. continue-as-new   <- durable.continue-as-new
///   2. child-result      <- durable.child-workflow
///   3. one-way-send      <- durable.one-way-send
///   4. parallel-batch    <- durable.parallel-kill-window
///
/// Host kills are modeled the K1/K2 way: the "killed" host's fence is
/// abandoned mid-flight and a fresh host (new fence) resumes from the
/// journal, with the window placed after a fenced commit but before its
/// dispatch. Consolidation deletes RESTATEMENTS, never ASSERTIONS: every
/// check of every retired proof maps to a CrashWindowLaw core check or keeps
/// its original name as a fact check (see the PR correspondence table).
module FoundationCrashWindowProof =

    // ---- shared helpers over the public kernel surface --------------------
    // (from the retired durable-debts / parallel-activities proofs)

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
                    return failwith "crash-window: workflow never completed"
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

    // =======================================================================
    // 1. continue-as-new (from durable.continue-as-new)
    // =======================================================================

    [<Literal>]
    let private caFactRestartReports = "a fresh host reports the terminal ContinuedAsNew and dispatches the handoff"

    [<Literal>]
    let private caFactExecutedOncePerGen = "the step executed exactly once per generation (gen N journal not re-replayed)"

    [<Literal>]
    let private caFactFreshJournal =
        "generation N+1 has a fresh journal: one start, one step call, despite redundant restarts"

    [<Literal>]
    let private caFactRolloverOnce = "generation N committed exactly one rollover record and one dispatch command"

    [<Literal>]
    let private caFactStatusChain = "status follows the generation chain to the live generation's result"

    let private continueAsNewSurface: CrashWindowLaw.CrashWindowSurface<_, _> =
        { Instance = "continue-as-new"
          OperationName = "foundation.crash-window.continue-as-new"
          FactNames =
            [ caFactRestartReports
              caFactExecutedOncePerGen
              caFactFreshJournal
              caFactRolloverOnce
              caFactStatusChain ]
          Seed =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "debt-rollover-" + suffix
                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    let executions = ref 0

                    let activities =
                        ActivityRegistry.empty
                        |> ActivityRegistry.register "gen/tick" (fun input ->
                            async {
                                executions.Value <- executions.Value + 1
                                return input
                            })
                        |> registered

                    let factory (input: Payload) : Durable<Payload> =
                        durable {
                            let! _ = Workflow.call "gen/tick" input
                            let remaining = int input

                            if remaining > 1 then
                                return! Workflow.continueAsNew (string (remaining - 1))
                            else
                                return "eternal-done"
                        }

                    let workflows =
                        WorkflowRegistry.empty
                        |> WorkflowRegistry.register "gen/eternal" factory
                        |> registered

                    let tick = tickInstance basin workflows activities
                    let status = DurableClient.getStatusWith basin workflows

                    let genZero = InstanceId.create ("eternal-" + suffix)
                    let genOne = Generation.next genZero

                    let! started = DurableClient.startWith basin genZero (WorkflowName "gen/eternal") "2"
                    startOk started

                    // Host A drives generation 0 until the rollover terminal is
                    // COMMITTED. The next-generation dispatch runs on a later
                    // tick, so stopping here is exactly a kill inside the
                    // handoff window.
                    let rec driveUntilContinued remaining =
                        async {
                            if remaining = 0 then
                                return failwith "rollover: generation 0 never rolled over"
                            else
                                let! _ = tick "host-a" genZero
                                let! current = status genZero

                                match current with
                                | DurableClientStatusRead.Succeeded(InstanceContinuedAsNew _) -> return ()
                                | _ -> return! driveUntilContinued (remaining - 1)
                        }

                    do! driveUntilContinued 20

                    let! genOneAtKill = status genOne
                    let executionsAtKill = executions.Value

                    // The window: the rollover terminal is journal state; the
                    // next generation is not yet started.
                    let nextGenerationNotStartedAtKill =
                        genOneAtKill = DurableClientStatusRead.Succeeded InstanceNotFound
                        && executionsAtKill = 1

                    return (basin, activities, workflows, executions, genZero, genOne, nextGenerationNotStartedAtKill)
                }
          Crash =
            // Host A's fence is abandoned mid-handoff — the K1 kill model; the
            // window evidence was captured at the end of Seed.
            fun _ctx _seeded -> async { return () }
          Recover =
            fun _ctx (basin, activities, workflows, _, genZero, genOne, _) ->
                async {
                    let tick = tickInstance basin workflows activities
                    let status = DurableClient.getStatusWith basin workflows

                    // Fresh host B (the restart): its tick observes the journaled
                    // terminal and dispatches the deduped next-generation start.
                    let! restartTick = tick "host-b" genZero

                    let restartTickReportedContinuedAsNew =
                        match restartTick with
                        | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.ContinuedAsNew(nextInput, _)) ->
                            nextInput = "1"
                        | _ -> false

                    // Two more redundant restarts: checkpoint + receiver
                    // highwater must absorb them without duplicating the start.
                    let! _ = tick "host-c" genZero
                    let! _ = tick "host-d" genZero

                    // Drive generation 1 (a fresh instance) to completion.
                    let rec driveUntilCompleted remaining =
                        async {
                            if remaining = 0 then
                                return failwith "rollover: generation 1 never completed"
                            else
                                let! _ = tick "host-b" genOne
                                let! current = status genOne

                                match current with
                                | DurableClientStatusRead.Succeeded(InstanceCompleted(_, value)) -> return value
                                | _ -> return! driveUntilCompleted (remaining - 1)
                        }

                    let! genOneValue = driveUntilCompleted 20
                    return (restartTickReportedContinuedAsNew, genOneValue)
                }
          Observe =
            fun _ctx seeded (restartTickReportedContinuedAsNew, genOneValue) ->
                async {
                    let (basin, _, workflows, executions, genZero, genOne, nextGenerationNotStartedAtKill) =
                        seeded

                    let! genZeroRecords = journalRecords basin genZero
                    let! genOneRecords = journalRecords basin genOne

                    let genOneCalls =
                        genOneRecords
                        |> countRecords (function
                            | Incoming(HistoryEvent(ActivityCalled _)) -> true
                            | _ -> false)

                    let genOneStarts =
                        genOneRecords
                        |> countRecords (function
                            | Incoming(WorkflowStarted _) -> true
                            | _ -> false)

                    let genZeroRollovers =
                        genZeroRecords
                        |> countRecords (function
                            | Incoming(HistoryEvent(WorkflowContinuedAsNew _)) -> true
                            | _ -> false)

                    let genZeroDispatchCommands =
                        genZeroRecords
                        |> countRecords (function
                            | Outgoing(Command(StartNextGeneration _)) -> true
                            | _ -> false)

                    let! followed = DurableClient.getStatusFollowingWith basin workflows genZero

                    let statusFollowsChain =
                        followed = DurableClientStatusRead.Succeeded(
                            InstanceCompleted(WorkflowName "gen/eternal", "eternal-done")
                        )

                    let executedOncePerGeneration = executions.Value = 2
                    let genOneJournalSingleCallAndStart = genOneCalls = 1 && genOneStarts = 1

                    let genZeroRolloverCommittedOnce =
                        genZeroRollovers = 1 && genZeroDispatchCommands = 1

                    return
                        { CrashWindowLaw.WindowEstablished = nextGenerationNotStartedAtKill
                          CrashWindowLaw.NothingLost = genOneValue = "eternal-done"
                          CrashWindowLaw.NothingDuplicated =
                            executedOncePerGeneration
                            && genOneJournalSingleCallAndStart
                            && genZeroRolloverCommittedOnce
                          // The two redundant host-c/host-d restarts did not
                          // duplicate the next-generation start.
                          CrashWindowLaw.RedundantRecoveryIdempotent = genOneJournalSingleCallAndStart
                          CrashWindowLaw.Facts =
                            [ caFactRestartReports, restartTickReportedContinuedAsNew
                              caFactExecutedOncePerGen, executedOncePerGeneration
                              caFactFreshJournal, genOneJournalSingleCallAndStart
                              caFactRolloverOnce, genZeroRolloverCommittedOnce
                              caFactStatusChain, statusFollowsChain ] }
                } }

    // =======================================================================
    // 2. child-result (from durable.child-workflow)
    // =======================================================================

    [<Literal>]
    let private crFactParked = "after the parent restart the parent parks durably on the child"

    [<Literal>]
    let private crFactBinding = "the restarted parent dispatches the child with its parent binding recorded"

    [<Literal>]
    let private crFactReEnsure = "re-ensuring after the child restart commits no second delivery command"

    [<Literal>]
    let private crFactDeliveredOnce = "the child's terminal result is delivered to the parent exactly once"

    [<Literal>]
    let private crFactChildStepOnce = "the child's step executed exactly once"

    [<Literal>]
    let private crFactSingleCompletion = "the parent journal holds exactly one child completion event"

    let private childResultSurface: CrashWindowLaw.CrashWindowSurface<_, _> =
        { Instance = "child-result"
          OperationName = "foundation.crash-window.child-result"
          FactNames =
            [ crFactParked
              crFactBinding
              crFactReEnsure
              crFactDeliveredOnce
              crFactChildStepOnce
              crFactSingleCompletion ]
          Seed =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "debt-child-" + suffix
                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName
                    let now = int64 (Reports.nowMillis ())

                    let workExecutions = ref 0

                    let activities =
                        ActivityRegistry.empty
                        |> ActivityRegistry.register "flow/work" (fun input ->
                            async {
                                workExecutions.Value <- workExecutions.Value + 1
                                return "w:" + input
                            })
                        |> registered

                    let parentFactory (input: Payload) : Durable<Payload> =
                        durable {
                            let! childResult = Workflow.callChild "flow/child" (input + ":c")
                            return "p<" + childResult + ">"
                        }

                    let childFactory (input: Payload) : Durable<Payload> =
                        durable {
                            let! value = Workflow.call "flow/work" input
                            return "c<" + value + ">"
                        }

                    let workflows =
                        WorkflowRegistry.empty
                        |> WorkflowRegistry.register "flow/parent" parentFactory
                        |> Result.bind (WorkflowRegistry.register "flow/child" childFactory)
                        |> registered

                    let status = DurableClient.getStatusWith basin workflows

                    let parent = InstanceId.create ("parent-" + suffix)
                    let child = ChildInstance.idFor parent (OpId 0)

                    let! started = DurableClient.startWith basin parent (WorkflowName "flow/parent") "job"
                    startOk started

                    // Parent host A: admit + step only — the CallChildWorkflow
                    // command is committed, then the host dies before dispatch.
                    let parentKey = DurableClient.instanceKey parent
                    do! S2Substrate.ensureStreams basin parentKey
                    let parentPair = S2Substrate.streams basin parentKey
                    let! parentOwnedA = S2Substrate.claim "phost-a" parentPair

                    let! _ =
                        Mailbox.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            MailboxEnvelopeCodec.decode
                            100
                            parentOwnedA

                    let! _ =
                        DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now parentOwnedA (parentFactory "job")

                    let! childAtParentKill = status child

                    // Window 1: the journaled child start survives the parent
                    // kill before dispatch — the child is not yet started.
                    let childNotStartedAtParentKill =
                        childAtParentKill = DurableClientStatusRead.Succeeded InstanceNotFound

                    return (basin, activities, workflows, workExecutions, parent, child, childFactory, childNotStartedAtParentKill)
                }
          Crash =
            // Parent host A's fence is abandoned after the CallChildWorkflow
            // commit; child host A's fence is abandoned later inside Recover,
            // after the DeliverChildResult commit (window 2).
            fun _ctx _seeded -> async { return () }
          Recover =
            fun _ctx (basin, activities, workflows, _, parent, child, childFactory, _) ->
                async {
                    let tick = tickInstance basin workflows activities
                    let status = DurableClient.getStatusWith basin workflows
                    let now = int64 (Reports.nowMillis ())

                    // Parent restart: a fresh full tick dispatches the journaled
                    // child start; the parent parks durably on the child.
                    let! _ = tick "phost-b" parent
                    let! parentAfterRestart = status parent

                    let parentParked =
                        match parentAfterRestart with
                        | DurableClientStatusRead.Succeeded(InstanceWaiting(_, OpId 0, NeedsChildWorkflow("flow/child", _))) ->
                            true
                        | _ -> false

                    // The dispatch is an inbox fact; the WorkflowParent binding
                    // lands in the child journal at mailbox admission (checked
                    // in Observe, once the child has ticked).
                    let! childInboxAfterRestart = inboxMessages basin child

                    let childStartEnvelopes (envelopes: MailboxEnvelope list) =
                        envelopes
                        |> List.filter (fun envelope ->
                            match envelope.Message with
                            | StartChildWorkflow(WorkflowName "flow/child", _, boundParent, OpId 0) ->
                                boundParent = InstanceId.value parent
                            | _ -> false)
                        |> List.length

                    let childStartDispatchedOnce = childStartEnvelopes childInboxAfterRestart = 1

                    // Child host A: run the child to internal completion with
                    // admit/step/activity only, then commit the terminal
                    // delivery command — and die before dispatching it.
                    let childKey = DurableClient.instanceKey child
                    let childPair = S2Substrate.streams basin childKey
                    let! childOwnedA = S2Substrate.claim "chost-a" childPair

                    let rec driveChildToCompletion remaining =
                        async {
                            if remaining = 0 then
                                return failwith "child never completed internally"
                            else
                                let! _ =
                                    Mailbox.runOnce
                                        StepRecordCodec.encode
                                        StepRecordCodec.decode
                                        MailboxEnvelopeCodec.decode
                                        100
                                        childOwnedA

                                let! step =
                                    DurableHost.stepOnce
                                        StepRecordCodec.encode
                                        StepRecordCodec.decode
                                        now
                                        childOwnedA
                                        (childFactory "job:c")

                                match step with
                                | DurableHostStatus.Completed value -> return value
                                | _ ->
                                    let! _ =
                                        ActivityCommandAdapter.runOnce
                                            StepRecordCodec.encode
                                            StepRecordCodec.decode
                                            100
                                            100
                                            activities
                                            childOwnedA

                                    return! driveChildToCompletion (remaining - 1)
                        }

                    let! childValue = driveChildToCompletion 10

                    let! ensured =
                        ChildResultAdapter.ensureCommand StepRecordCodec.encode StepRecordCodec.decode childOwnedA childValue

                    let ensuredCommitted =
                        match ensured with
                        | ChildResultCommandStatus.Committed _ -> true
                        | _ -> false

                    let! parentInboxAtChildKill = inboxMessages basin parent

                    let completionsAtChildKill =
                        parentInboxAtChildKill
                        |> List.filter (fun envelope ->
                            match envelope.Message with
                            | CompleteChild _ -> true
                            | _ -> false)
                        |> List.length

                    // Window 2: the terminal delivery command survives a child
                    // kill between the DeliverChildResult commit and its
                    // dispatch — the result is not yet delivered.
                    let resultNotDeliveredAtChildKill = ensuredCommitted && completionsAtChildKill = 0

                    // Child restart: a fresh claim re-ensures (idempotent — no
                    // second command) and a full tick dispatches the delivery.
                    let! childOwnedB = S2Substrate.claim "chost-b" childPair

                    let! reEnsured =
                        ChildResultAdapter.ensureCommand StepRecordCodec.encode StepRecordCodec.decode childOwnedB childValue

                    let! _ = tick "chost-c" child
                    let! _ = tick "chost-d" child

                    let! parentInboxFinal = inboxMessages basin parent

                    let completionsDelivered =
                        parentInboxFinal
                        |> List.filter (fun envelope ->
                            match envelope.Message with
                            | CompleteChild _ -> true
                            | _ -> false)
                        |> List.length

                    // Parent (fresh host again) resumes from the delivered result.
                    let! parentValue = driveToCompletion tick status "phost-e" parent 20

                    return
                        (parentParked,
                         childStartDispatchedOnce,
                         resultNotDeliveredAtChildKill,
                         reEnsured,
                         completionsDelivered,
                         parentValue)
                }
          Observe =
            fun _ctx seeded recovered ->
                async {
                    let (basin, _, _, workExecutions, parent, child, _, childNotStartedAtParentKill) = seeded

                    let (parentParked,
                         childStartDispatchedOnce,
                         resultNotDeliveredAtChildKill,
                         reEnsured,
                         completionsDelivered,
                         parentValue) =
                        recovered

                    let! parentRecordsFinal = journalRecords basin parent
                    let! childRecordsFinal = journalRecords basin child

                    let childCompletions =
                        parentRecordsFinal
                        |> countRecords (function
                            | Incoming(HistoryEvent(ChildWorkflowCompleted _)) -> true
                            | _ -> false)

                    let deliveryCommands =
                        childRecordsFinal
                        |> countRecords (function
                            | Outgoing(Command(DeliverChildResult _)) -> true
                            | _ -> false)

                    let deliveryCommandCommittedOnce =
                        reEnsured = ChildResultCommandStatus.AlreadyCommitted && deliveryCommands = 1

                    // The parent binding was admitted into the child journal, and
                    // the parent's later ticks (checkpointed) never re-dispatched
                    // the start.
                    let childBinding =
                        childRecordsFinal
                        |> countRecords (function
                            | Incoming(WorkflowParent(boundParent, OpId 0)) -> boundParent = InstanceId.value parent
                            | _ -> false)

                    let! childInboxFinal = inboxMessages basin child

                    let childStartsFinal =
                        childInboxFinal
                        |> List.filter (fun envelope ->
                            match envelope.Message with
                            | StartChildWorkflow(WorkflowName "flow/child", _, boundParent, OpId 0) ->
                                boundParent = InstanceId.value parent
                            | _ -> false)
                        |> List.length

                    let childStartedWithParentBinding =
                        childStartDispatchedOnce && childBinding = 1 && childStartsFinal = 1

                    let resultDeliveredExactlyOnce = completionsDelivered = 1
                    let childStepExecutedOnce = workExecutions.Value = 1
                    let parentJournalSingleChildCompletion = childCompletions = 1

                    return
                        { CrashWindowLaw.WindowEstablished =
                            childNotStartedAtParentKill && resultNotDeliveredAtChildKill
                          CrashWindowLaw.NothingLost = parentValue = "p<c<w:job:c>>"
                          CrashWindowLaw.NothingDuplicated =
                            resultDeliveredExactlyOnce
                            && childStepExecutedOnce
                            && parentJournalSingleChildCompletion
                          CrashWindowLaw.RedundantRecoveryIdempotent = deliveryCommandCommittedOnce
                          CrashWindowLaw.Facts =
                            [ crFactParked, parentParked
                              crFactBinding, childStartedWithParentBinding
                              crFactReEnsure, deliveryCommandCommittedOnce
                              crFactDeliveredOnce, resultDeliveredExactlyOnce
                              crFactChildStepOnce, childStepExecutedOnce
                              crFactSingleCompletion, parentJournalSingleChildCompletion ] }
                } }

    // =======================================================================
    // 3. one-way-send (from durable.one-way-send)
    // =======================================================================

    [<Literal>]
    let private owFactCallerAdvanced =
        "the caller advances past the journaled send before the handler ever executes (no await)"

    [<Literal>]
    let private owFactJournaledOnce = "the send is journaled once and never produces a completion event"

    [<Literal>]
    let private owFactAckGuards = "a published ack guards re-execution across a kill between ack and checkpoint"

    [<Literal>]
    let private owFactThrowingSwallowed = "a throwing send handler is swallowed: the calling workflow still completes"

    let private oneWaySendSurface: CrashWindowLaw.CrashWindowSurface<_, _> =
        { Instance = "one-way-send"
          OperationName = "foundation.crash-window.one-way-send"
          FactNames = [ owFactCallerAdvanced; owFactJournaledOnce; owFactAckGuards; owFactThrowingSwallowed ]
          Seed =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "debt-send-" + suffix
                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName
                    let now = int64 (Reports.nowMillis ())

                    let auditExecutions = ref 0

                    let activities =
                        ActivityRegistry.empty
                        |> ActivityRegistry.register "audit/note" (fun _ ->
                            async {
                                auditExecutions.Value <- auditExecutions.Value + 1
                                return ""
                            })
                        |> Result.bind (ActivityRegistry.register "audit/boom" (fun _ -> async { return failwith "boom" }))
                        |> Result.bind (ActivityRegistry.register "flow/main" (fun input -> async { return "m:" + input }))
                        |> registered

                    let senderFactory (input: Payload) : Durable<Payload> =
                        durable {
                            do! Workflow.send "audit/note" input
                            let! value = Workflow.call "flow/main" input
                            return "s<" + value + ">"
                        }

                    let faultyFactory (input: Payload) : Durable<Payload> =
                        durable {
                            do! Workflow.send "audit/boom" input
                            let! value = Workflow.call "flow/main" input
                            return "f<" + value + ">"
                        }

                    let workflows =
                        WorkflowRegistry.empty
                        |> WorkflowRegistry.register "flow/sender" senderFactory
                        |> Result.bind (WorkflowRegistry.register "flow/faulty" faultyFactory)
                        |> registered

                    // Scenario 1: the caller never awaits; delivery is
                    // exactly-once-effective across a host restart.
                    let sender = InstanceId.create ("sender-" + suffix)
                    let! started = DurableClient.startWith basin sender (WorkflowName "flow/sender") "job"
                    startOk started

                    let senderKey = DurableClient.instanceKey sender
                    do! S2Substrate.ensureStreams basin senderKey
                    let senderPair = S2Substrate.streams basin senderKey
                    let! senderOwnedA = S2Substrate.claim "shost-a" senderPair

                    let! _ =
                        Mailbox.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            MailboxEnvelopeCodec.decode
                            100
                            senderOwnedA

                    // Step 1 journals the send; step 2 journals the next call —
                    // the program advanced past the send with zero executions.
                    let! _ =
                        DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now senderOwnedA (senderFactory "job")

                    let! _ =
                        DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now senderOwnedA (senderFactory "job")

                    let! parked =
                        DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now senderOwnedA (senderFactory "job")

                    let callerAdvanced =
                        match parked with
                        | DurableHostStatus.Waiting(OpId 1, NeedsActivity activity) ->
                            activity.Name = "flow/main" && auditExecutions.Value = 0
                        | _ -> false

                    return (basin, activities, workflows, auditExecutions, sender, senderFactory, callerAdvanced, suffix)
                }
          Crash =
            // "Kill" host A: its fence is abandoned with the send journaled but
            // never executed.
            fun _ctx _seeded -> async { return () }
          Recover =
            fun _ctx (basin, activities, workflows, auditExecutions, sender, senderFactory, _, suffix) ->
                async {
                    let tick = tickInstance basin workflows activities
                    let status = DurableClient.getStatusWith basin workflows

                    // A fresh host completes the workflow.
                    let! senderValue = driveToCompletion tick status "shost-b" sender 20
                    let executionsAfterComplete = auditExecutions.Value

                    // Redundant restart tick: checkpoint + ack dedupe hold.
                    let! _ = tick "shost-c" sender
                    let executionsAfterRestart = auditExecutions.Value

                    // Scenario 2: the ack (published before the checkpoint)
                    // guards re-execution — the exact kill-between-ack-and-
                    // checkpoint window. A pre-acked send command is skipped.
                    let acked = InstanceId.create ("acked-" + suffix)
                    let! ackedStart = DurableClient.startWith basin acked (WorkflowName "flow/sender") "job2"
                    startOk ackedStart

                    let ackedKey = DurableClient.instanceKey acked
                    do! S2Substrate.ensureStreams basin ackedKey
                    let ackedPair = S2Substrate.streams basin ackedKey
                    let! ackedOwned = S2Substrate.claim "ahost-a" ackedPair
                    let now = int64 (Reports.nowMillis ())

                    let! _ =
                        Mailbox.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            MailboxEnvelopeCodec.decode
                            100
                            ackedOwned

                    let! _ =
                        DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now ackedOwned (senderFactory "job2")

                    let! ackedRecords = journalRecords basin acked

                    let sendCommandSeq =
                        ackedRecords
                        |> List.tryPick (fun (seqNum, entry) ->
                            match entry with
                            | Ok(Outgoing(Command(SendActivity _))) -> Some seqNum
                            | _ -> None)

                    let commandSeq =
                        match sendCommandSeq with
                        | Some seqNum -> seqNum
                        | None -> failwith "one-way: no SendActivity command journaled"

                    // Simulate the killed host's last act: the ack landed in the
                    // inbox, the checkpoint did not.
                    let ackEnvelope =
                        { Source = ActivityCommandAdapter.sendAckSource
                          SourceSeqNum = commandSeq
                          Message = AckSend(OpId 0) }

                    let! _ =
                        S2Substrate.appendMailboxText
                            [ "src", ActivityCommandAdapter.sendAckSource; "seq", string commandSeq ]
                            (MailboxEnvelopeCodec.encode ackEnvelope)
                            ackedPair

                    let executionsBeforeAdapter = auditExecutions.Value

                    let! adapterStatus =
                        ActivityCommandAdapter.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            100
                            100
                            activities
                            ackedOwned

                    let ackGuarded =
                        match adapterStatus with
                        | ActivityCommandAdapterStatus.Processed report ->
                            report.AlreadySent = 1 && auditExecutions.Value = executionsBeforeAdapter
                        | _ -> false

                    // Scenario 3: a throwing send handler never surfaces — the
                    // workflow completes anyway.
                    let faulty = InstanceId.create ("faulty-" + suffix)
                    let! faultyStart = DurableClient.startWith basin faulty (WorkflowName "flow/faulty") "job3"
                    startOk faultyStart

                    let! faultyValue = driveToCompletion tick status "fhost-a" faulty 20

                    return (senderValue, executionsAfterComplete, executionsAfterRestart, ackGuarded, faultyValue)
                }
          Observe =
            fun _ctx seeded recovered ->
                async {
                    let (basin, _, _, _, sender, _, callerAdvanced, _) = seeded

                    let (senderValue, executionsAfterComplete, executionsAfterRestart, ackGuarded, faultyValue) =
                        recovered

                    let! senderRecords = journalRecords basin sender

                    let sentEvents =
                        senderRecords
                        |> countRecords (function
                            | Incoming(HistoryEvent(ActivitySent(OpId 0, _))) -> true
                            | _ -> false)

                    let sendCompletionEvents =
                        senderRecords
                        |> countRecords (function
                            | Incoming(HistoryEvent(ActivityCompleted(OpId 0, _))) -> true
                            | _ -> false)

                    let sendJournaledWithoutCompletionEvent = sentEvents = 1 && sendCompletionEvents = 0

                    return
                        { CrashWindowLaw.WindowEstablished = callerAdvanced && sendJournaledWithoutCompletionEvent
                          CrashWindowLaw.NothingLost = senderValue = "s<m:job>"
                          CrashWindowLaw.NothingDuplicated = executionsAfterComplete = 1
                          CrashWindowLaw.RedundantRecoveryIdempotent = executionsAfterRestart = 1
                          CrashWindowLaw.Facts =
                            [ owFactCallerAdvanced, callerAdvanced
                              owFactJournaledOnce, sendJournaledWithoutCompletionEvent
                              owFactAckGuards, ackGuarded
                              owFactThrowingSwallowed, (faultyValue = "f<m:job3>") ] }
                } }

    // =======================================================================
    // 4. parallel-batch (from durable.parallel-kill-window)
    // =======================================================================

    [<Literal>]
    let private pbFactPublishedSkipped =
        "the batch-mate whose completion was durably published is skipped, never re-executed (no duplicate)"

    [<Literal>]
    let private pbFactUnpublishedOnce =
        "the batch-mate whose completion was not yet published executes exactly once (nothing lost)"

    [<Literal>]
    let private pbFactCheckpointAfter =
        "the checkpoint advances only after every due command's completion is durably published"

    [<Literal>]
    let private pbFactSingleCompletion = "the journal holds exactly one completion event per op"

    /// `skipDedupGuard = false` is the law; `true` is the known-bad
    /// negative-control variant modelling a recovery that skips the dedup
    /// guard: it re-executes the already-published batch-mate's handler
    /// directly before the adapter runs — the core "nothing duplicated" check
    /// must catch it.
    let private parallelBatchSurface (skipDedupGuard: bool) : CrashWindowLaw.CrashWindowSurface<_, _> =
        { Instance = "parallel-batch"
          OperationName = "foundation.crash-window.parallel-batch"
          FactNames = [ pbFactPublishedSkipped; pbFactUnpublishedOnce; pbFactCheckpointAfter; pbFactSingleCompletion ]
          Seed =
            fun ctx ->
                async {
                    let s2 = WorkloadContext.requireS2 ctx
                    let suffix = string (int64 (Reports.nowMillis ()))
                    let basinName = "par-kill-" + suffix
                    let! _ = s2.Client |> S2.createBasin basinName
                    let basin = s2.Client |> S2.basin basinName

                    let aExecutions = ref 0
                    let bExecutions = ref 0

                    let bHandler (input: Payload) =
                        async {
                            bExecutions.Value <- bExecutions.Value + 1
                            return "b:" + input
                        }

                    let activities =
                        ActivityRegistry.empty
                        |> ActivityRegistry.register "kw/a" (fun input ->
                            async {
                                aExecutions.Value <- aExecutions.Value + 1
                                return "a:" + input
                            })
                        |> Result.bind (ActivityRegistry.register "kw/b" bHandler)
                        |> registered

                    let factory (input: Payload) : Durable<Payload> =
                        durable {
                            let! values =
                                Workflow.all [ Activities.create "kw/a" input; Activities.create "kw/b" input ]

                            match values with
                            | [ aValue; bValue ] -> return "kw<" + aValue + "|" + bValue + ">"
                            | other -> return "kw-arity:" + string (List.length other)
                        }

                    let workflows =
                        WorkflowRegistry.empty
                        |> WorkflowRegistry.register "kw/flow" factory
                        |> registered

                    let instance = InstanceId.create ("kill-window-" + suffix)
                    let! started = DurableClient.startWith basin instance (WorkflowName "kw/flow") "job"
                    startOk started

                    // Host A admits the start and commits the fanned journal
                    // pair, then "dies" mid-batch: b's completion is durably
                    // published (fabricated exactly as the concurrent adapter
                    // publishes it — per-command source, provenance seqNum) but
                    // a's completion and the dispatcher checkpoint are not.
                    let key = DurableClient.instanceKey instance
                    do! S2Substrate.ensureStreams basin key
                    let pair = S2Substrate.streams basin key
                    let! ownedA = S2Substrate.claim "khost-a" pair
                    do! admitAndStep ownedA factory "job"

                    let! recordsAtKill = journalRecords basin instance

                    let commandSeq =
                        match callCommandSeq (OpId 1) recordsAtKill with
                        | Some seqNum -> seqNum
                        | None -> failwith "kill-window: no CallActivity command journaled for kw/b"

                    let source = ActivityCommandAdapter.completionSourceFor commandSeq

                    let envelope =
                        { Source = source
                          SourceSeqNum = commandSeq
                          Message = CompleteActivity(OpId 1, "b:killed-host") }

                    let! _ =
                        S2Substrate.appendMailboxText
                            [ "src", source; "seq", string commandSeq ]
                            (MailboxEnvelopeCodec.encode envelope)
                            pair

                    // The window: a completion was published, no checkpoint.
                    let checkpointHeldAtKill = activityCheckpointCount recordsAtKill = 0

                    return (basin, activities, workflows, instance, aExecutions, bExecutions, bHandler, checkpointHeldAtKill)
                }
          Crash =
            // Host A's fence is abandoned mid-batch, after b's completion was
            // durably published but before the dispatcher checkpoint.
            fun _ctx _seeded -> async { return () }
          Recover =
            fun _ctx (basin, activities, workflows, instance, aExecutions, bExecutions, bHandler, _) ->
                async {
                    if skipDedupGuard then
                        // Known-bad variant: a recovery that skips the dedup
                        // guard re-executes the published mate's handler
                        // directly before the adapter runs.
                        let! _ = bHandler "job"
                        ()

                    // Fresh host B recovers the batch: the published mate is
                    // skipped without re-execution, the unpublished mate runs,
                    // and only then does the checkpoint advance.
                    let key = DurableClient.instanceKey instance
                    let pair = S2Substrate.streams basin key
                    let! ownedB = S2Substrate.claim "khost-b" pair

                    let! recovered =
                        ActivityCommandAdapter.runOnce
                            StepRecordCodec.encode
                            StepRecordCodec.decode
                            100
                            100
                            activities
                            ownedB

                    let publishedMateSkipped, unpublishedMateRan, checkpointAdvanced =
                        match recovered with
                        | ActivityCommandAdapterStatus.Processed report ->
                            let completedOps =
                                report.Completed |> List.map (fun completion -> completion.OpId)

                            report.AlreadyPublished = 1 && bExecutions.Value = 0,
                            completedOps = [ OpId 0 ] && aExecutions.Value = 1,
                            (match report.Checkpoint with
                             | CommandDispatchCheckpointResult.Checkpointed _ -> true
                             | _ -> false)
                        | _ -> false, false, false

                    let tick = tickInstance basin workflows activities
                    let status = DurableClient.getStatusWith basin workflows

                    let! value = driveToCompletion tick status "khost-c" instance 20

                    // Redundant restart tick: dedupe must absorb it.
                    let! _ = tick "khost-d" instance

                    return (publishedMateSkipped, unpublishedMateRan, checkpointAdvanced, value)
                }
          Observe =
            fun _ctx seeded (publishedMateSkipped, unpublishedMateRan, checkpointAdvanced, value) ->
                async {
                    let (basin, _, _, instance, aExecutions, bExecutions, _, checkpointHeldAtKill) = seeded

                    let! recordsFinal = journalRecords basin instance

                    let singleCompletionEventPerOp =
                        completionEventCount (OpId 0) recordsFinal = 1
                        && completionEventCount (OpId 1) recordsFinal = 1

                    let publishedMateNotReExecuted = publishedMateSkipped && bExecutions.Value = 0
                    let unpublishedMateExecutedOnce = unpublishedMateRan && aExecutions.Value = 1

                    return
                        { CrashWindowLaw.WindowEstablished = checkpointHeldAtKill
                          CrashWindowLaw.NothingLost =
                            unpublishedMateExecutedOnce && value = "kw<a:job|b:killed-host>"
                          CrashWindowLaw.NothingDuplicated =
                            publishedMateNotReExecuted && singleCompletionEventPerOp
                          // After the redundant khost-d tick the journal still
                          // holds exactly one completion event per op.
                          CrashWindowLaw.RedundantRecoveryIdempotent = singleCompletionEventPerOp
                          CrashWindowLaw.Facts =
                            [ pbFactPublishedSkipped, publishedMateNotReExecuted
                              pbFactUnpublishedOnce, unpublishedMateExecutedOnce
                              pbFactCheckpointAfter, checkpointAdvanced
                              pbFactSingleCompletion, singleCompletionEventPerOp ] }
                } }

    // ---- properties + proof ------------------------------------------------

    let private continueAsNewProperty = CrashWindowLaw.makeProperty continueAsNewSurface
    let private childResultProperty = CrashWindowLaw.makeProperty childResultSurface
    let private oneWaySendProperty = CrashWindowLaw.makeProperty oneWaySendSurface
    let private parallelBatchProperty = CrashWindowLaw.makeProperty (parallelBatchSurface false)

    let proof =
        proof "foundation.crash-window" {
            describedAs
                "The at-most-once-under-crash invariant stated once (CrashWindowLaw) and instantiated over four kernel scenarios: continue-as-new (rollover terminal committed, next-generation dispatch pending), child-result (journaled child start + DeliverChildResult, both-side kills), one-way-send (journaled fire-and-forget + ack dedupe + swallowed handler failure), and parallel-batch (partial batch publication before the dispatcher checkpoint). The window is real; nothing lost; nothing duplicated; redundant recovery idempotent; trace-op evidence per instantiation."

            property continueAsNewProperty
            property childResultProperty
            property oneWaySendProperty
            property parallelBatchProperty
        }
