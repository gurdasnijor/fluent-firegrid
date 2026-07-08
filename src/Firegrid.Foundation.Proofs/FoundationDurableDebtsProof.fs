namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Firegrid.Store.Foundation.Durable

/// K1 — kernel-debt primitives, proven over the public kernel surface only
/// (DurableClient / DurableHost / Mailbox / adapters). Host kills are modeled
/// the way the wake-path proof models router restarts: the "killed" host's
/// fence is abandoned mid-flight and a fresh host (new fence) resumes from the
/// journal. The kill windows exercised are the dangerous ones: after a fenced
/// commit but before its dispatch.
///
/// - `durable.continue-as-new` — a WorkflowContinuedAsNew terminal committed by
///   generation N survives a host kill before the next-generation dispatch;
///   generation N+1 completes as a fresh instance (prior journal not
///   re-replayed); redundant restarts do not duplicate the next-generation
///   start; status follows the chain to the live generation.
/// - `durable.child-workflow` — a journaled child start survives a parent kill
///   before dispatch; the child's terminal result survives a child kill
///   between the DeliverChildResult commit and its dispatch; the parent parks
///   durably and completes with the child's result; every effect is
///   exactly-once-effective across all kills.
/// - `durable.one-way-send` — the send is journaled and the caller advances
///   without awaiting; delivery is exactly-once-effective across a host
///   restart (checkpoint + ack dedupe); a throwing send handler never surfaces
///   to the calling workflow.
module FoundationDurableDebtsProof =

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

    // ---- durable.continue-as-new ------------------------------------------

    type RolloverResult =
        { NextGenerationNotStartedAtKill: bool
          RestartTickReportedContinuedAsNew: bool
          NextGenerationCompleted: bool
          ExecutedOncePerGeneration: bool
          GenOneJournalSingleCallAndStart: bool
          GenZeroRolloverCommittedOnce: bool
          StatusFollowsChainToCompletion: bool }

    let private rolloverWorkload ctx =
        ProofOperation.run
            ctx
            "durable.continue-as-new"
            "durable-continue-as-new"
            { ProofOperationOptions.empty with
                Key = Some "durable-continue-as-new" }
            (async {
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
                // COMMITTED. The next-generation dispatch runs on a later tick,
                // so stopping here is exactly a kill inside the handoff window.
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

                let nextGenerationNotStartedAtKill =
                    genOneAtKill = DurableClientStatusRead.Succeeded InstanceNotFound
                    && executionsAtKill = 1

                // Fresh host B (the restart): its tick observes the journaled
                // terminal and dispatches the deduped next-generation start.
                let! restartTick = tick "host-b" genZero

                let restartTickReportedContinuedAsNew =
                    match restartTick with
                    | DurableWorkflowHostStatus.Ticked(DurableHostTickStatus.ContinuedAsNew(nextInput, _)) ->
                        nextInput = "1"
                    | _ -> false

                // Two more redundant restarts: checkpoint + receiver highwater
                // must absorb them without duplicating the start.
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
                    followed = DurableClientStatusRead.Succeeded(InstanceCompleted(WorkflowName "gen/eternal", "eternal-done"))

                let result =
                    { NextGenerationNotStartedAtKill = nextGenerationNotStartedAtKill
                      RestartTickReportedContinuedAsNew = restartTickReportedContinuedAsNew
                      NextGenerationCompleted = genOneValue = "eternal-done"
                      ExecutedOncePerGeneration = executions.Value = 2
                      GenOneJournalSingleCallAndStart = genOneCalls = 1 && genOneStarts = 1
                      GenZeroRolloverCommittedOnce = genZeroRollovers = 1 && genZeroDispatchCommands = 1
                      StatusFollowsChainToCompletion = statusFollowsChain }

                do!
                    ctx.EmitSpan
                        "proof.durable.continue-as-new.completed"
                        [ "proof.property", "durable.continue-as-new"
                          "rollover.executions", string executions.Value
                          "rollover.gen_one_calls", string genOneCalls
                          "rollover.gen_one_starts", string genOneStarts ]

                return result
            })

    let private rolloverProperty =
        property "durable.continue-as-new" {
            s2Lite ""
            workload rolloverWorkload

            verify (fun v ->
                [ v.Expect.Workload
                      "the rollover terminal is journal state: at the kill the next generation is not yet started"
                      (fun r -> r.NextGenerationNotStartedAtKill)
                  v.Expect.Workload "a fresh host reports the terminal ContinuedAsNew and dispatches the handoff" (fun r ->
                      r.RestartTickReportedContinuedAsNew)
                  v.Expect.Workload "generation N+1 completes with the carried state" (fun r ->
                      r.NextGenerationCompleted)
                  v.Expect.Workload "the step executed exactly once per generation (gen N journal not re-replayed)" (fun r ->
                      r.ExecutedOncePerGeneration)
                  v.Expect.Workload
                      "generation N+1 has a fresh journal: one start, one step call, despite redundant restarts"
                      (fun r -> r.GenOneJournalSingleCallAndStart)
                  v.Expect.Workload "generation N committed exactly one rollover record and one dispatch command" (fun r ->
                      r.GenZeroRolloverCommittedOnce)
                  v.Expect.Workload "status follows the generation chain to the live generation's result" (fun r ->
                      r.StatusFollowsChainToCompletion)
                  v.Trace.Operation
                      "continue-as-new operation recorded"
                      ({ TraceOperationMatch.named "durable.continue-as-new" with
                          Status = Some "ok"
                          OutputContains = [ "NextGenerationCompleted"; "ExecutedOncePerGeneration" ]
                          Count = Some 1 }) ])
        }

    // ---- durable.child-workflow --------------------------------------------

    type ChildWorkflowResult =
        { ChildNotStartedAtParentKill: bool
          ParentParkedOnChildAfterRestart: bool
          ChildStartedWithParentBinding: bool
          ResultNotDeliveredAtChildKill: bool
          DeliveryCommandCommittedOnce: bool
          ResultDeliveredExactlyOnce: bool
          ParentCompletedWithChildResult: bool
          ChildStepExecutedOnce: bool
          ParentJournalSingleChildCompletion: bool }

    let private childWorkload ctx =
        ProofOperation.run
            ctx
            "durable.child-workflow"
            "durable-child-workflow"
            { ProofOperationOptions.empty with
                Key = Some "durable-child-workflow" }
            (async {
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

                let tick = tickInstance basin workflows activities
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
                    Mailbox.runOnce StepRecordCodec.encode StepRecordCodec.decode MailboxEnvelopeCodec.decode 100 parentOwnedA

                let! _ =
                    DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now parentOwnedA (parentFactory "job")

                let! childAtParentKill = status child

                let childNotStartedAtParentKill =
                    childAtParentKill = DurableClientStatusRead.Succeeded InstanceNotFound

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
                // below, once the child has ticked).
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
                // admit/step/activity only, then commit the terminal delivery
                // command — and die before dispatching it.
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

                let resultNotDeliveredAtChildKill = ensuredCommitted && completionsAtChildKill = 0

                // Child restart: a fresh claim re-ensures (idempotent — no
                // second command) and a full tick dispatches the delivery.
                let! childOwnedB = S2Substrate.claim "chost-b" childPair

                let! reEnsured =
                    ChildResultAdapter.ensureCommand StepRecordCodec.encode StepRecordCodec.decode childOwnedB childValue

                let! _ = tick "chost-c" child
                let! _ = tick "chost-d" child

                let! childRecordsFinal = journalRecords basin child

                let deliveryCommands =
                    childRecordsFinal
                    |> countRecords (function
                        | Outgoing(Command(DeliverChildResult _)) -> true
                        | _ -> false)

                let deliveryCommandCommittedOnce =
                    reEnsured = ChildResultCommandStatus.AlreadyCommitted && deliveryCommands = 1

                let! parentInboxFinal = inboxMessages basin parent

                let completionsDelivered =
                    parentInboxFinal
                    |> List.filter (fun envelope ->
                        match envelope.Message with
                        | CompleteChild _ -> true
                        | _ -> false)
                    |> List.length

                // Parent (fresh host again) resumes from the delivered result.
                let rec driveParentToCompletion remaining =
                    async {
                        if remaining = 0 then
                            return failwith "parent never completed"
                        else
                            let! _ = tick "phost-e" parent
                            let! current = status parent

                            match current with
                            | DurableClientStatusRead.Succeeded(InstanceCompleted(_, value)) -> return value
                            | _ -> return! driveParentToCompletion (remaining - 1)
                    }

                let! parentValue = driveParentToCompletion 20

                let! parentRecordsFinal = journalRecords basin parent

                let childCompletions =
                    parentRecordsFinal
                    |> countRecords (function
                        | Incoming(HistoryEvent(ChildWorkflowCompleted _)) -> true
                        | _ -> false)

                // The parent binding was admitted into the child journal, and
                // the parent's later ticks (checkpointed) never re-dispatched
                // the start.
                let childBinding =
                    childRecordsFinal
                    |> countRecords (function
                        | Incoming(WorkflowParent(boundParent, OpId 0)) -> boundParent = InstanceId.value parent
                        | _ -> false)

                let! childInboxFinal = inboxMessages basin child

                let childStartedWithParentBinding =
                    childStartDispatchedOnce
                    && childBinding = 1
                    && childStartEnvelopes childInboxFinal = 1

                let result =
                    { ChildNotStartedAtParentKill = childNotStartedAtParentKill
                      ParentParkedOnChildAfterRestart = parentParked
                      ChildStartedWithParentBinding = childStartedWithParentBinding
                      ResultNotDeliveredAtChildKill = resultNotDeliveredAtChildKill
                      DeliveryCommandCommittedOnce = deliveryCommandCommittedOnce
                      ResultDeliveredExactlyOnce = completionsDelivered = 1
                      ParentCompletedWithChildResult = parentValue = "p<c<w:job:c>>"
                      ChildStepExecutedOnce = workExecutions.Value = 1
                      ParentJournalSingleChildCompletion = childCompletions = 1 }

                do!
                    ctx.EmitSpan
                        "proof.durable.child-workflow.completed"
                        [ "proof.property", "durable.child-workflow"
                          "child.parent_value", parentValue
                          "child.work_executions", string workExecutions.Value
                          "child.completions_delivered", string completionsDelivered ]

                return result
            })

    let private childProperty =
        property "durable.child-workflow" {
            s2Lite ""
            workload childWorkload

            verify (fun v ->
                [ v.Expect.Workload
                      "the journaled child start survives a parent kill before dispatch (child not yet started)"
                      (fun r -> r.ChildNotStartedAtParentKill)
                  v.Expect.Workload "after the parent restart the parent parks durably on the child" (fun r ->
                      r.ParentParkedOnChildAfterRestart)
                  v.Expect.Workload "the restarted parent dispatches the child with its parent binding recorded" (fun r ->
                      r.ChildStartedWithParentBinding)
                  v.Expect.Workload
                      "the terminal delivery command survives a child kill before dispatch (result not yet delivered)"
                      (fun r -> r.ResultNotDeliveredAtChildKill)
                  v.Expect.Workload "re-ensuring after the child restart commits no second delivery command" (fun r ->
                      r.DeliveryCommandCommittedOnce)
                  v.Expect.Workload "the child's terminal result is delivered to the parent exactly once" (fun r ->
                      r.ResultDeliveredExactlyOnce)
                  v.Expect.Workload "the parent completes with the child's composed result across all kills" (fun r ->
                      r.ParentCompletedWithChildResult)
                  v.Expect.Workload "the child's step executed exactly once" (fun r -> r.ChildStepExecutedOnce)
                  v.Expect.Workload "the parent journal holds exactly one child completion event" (fun r ->
                      r.ParentJournalSingleChildCompletion)
                  v.Trace.Operation
                      "child-workflow operation recorded"
                      ({ TraceOperationMatch.named "durable.child-workflow" with
                          Status = Some "ok"
                          OutputContains = [ "ParentCompletedWithChildResult"; "ResultDeliveredExactlyOnce" ]
                          Count = Some 1 }) ])
        }

    // ---- durable.one-way-send ----------------------------------------------

    type OneWaySendResult =
        { CallerAdvancedPastSendBeforeExecution: bool
          SendJournaledWithoutCompletionEvent: bool
          WorkflowCompletedAfterRestart: bool
          SendExecutedExactlyOnceAcrossRestart: bool
          AckGuardsReExecutionAfterKill: bool
          ThrowingSendDoesNotSurface: bool }

    let private oneWayWorkload ctx =
        ProofOperation.run
            ctx
            "durable.one-way-send"
            "durable-one-way-send"
            { ProofOperationOptions.empty with
                Key = Some "durable-one-way-send" }
            (async {
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
                    |> Result.bind (
                        ActivityRegistry.register "audit/boom" (fun _ -> async { return failwith "boom" })
                    )
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

                let tick = tickInstance basin workflows activities
                let status = DurableClient.getStatusWith basin workflows

                // --- Scenario 1: the caller never awaits; delivery is
                // exactly-once-effective across a host restart.
                let sender = InstanceId.create ("sender-" + suffix)
                let! started = DurableClient.startWith basin sender (WorkflowName "flow/sender") "job"
                startOk started

                let senderKey = DurableClient.instanceKey sender
                do! S2Substrate.ensureStreams basin senderKey
                let senderPair = S2Substrate.streams basin senderKey
                let! senderOwnedA = S2Substrate.claim "shost-a" senderPair

                let! _ =
                    Mailbox.runOnce StepRecordCodec.encode StepRecordCodec.decode MailboxEnvelopeCodec.decode 100 senderOwnedA

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

                // "Kill" host A; a fresh host completes the workflow.
                let rec driveToCompletion hostId instanceId remaining =
                    async {
                        if remaining = 0 then
                            return failwith "one-way: workflow never completed"
                        else
                            let! _ = tick hostId instanceId
                            let! current = status instanceId

                            match current with
                            | DurableClientStatusRead.Succeeded(InstanceCompleted(_, value)) -> return value
                            | _ -> return! driveToCompletion hostId instanceId (remaining - 1)
                    }

                let! senderValue = driveToCompletion "shost-b" sender 20
                let executionsAfterComplete = auditExecutions.Value

                // Redundant restart tick: checkpoint + ack dedupe hold.
                let! _ = tick "shost-c" sender
                let executionsAfterRestart = auditExecutions.Value

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

                // --- Scenario 2: the ack (published before the checkpoint)
                // guards re-execution — the exact kill-between-ack-and-
                // checkpoint window. A pre-acked send command is skipped.
                let acked = InstanceId.create ("acked-" + suffix)
                let! ackedStart = DurableClient.startWith basin acked (WorkflowName "flow/sender") "job2"
                startOk ackedStart

                let ackedKey = DurableClient.instanceKey acked
                do! S2Substrate.ensureStreams basin ackedKey
                let ackedPair = S2Substrate.streams basin ackedKey
                let! ackedOwned = S2Substrate.claim "ahost-a" ackedPair

                let! _ =
                    Mailbox.runOnce StepRecordCodec.encode StepRecordCodec.decode MailboxEnvelopeCodec.decode 100 ackedOwned

                let! _ =
                    DurableHost.stepOnce StepRecordCodec.encode StepRecordCodec.decode now ackedOwned (senderFactory "job2")

                let! ackedRecords = journalRecords basin acked

                let sendCommandSeq =
                    ackedRecords
                    |> List.tryPick (fun (seqNum, entry) ->
                        match entry with
                        | Ok(Outgoing(Command(SendActivity _))) -> Some seqNum
                        | _ -> None)

                match sendCommandSeq with
                | None -> return failwith "one-way: no SendActivity command journaled"
                | Some commandSeq ->
                    // Simulate the killed host's last act: the ack landed in
                    // the inbox, the checkpoint did not.
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
                            report.AlreadySent = 1
                            && auditExecutions.Value = executionsBeforeAdapter
                        | _ -> false

                    // --- Scenario 3: a throwing send handler never surfaces —
                    // the workflow completes anyway.
                    let faulty = InstanceId.create ("faulty-" + suffix)
                    let! faultyStart = DurableClient.startWith basin faulty (WorkflowName "flow/faulty") "job3"
                    startOk faultyStart

                    let! faultyValue = driveToCompletion "fhost-a" faulty 20

                    let result =
                        { CallerAdvancedPastSendBeforeExecution = callerAdvanced
                          SendJournaledWithoutCompletionEvent = sentEvents = 1 && sendCompletionEvents = 0
                          WorkflowCompletedAfterRestart = senderValue = "s<m:job>"
                          SendExecutedExactlyOnceAcrossRestart =
                            executionsAfterComplete = 1 && executionsAfterRestart = 1
                          AckGuardsReExecutionAfterKill = ackGuarded
                          ThrowingSendDoesNotSurface = faultyValue = "f<m:job3>" }

                    do!
                        ctx.EmitSpan
                            "proof.durable.one-way-send.completed"
                            [ "proof.property", "durable.one-way-send"
                              "send.executions", string auditExecutions.Value
                              "send.sender_value", senderValue
                              "send.faulty_value", faultyValue ]

                    return result
            })

    let private oneWayProperty =
        property "durable.one-way-send" {
            s2Lite ""
            workload oneWayWorkload

            verify (fun v ->
                [ v.Expect.Workload
                      "the caller advances past the journaled send before the handler ever executes (no await)"
                      (fun r -> r.CallerAdvancedPastSendBeforeExecution)
                  v.Expect.Workload "the send is journaled once and never produces a completion event" (fun r ->
                      r.SendJournaledWithoutCompletionEvent)
                  v.Expect.Workload "the workflow completes after a host kill and restart" (fun r ->
                      r.WorkflowCompletedAfterRestart)
                  v.Expect.Workload "the send handler executed exactly once across the restart" (fun r ->
                      r.SendExecutedExactlyOnceAcrossRestart)
                  v.Expect.Workload
                      "a published ack guards re-execution across a kill between ack and checkpoint"
                      (fun r -> r.AckGuardsReExecutionAfterKill)
                  v.Expect.Workload "a throwing send handler is swallowed: the calling workflow still completes" (fun r ->
                      r.ThrowingSendDoesNotSurface)
                  v.Trace.Operation
                      "one-way-send operation recorded"
                      ({ TraceOperationMatch.named "durable.one-way-send" with
                          Status = Some "ok"
                          OutputContains = [ "SendExecutedExactlyOnceAcrossRestart"; "ThrowingSendDoesNotSurface" ]
                          Count = Some 1 }) ])
        }

    let proof =
        proof "foundation.durable-debts" {
            describedAs
                "K1 kernel-debt primitives over the public kernel surface: ContinueAsNew generation rollover across a host kill (terminal record + deduped next-generation dispatch; fresh journal per generation; chain-following status), child workflows across both-side kills (journaled start, WorkflowParent binding, exactly-once-effective terminal result delivery), and one-way sends (journaled fire-and-forget, exactly-once-effective delivery, swallowed handler failures)."

            property rolloverProperty
            property childProperty
            property oneWayProperty
        }
