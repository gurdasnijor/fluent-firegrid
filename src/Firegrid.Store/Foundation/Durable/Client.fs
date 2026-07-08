namespace Firegrid.Store.Foundation.Durable

type DurableClientStartAck =
    { InstanceId: InstanceId
      MailboxSeqNum: int64 }

type DurableClientSignalAck =
    { InstanceId: InstanceId
      MailboxSeqNum: int64
      Source: string
      SignalName: string
      SourceSeqNum: int64 }

type DurableInstanceStatus =
    | InstanceNotFound
    | InstanceRunning of WorkflowName
    | InstanceWaiting of WorkflowName * OpId * Need
    | InstanceCompleted of WorkflowName * Payload
    | InstanceContinuedAsNew of WorkflowName * nextInput: Payload * next: InstanceId

[<RequireQualifiedAccess>]
type DurableClientFailure =
    | StartAppendFailed of string
    | SignalAppendFailed of string

[<RequireQualifiedAccess>]
type DurableClientStatusFailure =
    | StatusReadFailed of string
    | StatusDecodeFailed of seqNum: int64 * error: string
    | WorkflowNotFound of WorkflowName

[<RequireQualifiedAccess>]
type DurableClientStartStatus =
    | Accepted of DurableClientStartAck
    | Failed of DurableClientFailure

[<RequireQualifiedAccess>]
type DurableClientSignalStatus =
    | Accepted of DurableClientSignalAck
    | Failed of DurableClientFailure

[<RequireQualifiedAccess>]
type DurableClientStatusRead =
    | Succeeded of DurableInstanceStatus
    | Failed of DurableClientStatusFailure

[<RequireQualifiedAccess>]
module DurableClient =
    let private startSource = "client:start"
    let private signalSource = "client:signal"

    let instanceKey instanceId = StorageKey(InstanceId.value instanceId)

    let private appendStart basin instanceId message =
        async {
            try
                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let! ack = S2Substrate.appendMailboxText [ "kind", "start" ] (MailboxEnvelopeCodec.encode message) pair

                return
                    DurableClientStartStatus.Accepted
                        { InstanceId = instanceId
                          MailboxSeqNum = ack.Start.SeqNum }
            with error ->
                return DurableClientStartStatus.Failed(DurableClientFailure.StartAppendFailed error.Message)
        }

    /// Append a StartWorkflow message with explicit provenance — the receiving
    /// mailbox dedupes on (source, sourceSeqNum), so redelivery is harmless.
    let startFrom basin instanceId source sourceSeqNum workflowName input =
        appendStart
            basin
            instanceId
            { Source = source
              SourceSeqNum = sourceSeqNum
              Message = StartWorkflow(workflowName, input) }

    /// Append a child StartWorkflow carrying the parent binding; the child's
    /// journal records WorkflowParent so its terminal result can be delivered.
    let startChildFrom basin instanceId source sourceSeqNum workflowName input parent parentOpId =
        appendStart
            basin
            instanceId
            { Source = source
              SourceSeqNum = sourceSeqNum
              Message = StartChildWorkflow(workflowName, input, parent, parentOpId) }

    let startWith basin instanceId workflowName input =
        startFrom basin instanceId startSource 0L workflowName input

    let raiseSignalFrom basin instanceId source sourceSeqNum name payload =
        async {
            try
                if System.String.IsNullOrWhiteSpace source then
                    invalidArg (nameof source) "source must be non-empty"

                if sourceSeqNum < 0L then
                    invalidArg (nameof sourceSeqNum) "sourceSeqNum must be non-negative"

                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let envelope =
                    { Source = source
                      SourceSeqNum = sourceSeqNum
                      Message = RaiseSignal(name, payload) }

                let! ack =
                    S2Substrate.appendMailboxText
                        [ "kind", "signal"; "name", name ]
                        (MailboxEnvelopeCodec.encode envelope)
                        pair

                return
                    DurableClientSignalStatus.Accepted
                        { InstanceId = instanceId
                          MailboxSeqNum = ack.Start.SeqNum
                          Source = source
                          SignalName = name
                          SourceSeqNum = sourceSeqNum }
            with error ->
                return DurableClientSignalStatus.Failed(DurableClientFailure.SignalAppendFailed error.Message)
        }

    let raiseSignalWith basin instanceId sourceSeqNum name payload =
        raiseSignalFrom basin instanceId signalSource sourceSeqNum name payload

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (_, Ok record) :: rest -> loop (record :: records) rest
            | (seqNum, Error error) :: _ -> Error(DurableClientStatusFailure.StatusDecodeFailed(seqNum, error))

        loop [] decoded

    let private startedWorkflow records =
        records
        |> List.tryPick (function
            | Incoming(WorkflowStarted(name, input)) -> Some(name, input)
            | _ -> None)

    let getStatusWith basin workflows instanceId =
        async {
            try
                let key = instanceKey instanceId

                do! S2Substrate.ensureStreams basin key

                let pair = S2Substrate.streams basin key

                let owned =
                    { Key = key
                      Fence = FenceToken "client:status"
                      Log = pair.Log
                      Inbox = pair.Inbox }

                let! decoded = S2Substrate.readLogText StepRecordCodec.decode owned

                match decodeLog decoded with
                | Error failure -> return DurableClientStatusRead.Failed failure
                | Ok records ->
                    match startedWorkflow records with
                    | None -> return DurableClientStatusRead.Succeeded InstanceNotFound
                    | Some(workflowName, input) ->
                        match WorkflowRegistry.require (WorkflowName.value workflowName) workflows with
                        | Error(DurableRegistryError.WorkflowNotFound missing) ->
                            return DurableClientStatusRead.Failed(DurableClientStatusFailure.WorkflowNotFound missing)
                        | Error error ->
                            return
                                DurableClientStatusRead.Failed(
                                    DurableClientStatusFailure.StatusReadFailed(string error)
                                )
                        | Ok factory ->
                            let history = DurableStepper.historyFromRecords records

                            return
                                match DurableStepper.plan 0L history (factory input) with
                                | Complete value ->
                                    DurableClientStatusRead.Succeeded(InstanceCompleted(workflowName, value))
                                | Continued nextInput ->
                                    DurableClientStatusRead.Succeeded(
                                        InstanceContinuedAsNew(workflowName, nextInput, Generation.next instanceId)
                                    )
                                | Waiting(opId, need) ->
                                    DurableClientStatusRead.Succeeded(InstanceWaiting(workflowName, opId, need))
                                | Commit _ -> DurableClientStatusRead.Succeeded(InstanceRunning workflowName)
            with error ->
                return DurableClientStatusRead.Failed(DurableClientStatusFailure.StatusReadFailed error.Message)
        }

    /// Status following the ContinueAsNew chain: hops generation pointers until
    /// the live (non-continued) generation and reports its status. Terminates
    /// because generation numbers strictly increase and an undispatched next
    /// generation reads as InstanceNotFound.
    let getStatusFollowingWith basin workflows instanceId =
        let rec follow current =
            async {
                let! status = getStatusWith basin workflows current

                match status with
                | DurableClientStatusRead.Succeeded(InstanceContinuedAsNew(_, _, next)) -> return! follow next
                | other -> return other
            }

        follow instanceId
