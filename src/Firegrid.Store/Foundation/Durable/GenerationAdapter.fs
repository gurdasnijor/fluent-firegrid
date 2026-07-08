namespace Firegrid.Store.Foundation.Durable

type GenerationDispatch =
    { SourceSeqNum: int64
      MailboxSeqNum: int64
      OpId: OpId
      Next: InstanceId
      NextInput: Payload }

type GenerationAdapterReport =
    { Batch: DispatchBatch
      Dispatched: GenerationDispatch list
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type GenerationAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | MissingWorkflowStart
    | StartAppendFailed of string
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type GenerationAdapterStatus =
    | Processed of GenerationAdapterReport
    | Deposed of expectedFence: string
    | Failed of GenerationAdapterFailure

/// The durable half of ContinueAsNew: scans the rolled-over journal for
/// StartNextGeneration commands beyond the dispatcher checkpoint and appends a
/// deduped StartWorkflow to the next generation's inbox (a fresh instance, so
/// the prior journal is never replayed). Provenance = the command's log
/// seqNum; the next generation's mailbox highwater absorbs redelivery, and a
/// duplicate start is further absorbed by first-WorkflowStarted-wins.
[<RequireQualifiedAccess>]
module GenerationAdapter =
    let dispatcher = "generation"

    let startSource (StorageKey key) = "generation:" + key

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(GenerationAdapterFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(GenerationAdapterFailure.LogReadFailed error.Message)
        }

    let private startedWorkflow decoded =
        decoded
        |> List.tryPick (function
            | _, Incoming(WorkflowStarted(name, _)) -> Some name
            | _ -> None)

    let runOnce encode decode maxRecords basin (owned: OwnedKey) =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return GenerationAdapterStatus.Failed failure
            | Ok decoded ->
                let batch = DurableCommandDispatch.selectFromDecoded dispatcher maxRecords decoded
                let commands = DispatchBatch.commands batch
                let current = InstanceId.create (StorageKey.value owned.Key)

                let rec loop dispatched ignored remaining =
                    async {
                        match remaining with
                        | [] ->
                            let! checkpoint = DurableCommandDispatch.checkpoint encode dispatcher owned batch

                            return
                                match checkpoint with
                                | CommandDispatchCheckpointResult.Deposed expected ->
                                    GenerationAdapterStatus.Deposed expected
                                | CommandDispatchCheckpointResult.Failed failure ->
                                    GenerationAdapterStatus.Failed(GenerationAdapterFailure.CheckpointFailed failure)
                                | CommandDispatchCheckpointResult.Checkpointed _
                                | CommandDispatchCheckpointResult.NotRequired ->
                                    GenerationAdapterStatus.Processed
                                        { Batch = batch
                                          Dispatched = List.rev dispatched
                                          Ignored = ignored
                                          Checkpoint = checkpoint }
                        | command :: rest ->
                            match command.Command with
                            | StartNextGeneration(opId, nextInput) ->
                                match startedWorkflow decoded with
                                | None -> return GenerationAdapterStatus.Failed GenerationAdapterFailure.MissingWorkflowStart
                                | Some workflowName ->
                                    let next = Generation.next current

                                    let! start =
                                        DurableClient.startFrom
                                            basin
                                            next
                                            (startSource owned.Key)
                                            command.SourceSeqNum
                                            workflowName
                                            nextInput

                                    match start with
                                    | DurableClientStartStatus.Failed(DurableClientFailure.StartAppendFailed error)
                                    | DurableClientStartStatus.Failed(DurableClientFailure.SignalAppendFailed error) ->
                                        return
                                            GenerationAdapterStatus.Failed(
                                                GenerationAdapterFailure.StartAppendFailed error
                                            )
                                    | DurableClientStartStatus.Accepted ack ->
                                        let dispatch =
                                            { SourceSeqNum = command.SourceSeqNum
                                              MailboxSeqNum = ack.MailboxSeqNum
                                              OpId = opId
                                              Next = next
                                              NextInput = nextInput }

                                        return! loop (dispatch :: dispatched) ignored rest
                            | CallActivity _
                            | ScheduleTimer _
                            | CancelTimer _
                            | WriteLog _
                            | SendActivity _
                            | CallChildWorkflow _
                            | DeliverChildResult _ -> return! loop dispatched (ignored + 1) rest
                    }

                return! loop [] 0 commands
        }
