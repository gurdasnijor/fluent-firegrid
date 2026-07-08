namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log

type ChildStartDispatch =
    { SourceSeqNum: int64
      MailboxSeqNum: int64
      OpId: OpId
      Child: InstanceId
      Workflow: WorkflowName
      Input: Payload }

type ChildStartAdapterReport =
    { Batch: DispatchBatch
      Dispatched: ChildStartDispatch list
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type ChildStartAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | StartAppendFailed of string
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type ChildStartAdapterStatus =
    | Processed of ChildStartAdapterReport
    | Deposed of expectedFence: string
    | Failed of ChildStartAdapterFailure

type ChildResultDispatch =
    { SourceSeqNum: int64
      MailboxSeqNum: int64
      Parent: InstanceId
      ParentOpId: OpId
      Output: Payload }

type ChildResultAdapterReport =
    { Batch: DispatchBatch
      Delivered: ChildResultDispatch list
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type ChildResultAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | ResultAppendFailed of string
    | CommandCommitFailed of S2Errors.S2Failure
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type ChildResultAdapterStatus =
    | Processed of ChildResultAdapterReport
    | Deposed of expectedFence: string
    | Failed of ChildResultAdapterFailure

/// Outcome of ensuring the terminal DeliverChildResult command exists in a
/// completed child's journal. NotChild = the instance has no parent binding.
[<RequireQualifiedAccess>]
type ChildResultCommandStatus =
    | NotChild
    | AlreadyCommitted
    | Committed of S2.AppendAck
    | Deposed of expectedFence: string
    | Failed of ChildResultAdapterFailure

/// Parent side of the child-workflow primitive: scans the parent journal for
/// CallChildWorkflow commands beyond the dispatcher checkpoint and appends a
/// deduped StartChildWorkflow to the child's inbox (provenance = the command's
/// log seqNum, so the child's mailbox highwater absorbs redelivery).
[<RequireQualifiedAccess>]
module ChildStartAdapter =
    let dispatcher = "child-start"

    let startSource (StorageKey key) = "child-start:" + key

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(ChildStartAdapterFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(ChildStartAdapterFailure.LogReadFailed error.Message)
        }

    let runOnce encode decode maxRecords basin (owned: OwnedKey) =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return ChildStartAdapterStatus.Failed failure
            | Ok decoded ->
                let batch = DurableCommandDispatch.selectFromDecoded dispatcher maxRecords decoded
                let commands = DispatchBatch.commands batch
                let parent = InstanceId.create (StorageKey.value owned.Key)

                let rec loop dispatched ignored remaining =
                    async {
                        match remaining with
                        | [] ->
                            let! checkpoint = DurableCommandDispatch.checkpoint encode dispatcher owned batch

                            return
                                match checkpoint with
                                | CommandDispatchCheckpointResult.Deposed expected ->
                                    ChildStartAdapterStatus.Deposed expected
                                | CommandDispatchCheckpointResult.Failed failure ->
                                    ChildStartAdapterStatus.Failed(ChildStartAdapterFailure.CheckpointFailed failure)
                                | CommandDispatchCheckpointResult.Checkpointed _
                                | CommandDispatchCheckpointResult.NotRequired ->
                                    ChildStartAdapterStatus.Processed
                                        { Batch = batch
                                          Dispatched = List.rev dispatched
                                          Ignored = ignored
                                          Checkpoint = checkpoint }
                        | command :: rest ->
                            match command.Command with
                            | CallChildWorkflow(opId, workflow, input) ->
                                let child = ChildInstance.idFor parent opId
                                let workflowName = WorkflowName.create workflow

                                let! start =
                                    DurableClient.startChildFrom
                                        basin
                                        child
                                        (startSource owned.Key)
                                        command.SourceSeqNum
                                        workflowName
                                        input
                                        (InstanceId.value parent)
                                        opId

                                match start with
                                | DurableClientStartStatus.Failed(DurableClientFailure.StartAppendFailed error)
                                | DurableClientStartStatus.Failed(DurableClientFailure.SignalAppendFailed error) ->
                                    return
                                        ChildStartAdapterStatus.Failed(
                                            ChildStartAdapterFailure.StartAppendFailed error
                                        )
                                | DurableClientStartStatus.Accepted ack ->
                                    let dispatch =
                                        { SourceSeqNum = command.SourceSeqNum
                                          MailboxSeqNum = ack.MailboxSeqNum
                                          OpId = opId
                                          Child = child
                                          Workflow = workflowName
                                          Input = input }

                                    return! loop (dispatch :: dispatched) ignored rest
                            | CallActivity _
                            | ScheduleTimer _
                            | CancelTimer _
                            | WriteLog _
                            | SendActivity _
                            | DeliverChildResult _
                            | StartNextGeneration _ -> return! loop dispatched (ignored + 1) rest
                    }

                return! loop [] 0 commands
        }

/// Child side of the child-workflow primitive. `ensureCommand` commits the
/// terminal DeliverChildResult command exactly once (fenced) after the child
/// completes; `runOnce` dispatches pending commands to the parent's inbox with
/// provenance dedupe and checkpoints. A kill between the two is recovered by
/// the next tick re-running both halves idempotently.
[<RequireQualifiedAccess>]
module ChildResultAdapter =
    let dispatcher = "child-result"

    let resultSource (StorageKey key) = "child-result:" + key

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(ChildResultAdapterFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(ChildResultAdapterFailure.LogReadFailed error.Message)
        }

    let private parentBinding decoded =
        decoded
        |> List.tryPick (function
            | _, Incoming(WorkflowParent(parent, parentOpId)) -> Some(parent, parentOpId)
            | _ -> None)

    let private hasDeliveryCommand decoded =
        decoded
        |> List.exists (function
            | _, Outgoing(Command(DeliverChildResult _)) -> true
            | _ -> false)

    let ensureCommand encode decode (owned: OwnedKey) (output: Payload) =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return ChildResultCommandStatus.Failed failure
            | Ok decoded ->
                match parentBinding decoded with
                | None -> return ChildResultCommandStatus.NotChild
                | Some(parent, parentOpId) ->
                    if hasDeliveryCommand decoded then
                        return ChildResultCommandStatus.AlreadyCommitted
                    else
                        let records = [ Outgoing(Command(DeliverChildResult(parent, parentOpId, output))) ]
                        let! commit = S2Substrate.commitText encode records owned

                        return
                            match commit with
                            | Committed ack -> ChildResultCommandStatus.Committed ack
                            | Deposed expected -> ChildResultCommandStatus.Deposed expected
                            | CommitFailed failure ->
                                ChildResultCommandStatus.Failed(
                                    ChildResultAdapterFailure.CommandCommitFailed failure
                                )
        }

    let private appendResult basin (envelope: MailboxEnvelope) (parent: string) =
        async {
            let key = StorageKey parent

            try
                do! S2Substrate.ensureStreams basin key
                let pair = S2Substrate.streams basin key

                let! ack =
                    S2Substrate.appendMailboxText
                        [ "kind", "child-result"; "src", envelope.Source; "seq", string envelope.SourceSeqNum ]
                        (MailboxEnvelopeCodec.encode envelope)
                        pair

                return Ok ack
            with error ->
                return Error(ChildResultAdapterFailure.ResultAppendFailed error.Message)
        }

    let runOnce encode decode maxRecords basin (owned: OwnedKey) =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return ChildResultAdapterStatus.Failed failure
            | Ok decoded ->
                let batch = DurableCommandDispatch.selectFromDecoded dispatcher maxRecords decoded
                let commands = DispatchBatch.commands batch

                let rec loop delivered ignored remaining =
                    async {
                        match remaining with
                        | [] ->
                            let! checkpoint = DurableCommandDispatch.checkpoint encode dispatcher owned batch

                            return
                                match checkpoint with
                                | CommandDispatchCheckpointResult.Deposed expected ->
                                    ChildResultAdapterStatus.Deposed expected
                                | CommandDispatchCheckpointResult.Failed failure ->
                                    ChildResultAdapterStatus.Failed(ChildResultAdapterFailure.CheckpointFailed failure)
                                | CommandDispatchCheckpointResult.Checkpointed _
                                | CommandDispatchCheckpointResult.NotRequired ->
                                    ChildResultAdapterStatus.Processed
                                        { Batch = batch
                                          Delivered = List.rev delivered
                                          Ignored = ignored
                                          Checkpoint = checkpoint }
                        | command :: rest ->
                            match command.Command with
                            | DeliverChildResult(parent, parentOpId, output) ->
                                let envelope =
                                    { Source = resultSource owned.Key
                                      SourceSeqNum = command.SourceSeqNum
                                      Message = CompleteChild(parentOpId, output) }

                                let! appended = appendResult basin envelope parent

                                match appended with
                                | Error failure -> return ChildResultAdapterStatus.Failed failure
                                | Ok ack ->
                                    let dispatch =
                                        { SourceSeqNum = command.SourceSeqNum
                                          MailboxSeqNum = ack.Start.SeqNum
                                          Parent = InstanceId.create parent
                                          ParentOpId = parentOpId
                                          Output = output }

                                    return! loop (dispatch :: delivered) ignored rest
                            | CallActivity _
                            | ScheduleTimer _
                            | CancelTimer _
                            | WriteLog _
                            | SendActivity _
                            | CallChildWorkflow _
                            | StartNextGeneration _ -> return! loop delivered (ignored + 1) rest
                    }

                return! loop [] 0 commands
        }
