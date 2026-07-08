namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log

type ActivityCommandCompletion =
    { SourceSeqNum: int64
      MailboxSeqNum: int64
      OpId: OpId
      Activity: Activity
      Value: Payload }

/// A processed one-way send. Failures are swallowed by design (fire-and-forget
/// never surfaces to the calling workflow); they are reported here only.
type ActivityCommandSend =
    { SourceSeqNum: int64
      MailboxSeqNum: int64
      OpId: OpId
      Activity: Activity
      Failure: string option }

type ActivityCommandAdapterReport =
    { Batch: DispatchBatch
      Completed: ActivityCommandCompletion list
      Sent: ActivityCommandSend list
      AlreadyCompleted: int
      AlreadyPublished: int
      AlreadySent: int
      Ignored: int
      Checkpoint: CommandDispatchCheckpointResult }

[<RequireQualifiedAccess>]
type ActivityCommandAdapterFailure =
    | LogReadFailed of string
    | DecodeFailed of seqNum: int64 * error: string
    | MailboxReadFailed of string
    | MailboxDecodeFailed of seqNum: int64 * error: string
    | MissingHandler of ActivityName
    | HandlerFailed of ActivityName * error: string
    | CompletionPublishFailed of string
    | CheckpointFailed of CommandDispatchFailure

[<RequireQualifiedAccess>]
type ActivityCommandAdapterStatus =
    | Processed of ActivityCommandAdapterReport
    | Deposed of expectedFence: string
    | Failed of ActivityCommandAdapterFailure

[<RequireQualifiedAccess>]
module ActivityCommandAdapter =
    let dispatcher = "activity"

    let completionSource = "activity"

    let sendAckSource = "send"

    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(ActivityCommandAdapterFailure.DecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(ActivityCommandAdapterFailure.LogReadFailed error.Message)
        }

    let private invokeHandler activity handler =
        async {
            try
                let! result = handler activity.Input
                return Ok result
            with error ->
                return Error(ActivityCommandAdapterFailure.HandlerFailed(ActivityName activity.Name, error.Message))
        }

    let private readPublishedCompletions (decode: string -> Result<MailboxEnvelope, string>) owned =
        async {
            let pageSize = 100

            let rec readPage from seen =
                async {
                    try
                        let! records = S2Substrate.readMailbox from pageSize owned

                        let rec decodeRecords nextSeqNum state =
                            function
                            | [] -> Ok(nextSeqNum, state)
                            | (record: S2.ReadRecord) :: rest ->
                                match decode record.Body with
                                | Ok(envelope: MailboxEnvelope) ->
                                    let completions, acks = state

                                    let state =
                                        match envelope.Message with
                                        | CompleteActivity _ when envelope.Source = completionSource ->
                                            completions |> Set.add envelope.SourceSeqNum, acks
                                        | AckSend _ when envelope.Source = sendAckSource ->
                                            completions, acks |> Set.add envelope.SourceSeqNum
                                        | StartWorkflow _
                                        | RaiseSignal _
                                        | CompleteActivity _
                                        | FireTimer _
                                        | StartChildWorkflow _
                                        | CompleteChild _
                                        | AckSend _ -> state

                                    decodeRecords (record.SeqNum + 1L) state rest
                                | Error error ->
                                    Error(ActivityCommandAdapterFailure.MailboxDecodeFailed(record.SeqNum, error))

                        match decodeRecords from seen records with
                        | Error failure -> return Error failure
                        | Ok(nextSeqNum, state) ->
                            if List.isEmpty records then
                                return Ok state
                            else
                                return! readPage nextSeqNum state
                    with error ->
                        match S2Errors.classify error with
                        | S2Errors.RangeNotSatisfiable _ -> return Ok seen
                        | _ -> return Error(ActivityCommandAdapterFailure.MailboxReadFailed error.Message)
                }

            return! readPage 0L (Set.empty, Set.empty)
        }

    let private publishCompletion owned sourceSeqNum opId value =
        async {
            let envelope: MailboxEnvelope =
                { Source = completionSource
                  SourceSeqNum = sourceSeqNum
                  Message = CompleteActivity(opId, value) }

            try
                let! ack =
                    owned.Inbox
                    |> S2.append
                        [ S2.Record.textWith
                              [ "src", completionSource; "seq", string sourceSeqNum ]
                              (MailboxEnvelopeCodec.encode envelope) ]

                return Ok(ack, envelope)
            with error ->
                return
                    Error(
                        ActivityCommandAdapterStatus.Failed(
                            ActivityCommandAdapterFailure.CompletionPublishFailed error.Message
                        )
                    )
        }

    let private publishSendAck owned sourceSeqNum opId =
        async {
            let envelope: MailboxEnvelope =
                { Source = sendAckSource
                  SourceSeqNum = sourceSeqNum
                  Message = AckSend opId }

            try
                let! ack =
                    owned.Inbox
                    |> S2.append
                        [ S2.Record.textWith
                              [ "src", sendAckSource; "seq", string sourceSeqNum ]
                              (MailboxEnvelopeCodec.encode envelope) ]

                return Ok ack
            with error ->
                return
                    Error(
                        ActivityCommandAdapterStatus.Failed(
                            ActivityCommandAdapterFailure.CompletionPublishFailed error.Message
                        )
                    )
        }

    let runOnce encode decode maxRecords registry owned =
        async {
            let! log = readLog decode owned

            match log with
            | Error failure -> return ActivityCommandAdapterStatus.Failed failure
            | Ok decoded ->
                let! published = readPublishedCompletions MailboxEnvelopeCodec.decode owned

                match published with
                | Error failure -> return ActivityCommandAdapterStatus.Failed failure
                | Ok(publishedSourceSeqNums, ackedSourceSeqNums) ->
                    let batch = DurableCommandDispatch.selectFromDecoded dispatcher maxRecords decoded
                    let history = decoded |> List.map snd |> DurableStepper.historyFromRecords
                    let commands = DispatchBatch.commands batch

                    let rec loop state remaining =
                        async {
                            let published, acked, completed, sent, alreadyCompleted, alreadyPublished, alreadySent, ignored =
                                state

                            match remaining with
                            | [] ->
                                let! checkpoint = DurableCommandDispatch.checkpoint encode dispatcher owned batch

                                return
                                    match checkpoint with
                                    | CommandDispatchCheckpointResult.Deposed expected ->
                                        ActivityCommandAdapterStatus.Deposed expected
                                    | CommandDispatchCheckpointResult.Failed failure ->
                                        ActivityCommandAdapterStatus.Failed(
                                            ActivityCommandAdapterFailure.CheckpointFailed failure
                                        )
                                    | CommandDispatchCheckpointResult.Checkpointed _
                                    | CommandDispatchCheckpointResult.NotRequired ->
                                        ActivityCommandAdapterStatus.Processed
                                            { Batch = batch
                                              Completed = List.rev completed
                                              Sent = List.rev sent
                                              AlreadyCompleted = alreadyCompleted
                                              AlreadyPublished = alreadyPublished
                                              AlreadySent = alreadySent
                                              Ignored = ignored
                                              Checkpoint = checkpoint }
                            | command :: rest ->
                                match command.Command with
                                | CallActivity(opId, activity) ->
                                    if History.completed opId history |> Option.isSome then
                                        return!
                                            loop
                                                (published,
                                                 acked,
                                                 completed,
                                                 sent,
                                                 alreadyCompleted + 1,
                                                 alreadyPublished,
                                                 alreadySent,
                                                 ignored)
                                                rest
                                    elif published |> Set.contains command.SourceSeqNum then
                                        return!
                                            loop
                                                (published,
                                                 acked,
                                                 completed,
                                                 sent,
                                                 alreadyCompleted,
                                                 alreadyPublished + 1,
                                                 alreadySent,
                                                 ignored)
                                                rest
                                    else
                                        match ActivityRegistry.require activity.Name registry with
                                        | Error(DurableRegistryError.ActivityNotFound name) ->
                                            return
                                                ActivityCommandAdapterStatus.Failed(
                                                    ActivityCommandAdapterFailure.MissingHandler name
                                                )
                                        | Error error ->
                                            return
                                                ActivityCommandAdapterStatus.Failed(
                                                    ActivityCommandAdapterFailure.HandlerFailed(
                                                        ActivityName activity.Name,
                                                        string error
                                                    )
                                                )
                                        | Ok handler ->
                                            let! handled = invokeHandler activity handler

                                            match handled with
                                            | Error failure -> return ActivityCommandAdapterStatus.Failed failure
                                            | Ok value ->
                                                let! publishedCompletion =
                                                    publishCompletion owned command.SourceSeqNum opId value

                                                match publishedCompletion with
                                                | Error status -> return status
                                                | Ok(ack, _) ->
                                                    let completion =
                                                        { SourceSeqNum = command.SourceSeqNum
                                                          MailboxSeqNum = ack.Start.SeqNum
                                                          OpId = opId
                                                          Activity = activity
                                                          Value = value }

                                                    return!
                                                        loop
                                                            (published |> Set.add command.SourceSeqNum,
                                                             acked,
                                                             completion :: completed,
                                                             sent,
                                                             alreadyCompleted,
                                                             alreadyPublished,
                                                             alreadySent,
                                                             ignored)
                                                            rest
                                | SendActivity(opId, activity) ->
                                    // One-way send: execute at-least-once, ack to the own
                                    // inbox for the dedupe window, swallow handler failures
                                    // (fire-and-forget never surfaces to the workflow).
                                    if acked |> Set.contains command.SourceSeqNum then
                                        return!
                                            loop
                                                (published,
                                                 acked,
                                                 completed,
                                                 sent,
                                                 alreadyCompleted,
                                                 alreadyPublished,
                                                 alreadySent + 1,
                                                 ignored)
                                                rest
                                    else
                                        match ActivityRegistry.require activity.Name registry with
                                        | Error(DurableRegistryError.ActivityNotFound name) ->
                                            return
                                                ActivityCommandAdapterStatus.Failed(
                                                    ActivityCommandAdapterFailure.MissingHandler name
                                                )
                                        | Error error ->
                                            return
                                                ActivityCommandAdapterStatus.Failed(
                                                    ActivityCommandAdapterFailure.HandlerFailed(
                                                        ActivityName activity.Name,
                                                        string error
                                                    )
                                                )
                                        | Ok handler ->
                                            let! handled = invokeHandler activity handler

                                            let failure =
                                                match handled with
                                                | Ok _ -> None
                                                | Error(ActivityCommandAdapterFailure.HandlerFailed(_, message)) ->
                                                    Some message
                                                | Error other -> Some(string other)

                                            let! publishedAck = publishSendAck owned command.SourceSeqNum opId

                                            match publishedAck with
                                            | Error status -> return status
                                            | Ok ack ->
                                                let delivery =
                                                    { SourceSeqNum = command.SourceSeqNum
                                                      MailboxSeqNum = ack.Start.SeqNum
                                                      OpId = opId
                                                      Activity = activity
                                                      Failure = failure }

                                                return!
                                                    loop
                                                        (published,
                                                         acked |> Set.add command.SourceSeqNum,
                                                         completed,
                                                         delivery :: sent,
                                                         alreadyCompleted,
                                                         alreadyPublished,
                                                         alreadySent,
                                                         ignored)
                                                        rest
                                | ScheduleTimer _
                                | CancelTimer _
                                | WriteLog _
                                | CallChildWorkflow _
                                | DeliverChildResult _
                                | StartNextGeneration _ ->
                                    return!
                                        loop
                                            (published,
                                             acked,
                                             completed,
                                             sent,
                                             alreadyCompleted,
                                             alreadyPublished,
                                             alreadySent,
                                             ignored + 1)
                                            rest
                        }

                    return! loop (publishedSourceSeqNums, ackedSourceSeqNums, [], [], 0, 0, 0, 0) commands
        }
