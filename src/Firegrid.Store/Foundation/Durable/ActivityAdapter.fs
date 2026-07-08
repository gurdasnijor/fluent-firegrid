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

    /// The mailbox admission contract (Mailbox highwater dedupe) requires each
    /// source to publish monotonically increasing SourceSeqNums. A concurrent
    /// batch publishes in completion order, not command order, so every
    /// command's completion rides its own source (one message per source keeps
    /// the contract trivially). Provenance is unchanged: SourceSeqNum is still
    /// the command's journal seqNum.
    let completionSourceFor (sourceSeqNum: int64) =
        completionSource + ":" + string sourceSeqNum

    /// Per-command source for one-way send acks; see completionSourceFor.
    let sendAckSourceFor (sourceSeqNum: int64) =
        sendAckSource + ":" + string sourceSeqNum

    /// Matches both the per-command sources written by concurrent batches and
    /// the legacy shared source written before concurrency, so inflight
    /// records from either era dedupe correctly.
    let private matchesSource (prefix: string) (source: string) =
        source = prefix || source.StartsWith(prefix + ":")

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
                                        | CompleteActivity _ when matchesSource completionSource envelope.Source ->
                                            completions |> Set.add envelope.SourceSeqNum, acks
                                        | AckSend _ when matchesSource sendAckSource envelope.Source ->
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
            let source = completionSourceFor sourceSeqNum

            let envelope: MailboxEnvelope =
                { Source = source
                  SourceSeqNum = sourceSeqNum
                  Message = CompleteActivity(opId, value) }

            try
                let! ack =
                    owned.Inbox
                    |> S2.append
                        [ S2.Record.textWith
                              [ "src", source; "seq", string sourceSeqNum ]
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

    let private publishSendAck owned sourceSeqNum opId =
        async {
            let source = sendAckSourceFor sourceSeqNum

            let envelope: MailboxEnvelope =
                { Source = source
                  SourceSeqNum = sourceSeqNum
                  Message = AckSend opId }

            try
                let! ack =
                    owned.Inbox
                    |> S2.append
                        [ S2.Record.textWith
                              [ "src", source; "seq", string sourceSeqNum ]
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

    /// A due command with its resolved handler: everything needed to execute
    /// it independently of its batch-mates.
    [<RequireQualifiedAccess>]
    type private Due =
        | Call of command: DispatchCommand * opId: OpId * activity: Activity * handler: ActivityHandler
        | Send of command: DispatchCommand * opId: OpId * activity: Activity * handler: ActivityHandler

    [<RequireQualifiedAccess>]
    type private Outcome =
        | Completed of ActivityCommandCompletion
        | Sent of ActivityCommandSend
        | Failed of ActivityCommandAdapterStatus

    /// Walk the batch in command order and split it into skip counters (the
    /// exactly-once dedupe: journal completions, published completions, and
    /// published send acks are all keyed by provenance, so classification
    /// against the sets read at batch start is order-independent) and due work
    /// items with their handlers resolved up front. A missing handler fails
    /// the batch before anything executes: a deployment error should wedge
    /// visibly and retry once the handler is registered.
    let private classify history published acked registry commands =
        let rec loop due alreadyCompleted alreadyPublished alreadySent ignored remaining =
            match remaining with
            | [] -> Ok(List.rev due, alreadyCompleted, alreadyPublished, alreadySent, ignored)
            | (command: DispatchCommand) :: rest ->
                match command.Command with
                | CallActivity(opId, activity) ->
                    if History.completed opId history |> Option.isSome then
                        loop due (alreadyCompleted + 1) alreadyPublished alreadySent ignored rest
                    elif published |> Set.contains command.SourceSeqNum then
                        loop due alreadyCompleted (alreadyPublished + 1) alreadySent ignored rest
                    else
                        match ActivityRegistry.require activity.Name registry with
                        | Error(DurableRegistryError.ActivityNotFound name) ->
                            Error(ActivityCommandAdapterFailure.MissingHandler name)
                        | Error error ->
                            Error(ActivityCommandAdapterFailure.HandlerFailed(ActivityName activity.Name, string error))
                        | Ok handler ->
                            loop
                                (Due.Call(command, opId, activity, handler) :: due)
                                alreadyCompleted
                                alreadyPublished
                                alreadySent
                                ignored
                                rest
                | SendActivity(opId, activity) ->
                    if acked |> Set.contains command.SourceSeqNum then
                        loop due alreadyCompleted alreadyPublished (alreadySent + 1) ignored rest
                    else
                        match ActivityRegistry.require activity.Name registry with
                        | Error(DurableRegistryError.ActivityNotFound name) ->
                            Error(ActivityCommandAdapterFailure.MissingHandler name)
                        | Error error ->
                            Error(ActivityCommandAdapterFailure.HandlerFailed(ActivityName activity.Name, string error))
                        | Ok handler ->
                            loop
                                (Due.Send(command, opId, activity, handler) :: due)
                                alreadyCompleted
                                alreadyPublished
                                alreadySent
                                ignored
                                rest
                | ScheduleTimer _
                | CancelTimer _
                | WriteLog _
                | CallChildWorkflow _
                | DeliverChildResult _
                | StartNextGeneration _ ->
                    loop due alreadyCompleted alreadyPublished alreadySent (ignored + 1) rest

        loop [] 0 0 0 0 commands

    /// Execute one due command to a total outcome: every exception is caught
    /// and every publish failure is returned as a value, so a failing item can
    /// never cancel or abort a batch-mate mid-publish.
    let private executeDue owned due =
        async {
            match due with
            | Due.Call(command, opId, activity, handler) ->
                let! handled = invokeHandler activity handler

                match handled with
                | Error failure -> return Outcome.Failed(ActivityCommandAdapterStatus.Failed failure)
                | Ok value ->
                    let! publishedCompletion = publishCompletion owned command.SourceSeqNum opId value

                    match publishedCompletion with
                    | Error status -> return Outcome.Failed status
                    | Ok ack ->
                        return
                            Outcome.Completed
                                { SourceSeqNum = command.SourceSeqNum
                                  MailboxSeqNum = ack.Start.SeqNum
                                  OpId = opId
                                  Activity = activity
                                  Value = value }
            | Due.Send(command, opId, activity, handler) ->
                // One-way send: execute at-least-once, ack to the own inbox for
                // the dedupe window, swallow handler failures (fire-and-forget
                // never surfaces to the workflow).
                let! handled = invokeHandler activity handler

                let failure =
                    match handled with
                    | Ok _ -> None
                    | Error(ActivityCommandAdapterFailure.HandlerFailed(_, message)) -> Some message
                    | Error other -> Some(string other)

                let! publishedAck = publishSendAck owned command.SourceSeqNum opId

                match publishedAck with
                | Error status -> return Outcome.Failed status
                | Ok ack ->
                    return
                        Outcome.Sent
                            { SourceSeqNum = command.SourceSeqNum
                              MailboxSeqNum = ack.Start.SeqNum
                              OpId = opId
                              Activity = activity
                              Failure = failure }
        }

    /// Execute the due batch concurrently, at most maxConcurrent items in
    /// flight, joining every item before returning. Outcomes come back in
    /// command order regardless of completion order.
    let private executeBatch owned maxConcurrent due =
        let rec loop acc chunks =
            async {
                match chunks with
                | [] -> return acc |> List.rev |> List.concat
                | chunk :: rest ->
                    let! outcomes = chunk |> List.map (executeDue owned) |> Async.Parallel
                    return! loop (List.ofArray outcomes :: acc) rest
            }

        loop [] (due |> List.chunkBySize maxConcurrent)

    /// Executes a tick's due CallActivity/SendActivity commands concurrently
    /// (capped by maxConcurrent; pass the default whole-batch cap to overlap
    /// everything due). Exactly-once-effective execution is preserved by
    /// provenance: completions and send acks are durably published to the own
    /// inbox keyed by the command's journal seqNum, and the dispatcher
    /// checkpoint is committed only after EVERY due command in the batch has
    /// its completion/ack durably published. Any failure leaves the cursor
    /// untouched, so the next tick rescans the same window and the
    /// published/acked sets absorb the batch-mates that already landed —
    /// nothing is lost and nothing re-executes.
    let runOnce encode decode maxRecords maxConcurrent registry owned =
        async {
            if maxConcurrent <= 0 then
                invalidArg (nameof maxConcurrent) "maxConcurrent must be positive"

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

                    match classify history publishedSourceSeqNums ackedSourceSeqNums registry commands with
                    | Error failure -> return ActivityCommandAdapterStatus.Failed failure
                    | Ok(due, alreadyCompleted, alreadyPublished, alreadySent, ignored) ->
                        let! outcomes = executeBatch owned maxConcurrent due

                        // One failing item does not lose its batch-mates: every
                        // outcome above is joined (successes durably published)
                        // before the first failure — reported deterministically
                        // in command order — is surfaced without a checkpoint.
                        let firstFailure =
                            outcomes
                            |> List.tryPick (function
                                | Outcome.Failed status -> Some status
                                | Outcome.Completed _
                                | Outcome.Sent _ -> None)

                        match firstFailure with
                        | Some status -> return status
                        | None ->
                            let completed =
                                outcomes
                                |> List.choose (function
                                    | Outcome.Completed completion -> Some completion
                                    | Outcome.Sent _
                                    | Outcome.Failed _ -> None)

                            let sent =
                                outcomes
                                |> List.choose (function
                                    | Outcome.Sent delivery -> Some delivery
                                    | Outcome.Completed _
                                    | Outcome.Failed _ -> None)

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
                                          Completed = completed
                                          Sent = sent
                                          AlreadyCompleted = alreadyCompleted
                                          AlreadyPublished = alreadyPublished
                                          AlreadySent = alreadySent
                                          Ignored = ignored
                                          Checkpoint = checkpoint }
        }
