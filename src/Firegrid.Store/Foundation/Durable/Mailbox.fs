namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log

type MailboxAdmittedMessage =
    { MailboxSeqNum: int64
      Envelope: MailboxEnvelope
      Events: Event list }

type MailboxReport =
    { FromSeqNum: int64
      NextSeqNum: int64
      Scanned: int
      Accepted: MailboxAdmittedMessage list
      Duplicates: int
      Commit: CommitResult option }

[<RequireQualifiedAccess>]
type MailboxFailure =
    | LogReadFailed of string
    | LogDecodeFailed of seqNum: int64 * error: string
    | MailboxReadFailed of string
    | MailboxDecodeFailed of seqNum: int64 * error: string
    | CommitFailed of S2Errors.S2Failure

[<RequireQualifiedAccess>]
type MailboxStatus =
    | Folded of MailboxReport
    | Deposed of expectedFence: string
    | Failed of MailboxFailure

[<RequireQualifiedAccess>]
module Mailbox =
    let private decodeLog decoded =
        let rec loop records =
            function
            | [] -> Ok(List.rev records)
            | (seqNum, Ok record) :: rest -> loop ((seqNum, record) :: records) rest
            | (seqNum, Error error) :: _ -> Error(MailboxFailure.LogDecodeFailed(seqNum, error))

        loop [] decoded

    let private readLog decode owned =
        async {
            try
                let! decoded = S2Substrate.readLogText decode owned
                return decodeLog decoded
            with error ->
                return Error(MailboxFailure.LogReadFailed error.Message)
        }

    let private cursor decoded =
        decoded
        |> List.choose (function
            | _, Incoming(MailboxCheckpoint nextSeqNum) -> Some nextSeqNum
            | _ -> None)
        |> List.fold max 0L

    let private highwater decoded =
        decoded
        |> List.choose (function
            | _, Incoming(MailboxSourceHighwater(source, nextSeqNum)) -> Some(source, nextSeqNum)
            | _ -> None)
        |> List.fold
            (fun state (source, nextSeqNum) ->
                let current = state |> Map.tryFind source |> Option.defaultValue 0L
                state |> Map.add source (max current nextSeqNum))
            Map.empty

    let private readMailbox (decode: string -> Result<MailboxEnvelope, string>) from count owned =
        async {
            try
                let! readRecords = S2Substrate.readMailbox from count owned
                let records: S2.ReadRecord list = readRecords

                let rec loop (acc: (int64 * MailboxEnvelope) list) (remaining: S2.ReadRecord list) =
                    match remaining with
                    | [] -> Ok(List.rev acc)
                    | (record: S2.ReadRecord) :: rest ->
                        match decode record.Body with
                        | Ok envelope -> loop ((record.SeqNum, envelope) :: acc) rest
                        | Error error -> Error(MailboxFailure.MailboxDecodeFailed(record.SeqNum, error))

                return loop [] records
            with error ->
                match S2Errors.classify error with
                | S2Errors.RangeNotSatisfiable _ -> return Ok []
                | _ -> return Error(MailboxFailure.MailboxReadFailed error.Message)
        }

    let private eventsFor =
        function
        | CompleteActivity(opId, value) -> [ ActivityCompleted(opId, value) ]
        | FireTimer(opId, _) -> [ TimerFired opId ]
        | CompleteChild(opId, output) -> [ ChildWorkflowCompleted(opId, output) ]
        | StartWorkflow _
        | StartChildWorkflow _
        | RaiseSignal _
        | AckSend _ -> []

    let private recordsFor mailboxSeqNum (envelope: MailboxEnvelope) =
        let events = eventsFor envelope.Message

        let eventRecords = events |> List.map (HistoryEvent >> Incoming)

        let startRecords =
            match envelope.Message with
            | StartWorkflow(name, input) -> [ Incoming(WorkflowStarted(name, input)) ]
            | StartChildWorkflow(name, input, parent, parentOpId) ->
                [ Incoming(WorkflowStarted(name, input))
                  Incoming(WorkflowParent(parent, parentOpId)) ]
            | RaiseSignal _
            | CompleteActivity _
            | FireTimer _
            | CompleteChild _
            | AckSend _ -> []

        let accepted: MailboxAdmittedMessage =
            { MailboxSeqNum = mailboxSeqNum
              Envelope = envelope
              Events = events }

        accepted,
        [ yield Incoming(MailboxMessageAccepted envelope)
          yield! startRecords
          yield! eventRecords
          yield Incoming(MailboxSourceHighwater(envelope.Source, envelope.SourceSeqNum + 1L)) ]

    let private selectFresh highwater (inboxRecords: (int64 * MailboxEnvelope) list) =
        let rec loop
            (state: Map<string, int64>)
            (accepted: MailboxAdmittedMessage list)
            duplicates
            (records: HistoryEntry<StepRecord> list)
            (remaining: (int64 * MailboxEnvelope) list)
            =
            match remaining with
            | [] -> accepted, duplicates, records
            | (mailboxSeqNum, envelope) :: rest ->
                let nextSeen = state |> Map.tryFind envelope.Source |> Option.defaultValue 0L

                if envelope.SourceSeqNum < nextSeen then
                    loop state accepted (duplicates + 1) records rest
                else
                    let acceptedMessage, committedRecords = recordsFor mailboxSeqNum envelope

                    loop
                        (state |> Map.add envelope.Source (envelope.SourceSeqNum + 1L))
                        (accepted @ [ acceptedMessage ])
                        duplicates
                        (records @ committedRecords)
                        rest

        loop highwater [] 0 [] inboxRecords

    let runOnce encode decodeLogEntry decodeMailboxEnvelope maxRecords owned =
        async {
            let! log = readLog decodeLogEntry owned

            match log with
            | Error failure -> return MailboxStatus.Failed failure
            | Ok decoded ->
                let fromSeqNum = cursor decoded
                let! inbox = readMailbox decodeMailboxEnvelope fromSeqNum maxRecords owned

                match inbox with
                | Error failure -> return MailboxStatus.Failed failure
                | Ok inboxRecords ->
                    let nextSeqNum =
                        match List.rev inboxRecords with
                        | (seqNum, _) :: _ -> seqNum + 1L
                        | [] -> fromSeqNum

                    if nextSeqNum = fromSeqNum then
                        return
                            MailboxStatus.Folded
                                { FromSeqNum = fromSeqNum
                                  NextSeqNum = nextSeqNum
                                  Scanned = 0
                                  Accepted = []
                                  Duplicates = 0
                                  Commit = None }
                    else
                        let accepted, duplicates, records = selectFresh (highwater decoded) inboxRecords

                        let records = records @ [ Incoming(MailboxCheckpoint nextSeqNum) ]
                        let! commit = S2Substrate.commitText encode records owned

                        return
                            match commit with
                            | Committed _ ->
                                MailboxStatus.Folded
                                    { FromSeqNum = fromSeqNum
                                      NextSeqNum = nextSeqNum
                                      Scanned = List.length inboxRecords
                                      Accepted = accepted
                                      Duplicates = duplicates
                                      Commit = Some commit }
                            | Deposed expected -> MailboxStatus.Deposed expected
                            | CommitFailed failure -> MailboxStatus.Failed(MailboxFailure.CommitFailed failure)
        }
