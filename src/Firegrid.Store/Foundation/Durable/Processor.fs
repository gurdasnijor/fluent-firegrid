namespace Firegrid.Store.Foundation.Durable

open Firegrid.Log

[<Struct>]
type TimerId = TimerId of string

type Timestamp = int64

[<Struct>]
type EffectId = EffectId of string

[<Struct>]
type SubjectId = SubjectId of string

type ActorAddress = { Segments: string list }

[<RequireQualifiedAccess>]
type WakeReason =
    | MailboxReady
    | TimerFired of TimerId * Timestamp
    | ChildTerminal of SubjectId

[<RequireQualifiedAccess>]
type Intent =
    | SetTimer of TimerId * dueAt: Timestamp
    | Send of target: ActorAddress * payload: Payload
    | Execute of EffectId * payload: Payload

type StoredRecord<'record> =
    { Seq: int64
      Body: 'record
      Timestamp: Timestamp }

type Admitted<'msg> =
    { MailboxSeqNum: int64
      Source: string
      SourceSeqNum: int64
      Message: 'msg }

type Decision<'state, 'record, 'terminal> =
    { State: 'state
      Append: 'record list
      Intents: Intent list
      Seal: 'terminal option }

type Handler<'state, 'msg, 'record, 'terminal> =
    { Initial: 'state
      Fold: 'state -> StoredRecord<'record> -> 'state
      OnAdmitted: 'state -> Admitted<'msg> -> Decision<'state, 'record, 'terminal>
      OnWake: 'state -> WakeReason -> Decision<'state, 'record, 'terminal> }

type CommittedIntent =
    { Seq: int64
      IntentIndex: int
      Intent: Intent }

type DriveCommitAck =
    { Appended: int
      Intents: CommittedIntent list
      Sealed: bool }

[<RequireQualifiedAccess>]
type DriveCommitFailure =
    | CommitDeposed of expectedFence: string
    | CommitRejected of S2Errors.S2Failure

[<RequireQualifiedAccess>]
type DriveOutcome<'terminal> =
    | Idle
    | Advanced
    | Sealed of 'terminal
    | Deposed of expectedFence: string
    | Failed of DriveError

and [<RequireQualifiedAccess>] DriveError =
    | ClaimFailed of string
    | RebuildFailed of string
    | MailboxFailed of MailboxFailure
    | CommitFailed of S2Errors.S2Failure
    | IntentDispatchFailed of string
    | Unexpected of string

type DriveEnv =
    { Wake: WakeReason option
      Claim: ActorAddress -> Async<Result<FenceToken, string>>
      Rebuild: ActorAddress -> FenceToken -> Async<Result<StoredRecord<obj> list, string>>
      Admit: ActorAddress -> FenceToken -> Async<Result<Admitted<obj> list, MailboxFailure>>
      Commit: ActorAddress -> FenceToken -> obj list -> Intent list -> obj option -> Async<Result<DriveCommitAck, DriveCommitFailure>>
      Dispatch: ActorAddress -> FenceToken -> CommittedIntent list -> Async<Result<unit, string>> }

[<RequireQualifiedAccess>]
module Processor =
    let private castStored<'record> (record: StoredRecord<obj>) : StoredRecord<'record> =
        { Seq = record.Seq
          Body = unbox<'record> record.Body
          Timestamp = record.Timestamp }

    let private castAdmitted<'msg> (admitted: Admitted<obj>) : Admitted<'msg> =
        { MailboxSeqNum = admitted.MailboxSeqNum
          Source = admitted.Source
          SourceSeqNum = admitted.SourceSeqNum
          Message = unbox<'msg> admitted.Message }

    let private combine state decisions =
        decisions
        |> List.fold
            (fun (_, append, intents, seal) decision ->
                decision.State,
                append @ (decision.Append |> List.map box),
                intents @ decision.Intents,
                match seal with
                | Some _ -> seal
                | None -> decision.Seal |> Option.map box)
            (state, [], [], None)

    let drive
        (env: DriveEnv)
        (address: ActorAddress)
        (handler: Handler<'state, 'msg, 'record, 'terminal>)
        : Async<DriveOutcome<'terminal>> =
        async {
            try
                let! claim = env.Claim address

                match claim with
                | Error error -> return DriveOutcome.Failed(DriveError.ClaimFailed error)
                | Ok fence ->
                    let! rebuilt = env.Rebuild address fence

                    match rebuilt with
                    | Error error -> return DriveOutcome.Failed(DriveError.RebuildFailed error)
                    | Ok stored ->
                        let state =
                            stored
                            |> List.map castStored<'record>
                            |> List.fold handler.Fold handler.Initial

                        let! admitted = env.Admit address fence

                        match admitted with
                        | Error error -> return DriveOutcome.Failed(DriveError.MailboxFailed error)
                        | Ok admitted ->
                            let decisions =
                                match admitted, env.Wake with
                                | [], None -> []
                                | [], Some wake -> [ handler.OnWake state wake ]
                                | messages, _ ->
                                    let folder state admitted =
                                        let decision = handler.OnAdmitted state (castAdmitted<'msg> admitted)
                                        decision, decision.State

                                    messages
                                    |> List.mapFold folder state
                                    |> fst

                            match decisions with
                            | [] -> return DriveOutcome.Idle
                            | _ ->
                                let _, append, intents, seal = combine state decisions
                                let! committed = env.Commit address fence append intents seal

                                match committed with
                                | Error(DriveCommitFailure.CommitDeposed expected) ->
                                    return DriveOutcome.Deposed expected
                                | Error(DriveCommitFailure.CommitRejected failure) ->
                                    return DriveOutcome.Failed(DriveError.CommitFailed failure)
                                | Ok ack ->
                                    let! dispatched = env.Dispatch address fence ack.Intents

                                    match dispatched with
                                    | Error error ->
                                        return DriveOutcome.Failed(DriveError.IntentDispatchFailed error)
                                    | Ok() ->
                                        match seal with
                                        | Some terminal -> return DriveOutcome.Sealed(unbox<'terminal> terminal)
                                        | None -> return DriveOutcome.Advanced
            with error ->
                return DriveOutcome.Failed(DriveError.Unexpected error.Message)
        }
