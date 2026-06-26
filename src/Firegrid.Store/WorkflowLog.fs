namespace Firegrid.Store

open Effect
open Firegrid.Log
open Fable.Core.JsInterop

[<RequireQualifiedAccess>]
module WorkflowLog =

    let eventsStreamName (runtime: S2Runtime) (runId: RunId) : string =
        Naming.runEventsStreamName runtime.Namespace runId

    let private eventType (event: WorkflowEvent) : string = JsJson.stringProp "type" event

    let private eventCreatedAt (event: WorkflowEvent) : float = JsJson.numberProp "ts" event

    let private eventStepId (event: WorkflowEvent) : string option = JsJson.optionalStringProp "stepId" event

    let private envelopeObject (envelope: EventEnvelope) : obj =
        createObj
            [ "eventIndex" ==> envelope.EventIndex
              "event" ==> envelope.Event ]

    let private storedWorkflowEvent (runId: RunId) (envelope: EventEnvelope) : StoredWorkflowEvent =
        { RunId = runId
          EventIndex = envelope.EventIndex
          Event = envelope.Event
          EventType = eventType envelope.Event
          StepId = eventStepId envelope.Event
          CreatedAt = eventCreatedAt envelope.Event }

    let private jsonRecord (value: obj) (headers: (string * string) list) : S2AppendRecord =
        S2.AppendRecord.stringWith (JsJson.stringify value) headers None

    let ensureRunEventStream (runtime: S2Runtime) (runId: RunId) : Effect<S2StreamRef, S2Error, unit> =
        Runtime.ensureStream runtime (eventsStreamName runtime runId)

    let appendJsonFact
        (runtime: S2Runtime)
        (streamName: string)
        (fact: obj)
        (headers: (string * string) list)
        (matchSeqNum: float option)
        : Effect<S2AppendAck, S2Error, unit> =
        effect {
            let! target = Runtime.ensureStreamContext runtime streamName

            return!
                S2.Stream.appendRecords
                    target
                    [ jsonRecord fact headers ]
                    (Some
                        { S2AppendOptions.Empty with
                            MatchSeqNum = matchSeqNum })
        }
        |> Runtime.provide runtime

    let readJsonRecords<'A>
        (runtime: S2Runtime)
        (streamName: string)
        (fromSeqNum: float)
        : Effect<ReadJsonRecordsResult<'A>, S2Error, unit> =
        effect {
            let! target = Runtime.ensureStreamContext runtime streamName
            let! tail = S2.Stream.tail target

            if tail.SeqNum <= fromSeqNum then
                return { NextSeqNum = tail.SeqNum; Records = [] }
            else
                let count = int (tail.SeqNum - fromSeqNum)

                let! batch =
                    S2.Stream.readStrings
                        target
                        (Some(S2ReadStart.FromSeqNum fromSeqNum))
                        (Some
                            { S2ReadStop.Empty with
                                Limits = Some { S2ReadLimits.Empty with Count = Some count } })

                let records =
                    batch.Records
                    |> List.choose (fun record ->
                        match record.Body with
                        | S2RecordBody.StringBody body -> Some(JsJson.parse body : 'A)
                        | S2RecordBody.BytesBody _ -> None)

                return { NextSeqNum = tail.SeqNum; Records = records }
        }
        |> Runtime.provide runtime

    let appendEvents (runtime: S2Runtime) (args: AppendEventsArgs) : Effect<AppendEventsResult, S2Error, unit> =
        effect {
            let streamName = eventsStreamName runtime args.RunId

            let envelopes =
                args.Events
                |> List.mapi (fun index event ->
                    { EventIndex = args.ExpectedNextIndex + float index
                      Event = event })

            let records =
                envelopes
                |> List.map (fun envelope ->
                    jsonRecord
                        (envelopeObject envelope)
                        [ "tanstack.workflow.run_id", args.RunId
                          "tanstack.workflow.event_index", string envelope.EventIndex
                          "tanstack.workflow.event_type", eventType envelope.Event
                          "tanstack.workflow.step_id", envelope.Event |> eventStepId |> Option.defaultValue "" ])

            let! target = Runtime.ensureStreamContext runtime streamName

            let! ack =
                S2.Stream.appendRecords
                    target
                    records
                    (Some
                        { S2AppendOptions.Empty with
                            MatchSeqNum = Some args.ExpectedNextIndex })

            return { NextIndex = ack.End.SeqNum }
        }
        |> Runtime.provide runtime

    let readEvents (runtime: S2Runtime) (args: ReadEventsArgs) : Effect<StoredWorkflowEvent list, S2Error, unit> =
        readJsonRecords<EventEnvelope> runtime (eventsStreamName runtime args.RunId) (args.FromIndex |> Option.defaultValue 0.0)
        |> Effect.map (fun result -> result.Records |> List.map (storedWorkflowEvent args.RunId))
