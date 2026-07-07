namespace Firegrid.Store

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

    let private jsonRecord (value: obj) (headers: (string * string) list) : S2.Record =
        S2.Record.textWith headers (JsJson.stringify value)

    let ensureRunEventStream (runtime: S2Runtime) (runId: RunId) : Async<S2StreamRef> =
        Runtime.ensureStream runtime (eventsStreamName runtime runId)

    let appendJsonFact
        (runtime: S2Runtime)
        (streamName: string)
        (fact: obj)
        (headers: (string * string) list)
        (matchSeqNum: float option)
        : Async<S2.AppendAck> =
        async {
            let! target = Runtime.ensureStream runtime streamName
            let stream = Runtime.stream runtime target

            return!
                stream
                |> S2.appendWith
                    { S2.AppendOptions.none with
                        MatchSeqNum = matchSeqNum |> Option.map int64 }
                    [ jsonRecord fact headers ]
        }

    let readJsonRecords<'A>
        (runtime: S2Runtime)
        (streamName: string)
        (fromSeqNum: float)
        : Async<ReadJsonRecordsResult<'A>> =
        async {
            let! target = Runtime.ensureStream runtime streamName
            let stream = Runtime.stream runtime target
            let! tail = S2.checkTail stream
            let fromSeqNum = int64 fromSeqNum

            if tail.SeqNum <= fromSeqNum then
                return { NextSeqNum = float tail.SeqNum; Records = [] }
            else
                let count = int (tail.SeqNum - fromSeqNum)

                let! records =
                    stream
                    |> S2.readWith
                        { S2.ReadOptions.empty with
                            Start = Some(S2.FromSeqNum fromSeqNum)
                            Count = Some count }

                let records =
                    records
                    |> List.map (fun record -> JsJson.parse record.Body : 'A)

                return { NextSeqNum = float tail.SeqNum; Records = records }
        }

    let appendEvents (runtime: S2Runtime) (args: AppendEventsArgs) : Async<AppendEventsResult> =
        async {
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

            let! target = Runtime.ensureStream runtime streamName
            let stream = Runtime.stream runtime target

            let! ack =
                stream
                |> S2.appendWith
                    { S2.AppendOptions.none with
                        MatchSeqNum = Some(int64 args.ExpectedNextIndex) }
                    records

            return { NextIndex = float ack.End.SeqNum }
        }

    let readEvents (runtime: S2Runtime) (args: ReadEventsArgs) : Async<StoredWorkflowEvent list> =
        async {
            let! result =
                readJsonRecords<EventEnvelope>
                    runtime
                    (eventsStreamName runtime args.RunId)
                    (args.FromIndex |> Option.defaultValue 0.0)

            return result.Records |> List.map (storedWorkflowEvent args.RunId)
        }
