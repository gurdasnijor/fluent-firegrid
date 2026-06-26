namespace Firegrid.Store

open Effect

[<RequireQualifiedAccess>]
module S2ObjectState =

    let streamName (runtime: S2Runtime) (address: S2ObjectStateAddress) : string =
        Naming.objectStateStreamName runtime.Namespace address

    let invocationStreamName (runtime: S2Runtime) (address: S2ObjectStateAddress) : string =
        Naming.objectInvocationStreamName runtime.Namespace address

    let target (runtime: S2Runtime) (address: S2ObjectStateAddress) : S2StreamRef =
        Runtime.streamTarget runtime (streamName runtime address)

    let appendEventJson (runtime: S2Runtime) (append: S2StateAppend) : Effect<S2AppendAck, S2Error, unit> =
        S2.Stream.appendJsonString (target runtime append.Address) append.BodyJson append.MatchSeqNum
        |> Runtime.provide runtime

    let readEventJson (runtime: S2Runtime) (read: S2StateRead) : Effect<string list, S2Error, unit> =
        let stop =
            read.MaxRecords
            |> Option.map (fun count ->
                { S2ReadStop.Empty with
                    Limits = Some { S2ReadLimits.Empty with Count = Some count } })

        let start = read.FromSeqNum |> Option.map S2ReadStart.FromSeqNum

        S2.Stream.readStrings (target runtime read.Address) start stop
        |> Effect.map (fun batch ->
            batch.Records
            |> List.choose (fun record ->
                match record.Body with
                | S2RecordBody.StringBody body -> Some body
                | S2RecordBody.BytesBody _ -> None))
        |> Runtime.provide runtime
