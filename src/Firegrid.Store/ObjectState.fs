namespace Firegrid.Store

open Firegrid.Log

[<RequireQualifiedAccess>]
module S2ObjectState =

    let streamName (runtime: S2Runtime) (address: S2ObjectStateAddress) : string =
        Naming.objectStateStreamName runtime.Namespace address

    let invocationStreamName (runtime: S2Runtime) (address: S2ObjectStateAddress) : string =
        Naming.objectInvocationStreamName runtime.Namespace address

    let target (runtime: S2Runtime) (address: S2ObjectStateAddress) : S2StreamRef =
        Runtime.streamTarget runtime (streamName runtime address)

    let appendEventJson (runtime: S2Runtime) (append: S2StateAppend) : Async<S2.AppendAck> =
        async {
            let stream = Runtime.stream runtime (target runtime append.Address)

            return!
                stream
                |> S2.appendWith
                    { S2.AppendOptions.none with
                        MatchSeqNum = append.MatchSeqNum |> Option.map int64 }
                    [ S2.Record.text append.BodyJson ]
        }

    let readEventJson (runtime: S2Runtime) (read: S2StateRead) : Async<string list> =
        async {
            let stream = Runtime.stream runtime (target runtime read.Address)
            let start = read.FromSeqNum |> Option.map (int64 >> S2.FromSeqNum)

            let options =
                { S2.ReadOptions.empty with
                    Start = start
                    Count = read.MaxRecords }

            let! records = stream |> S2.readWith options
            return records |> List.map (fun record -> record.Body)
        }
