namespace Firegrid.FluentFiregrid.S2

[<RequireQualifiedAccess>]
module Naming =

    let private sanitize (value: string) =
        let replaced =
            value
            |> Seq.map (fun c ->
                if System.Char.IsLetterOrDigit c || c = '-' || c = '_' || c = '.' then
                    c
                else
                    '-')
            |> Seq.toArray
            |> System.String

        if System.String.IsNullOrWhiteSpace replaced then "default" else replaced

    let objectStateStreamName (ns: string) (address: S2ObjectStateAddress) : string =
        $"{sanitize ns}/obj/{sanitize address.ObjectName}/{sanitize address.Key}/state"

    let objectInvocationStreamName (ns: string) (address: S2ObjectStateAddress) : string =
        $"{sanitize ns}/obj/{sanitize address.ObjectName}/{sanitize address.Key}/invocations"

    let delayedStartStreamName (ns: string) : string = $"{sanitize ns}/delayed-starts"

    let runEventsStreamName (ns: string) (runId: RunId) : string =
        $"{sanitize ns}.runs.{sanitize runId}.events"

    let runMetaStreamName (ns: string) (runId: RunId) : string =
        $"{sanitize ns}.runs.{sanitize runId}.meta"

    let runIndexStreamName (ns: string) : string = $"{sanitize ns}.runs.index"

    let timersStreamName (ns: string) : string = $"{sanitize ns}.timers"

    let schedulesStreamName (ns: string) : string = $"{sanitize ns}.schedules"
