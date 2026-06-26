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
