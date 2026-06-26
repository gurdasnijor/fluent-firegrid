namespace Firegrid.Trace

type ChdbConfig =
    { Path: string option }

type ChdbSession =
    { Config: ChdbConfig }

[<RequireQualifiedAccess>]
module ChdbClient =
    let create config = { Config = config }
