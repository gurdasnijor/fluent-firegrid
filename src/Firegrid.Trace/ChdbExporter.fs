namespace Firegrid.Trace

type SpanRow = obj

type ChdbSpanExporter =
    { Session: ChdbSession }

[<RequireQualifiedAccess>]
module ChdbSpanExporter =
    let create session = { Session = session }
