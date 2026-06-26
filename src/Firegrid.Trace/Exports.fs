module Firegrid.Trace.Exports

open Firegrid.Trace

let ChdbClient =
    {| create = ChdbClient.create |}

let ChdbSpanExporter =
    {| create = ChdbSpanExporter.create |}

let chdbSession config = ChdbClient.create config
let chdbSpanExporter session = ChdbSpanExporter.create session
let insertChdbSpanRows _session _rows = ()
let layer config = chdbSession config
