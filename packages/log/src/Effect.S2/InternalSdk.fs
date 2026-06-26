namespace Effect

open Fable.Core
open Fable.Core.JsInterop

[<RequireQualifiedAccess>]
module internal S2Sdk =

    type RawClient = obj
    type RawBasin = obj
    type RawStream = obj
    type RawAppendRecord = obj
    type RawAppendInput = obj

#if FABLE_COMPILER
    [<Import("S2", "@s2-dev/streamstore")>]
    let private s2Constructor: obj = jsNative

    [<Import("AppendRecord", "@s2-dev/streamstore")>]
    let private appendRecordFactory: obj = jsNative

    [<Import("AppendInput", "@s2-dev/streamstore")>]
    let private appendInputFactory: obj = jsNative
#else
    let private s2Constructor: obj = null
    let private appendRecordFactory: obj = null
    let private appendInputFactory: obj = null
#endif

    [<Emit("undefined")>]
    let undefinedObj: obj = jsNative

    [<Emit("new $0($1)")>]
    let private newClient (_ctor: obj) (_options: obj) : RawClient = jsNative

    [<Emit("new Date($0)")>]
    let dateFromEpochMillis (_millis: float) : JS.Date = jsNative

    [<Emit("$0 instanceof Date ? $0.getTime() : Date.parse($0)")>]
    let dateEpochMillis (_value: obj) : float = jsNative

    [<Emit("$0 == null")>]
    let isNullish (_value: obj) : bool = jsNative

    [<Emit("$0[$1]")>]
    let prop<'T> (_value: obj) (_key: string) : 'T = jsNative

    [<Emit("String($0)")>]
    let stringValue (_value: obj) : string = jsNative

    [<Emit("Number($0)")>]
    let numberValue (_value: obj) : float = jsNative

    [<Emit("Array.from($0 || [])")>]
    let arrayFrom<'T> (_value: obj) : 'T[] = jsNative

    [<Emit("$0 && $0.name ? String($0.name) : 'S2Error'")>]
    let errorName (_error: exn) : string = jsNative

    [<Emit("$0 && $0.message ? String($0.message) : String($0)")>]
    let errorMessage (_error: exn) : string = jsNative

    [<Emit("$0 && $0.code != null ? String($0.code) : null")>]
    let errorCode (_error: exn) : obj = jsNative

    [<Emit("$0 && $0.status != null ? Number($0.status) : null")>]
    let errorStatus (_error: exn) : obj = jsNative

    [<Emit("$0 && $0.origin != null ? String($0.origin) : null")>]
    let errorOrigin (_error: exn) : obj = jsNative

    [<Emit("$0 && $0.data != null ? $0.data : null")>]
    let errorData (_error: exn) : obj = jsNative

    [<Emit("$0.basin($1)")>]
    let basin (_client: RawClient) (_name: string) : RawBasin = jsNative

    [<Emit("$0.basin($1).stream($2)")>]
    let stream (_client: RawClient) (_basin: string) (_stream: string) : RawStream = jsNative

    [<Emit("$0.basins.list($1, $2)")>]
    let listBasins (_client: RawClient) (_args: obj) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.basins.ensure($1, $2)")>]
    let ensureBasin (_client: RawClient) (_args: obj) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.streams.list($1, $2)")>]
    let listStreams (_basin: RawBasin) (_args: obj) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.streams.ensure($1, $2)")>]
    let ensureStream (_basin: RawBasin) (_args: obj) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.streams.create($1, $2)")>]
    let createStream (_basin: RawBasin) (_args: obj) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.streams.delete($1, $2)")>]
    let deleteStream (_basin: RawBasin) (_args: obj) (_options: obj) : JS.Promise<unit> = jsNative

    [<Emit("$0.checkTail($1)")>]
    let checkTail (_stream: RawStream) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.append($1, $2)")>]
    let append (_stream: RawStream) (_input: RawAppendInput) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.read($1, $2)")>]
    let read (_stream: RawStream) (_input: obj) (_options: obj) : JS.Promise<obj> = jsNative

    [<Emit("$0.readSession($1, $2)")>]
    let readSession (_stream: RawStream) (_input: obj) (_options: obj) : JS.Promise<JS.AsyncIterable<obj>> = jsNative

    [<Emit("$0.string($1)")>]
    let appendRecordString (_factory: obj) (_params: obj) : RawAppendRecord = jsNative

    [<Emit("$0.bytes($1)")>]
    let appendRecordBytes (_factory: obj) (_params: obj) : RawAppendRecord = jsNative

    [<Emit("$0.create($1)")>]
    let appendInputCreate (_factory: obj) (_records: RawAppendRecord[]) : RawAppendInput = jsNative

    [<Emit("$0.create($1, $2)")>]
    let appendInputCreateWithOptions (_factory: obj) (_records: RawAppendRecord[]) (_options: obj) : RawAppendInput =
        jsNative

    let client (options: obj) : RawClient = newClient s2Constructor options

    let stringRecord (options: obj) : RawAppendRecord =
        appendRecordString appendRecordFactory options

    let bytesRecord (options: obj) : RawAppendRecord =
        appendRecordBytes appendRecordFactory options

    let appendInput (records: RawAppendRecord[]) (options: obj option) : RawAppendInput =
        match options with
        | Some opts -> appendInputCreateWithOptions appendInputFactory records opts
        | None -> appendInputCreate appendInputFactory records
