namespace Effect

open Fable.Core
open Fable.Core.JsInterop

type S2Client = internal { Raw: S2Sdk.RawClient }

[<RequireQualifiedAccess>]
module S2 =

    let tag: Tag<S2Client> = Tag.make<S2Client> "@firegrid/log/S2Client"

    let private epochDateTime (value: obj) : DateTime =
        { EpochMillis = int64 (S2Sdk.dateEpochMillis value) }

    let private jsDate (value: DateTime) : JS.Date =
        S2Sdk.dateFromEpochMillis (float value.EpochMillis)

    let private optionalObj (value: obj) : obj =
        if S2Sdk.isNullish value then S2Sdk.undefinedObj else value

    let private boxOption (name: string) (value: 'a option) =
        value |> Option.map (fun v -> name ==> v)

    let private boxDateOption (name: string) (value: DateTime option) =
        value |> Option.map (fun v -> name ==> jsDate v)

    let private makeObj (fields: (string * obj) list) : obj =
        fields |> List.map (fun (key, value) -> key ==> value) |> createObj

    let private requestOptions (options: S2RequestOptions option) : obj =
        match options with
        | None -> S2Sdk.undefinedObj
        | Some opts -> [ boxOption "signal" opts.Signal ] |> List.choose id |> createObj

    let private pageRequest (request: S2PageRequest option) : obj =
        match request with
        | None -> S2Sdk.undefinedObj
        | Some req ->
            [ boxOption "prefix" req.Prefix
              boxOption "startAfter" req.StartAfter
              boxOption "limit" req.Limit ]
            |> List.choose id
            |> createObj

    let private optionString (value: obj) : string option =
        if S2Sdk.isNullish value then
            None
        else
            Some(S2Sdk.stringValue value)

    let private optionDateTime (value: obj) : DateTime option =
        if S2Sdk.isNullish value then
            None
        else
            Some(epochDateTime value)

    let private toError (error: exn) : S2Error =
        { Name = S2Sdk.errorName error
          Message = S2Sdk.errorMessage error
          Code = optionString (S2Sdk.errorCode error)
          Status =
            let status = S2Sdk.errorStatus error

            if S2Sdk.isNullish status then
                None
            else
                Some(int (S2Sdk.numberValue status))
          Origin = optionString (S2Sdk.errorOrigin error)
          Data =
            let data = S2Sdk.errorData error
            if S2Sdk.isNullish data then None else Some data }

    let private tryPromise (promise: unit -> JS.Promise<'A>) (map: 'A -> 'B) : Effect<'B, S2Error, 'R> =
        Effect.tryPromiseJS promise toError |> Effect.map map

    let private clientOptions (config: S2Config) : obj =
        let endpoints =
            config.Endpoint
            |> Option.map (fun endpoint ->
                createObj
                    [ "account" ==> endpoint
                      "basin" ==> endpoint ])

        [ Some("accessToken" ==> config.AccessToken)
          boxOption "endpoints" endpoints
          boxOption "requestTimeoutMillis" config.RequestTimeoutMillis
          boxOption "connectionTimeoutMillis" config.ConnectionTimeoutMillis ]
        |> List.choose id
        |> createObj

    let config (accessToken: string) : S2Config = S2Config.Create accessToken

    let configWithEndpoint (accessToken: string) (endpoint: string) : S2Config =
        { S2Config.Create accessToken with
            Endpoint = Some endpoint }

    let make (config: S2Config) : Effect<S2Client, S2Error, 'R> =
        Effect.sync (fun () -> { Raw = S2Sdk.client (clientOptions config) })

    let layer (config: S2Config) : Layer<S2Error, 'RIn> = Layer.effect tag (make config)

    let service<'E> : Effect<S2Client, 'E, Context> = Effect.service tag

    let streamRef (basin: string) (stream: string) : S2StreamRef = { Basin = basin; Stream = stream }

    let private withClient (f: S2Client -> Effect<'A, S2Error, Context>) : Effect<'A, S2Error, Context> =
        Effect.service tag |> Effect.flatMap f

    let private streamHandle (target: S2StreamRef) (client: S2Client) : S2Sdk.RawStream =
        S2Sdk.stream client.Raw target.Basin target.Stream

    let private position (raw: obj) : S2StreamPosition =
        { SeqNum = S2Sdk.prop<float> raw "seqNum"
          Timestamp = epochDateTime (S2Sdk.prop<obj> raw "timestamp") }

    let private appendAck (raw: obj) : S2AppendAck =
        { Start = position (S2Sdk.prop<obj> raw "start")
          End = position (S2Sdk.prop<obj> raw "end")
          Tail = position (S2Sdk.prop<obj> raw "tail") }

    let private tailResponse (raw: obj) : S2TailResponse =
        { Tail = position (S2Sdk.prop<obj> raw "tail") }

    let private rawHeaders (headers: (string * string) list) : (string * string)[] = headers |> List.toArray

    let private rawBytesHeaders (headers: (byte[] * byte[]) list) : (byte[] * byte[])[] = headers |> List.toArray

    let private rawAppendRecord (record: S2AppendRecord) : S2Sdk.RawAppendRecord =
        match record with
        | StringRecord r ->
            [ Some("body" ==> r.Body)
              Some("headers" ==> rawHeaders r.Headers)
              boxDateOption "timestamp" r.Timestamp ]
            |> List.choose id
            |> createObj
            |> S2Sdk.stringRecord
        | BytesRecord r ->
            [ Some("body" ==> r.Body)
              Some("headers" ==> rawBytesHeaders r.Headers)
              boxDateOption "timestamp" r.Timestamp ]
            |> List.choose id
            |> createObj
            |> S2Sdk.bytesRecord

    let private appendOptions (options: S2AppendOptions option) : obj option =
        options
        |> Option.map (fun opts ->
            [ boxOption "matchSeqNum" opts.MatchSeqNum
              boxOption "fencingToken" opts.FencingToken ]
            |> List.choose id
            |> createObj)

    let private appendInput (request: S2AppendRequest) : S2Sdk.RawAppendInput =
        request.Records
        |> List.map rawAppendRecord
        |> List.toArray
        |> fun records -> S2Sdk.appendInput records (appendOptions request.Options)

    let private readStart (start: S2ReadStart option) (clamp: bool option) : obj option =
        match start, clamp with
        | None, None -> None
        | _ ->
            let from =
                match start with
                | None -> []
                | Some(FromSeqNum seqNum) -> [ "seqNum" ==> seqNum ]
                | Some(FromTimestamp ts) -> [ "timestamp" ==> jsDate ts ]
                | Some(FromTailOffset offset) -> [ "tailOffset" ==> offset ]

            [ if from.Length > 0 then
                  Some("from" ==> createObj from)
              else
                  None
              boxOption "clamp" clamp ]
            |> List.choose id
            |> createObj
            |> Some

    let private readStop (stop: S2ReadStop option) : obj option =
        stop
        |> Option.map (fun stop ->
            let limits =
                stop.Limits
                |> Option.map (fun limits ->
                    [ boxOption "count" limits.Count; boxOption "bytes" limits.Bytes ]
                    |> List.choose id
                    |> createObj)

            [ boxOption "limits" limits
              boxDateOption "untilTimestamp" stop.UntilTimestamp
              boxOption "waitSecs" stop.WaitSeconds ]
            |> List.choose id
            |> createObj)

    let private readInput (request: S2ReadRequest) : obj =
        [ boxOption "start" (readStart request.Start request.Clamp)
          boxOption "stop" (readStop request.Stop)
          boxOption "ignoreCommandRecords" request.IgnoreCommandRecords ]
        |> List.choose id
        |> createObj

    let private readOptions (request: S2ReadRequest) : obj =
        let baseOptions =
            match request.RequestOptions with
            | None -> []
            | Some opts -> [ boxOption "signal" opts.Signal ] |> List.choose id

        let format =
            match request.Format with
            | ReadString -> "string"
            | ReadBytes -> "bytes"

        ("as" ==> format) :: baseOptions |> createObj

    let private recordBody (raw: obj) (format: S2ReadFormat) : S2RecordBody =
        match format with
        | ReadString -> StringBody(S2Sdk.prop<string> raw "body")
        | ReadBytes -> BytesBody(S2Sdk.prop<byte[]> raw "body")

    let private recordHeaders (raw: obj) (format: S2ReadFormat) : S2RecordHeader list =
        let headers = S2Sdk.prop<obj> raw "headers" |> S2Sdk.arrayFrom<obj>

        headers
        |> Array.map (fun pair ->
            let first = S2Sdk.prop<obj> pair "0"
            let second = S2Sdk.prop<obj> pair "1"

            match format with
            | ReadString -> StringHeader(S2Sdk.stringValue first, S2Sdk.stringValue second)
            | ReadBytes -> BytesHeader(unbox<byte[]> first, unbox<byte[]> second))
        |> Array.toList

    let private readRecord (format: S2ReadFormat) (raw: obj) : S2ReadRecord =
        { SeqNum = S2Sdk.prop<float> raw "seqNum"
          Body = recordBody raw format
          Headers = recordHeaders raw format
          Timestamp = epochDateTime (S2Sdk.prop<obj> raw "timestamp") }

    let private readBatch (format: S2ReadFormat) (raw: obj) : S2ReadBatch =
        let records =
            S2Sdk.prop<obj> raw "records"
            |> S2Sdk.arrayFrom<obj>
            |> Array.map (readRecord format)
            |> Array.toList

        let tail = S2Sdk.prop<obj> raw "tail" |> optionDateTime

        { Records = records
          Tail =
            match tail with
            | None -> None
            | Some _ -> Some(position (S2Sdk.prop<obj> raw "tail")) }

    let private basinInfo (raw: obj) : S2BasinInfo =
        { Name = S2Sdk.prop<string> raw "name"
          Location = optionString (S2Sdk.prop<obj> raw "location")
          CreatedAt = epochDateTime (S2Sdk.prop<obj> raw "createdAt")
          DeletedAt = optionDateTime (S2Sdk.prop<obj> raw "deletedAt") }

    let private streamInfo (raw: obj) : S2StreamInfo =
        { Name = S2Sdk.prop<string> raw "name"
          CreatedAt = epochDateTime (S2Sdk.prop<obj> raw "createdAt")
          DeletedAt = optionDateTime (S2Sdk.prop<obj> raw "deletedAt")
          Cipher = optionString (S2Sdk.prop<obj> raw "cipher") }

    [<RequireQualifiedAccess>]
    module AppendRecord =

        let string (body: string) : S2AppendRecord =
            StringRecord
                { Body = body
                  Headers = []
                  Timestamp = None }

        let stringWith (body: string) (headers: (string * string) list) (timestamp: DateTime option) : S2AppendRecord =
            StringRecord
                { Body = body
                  Headers = headers
                  Timestamp = timestamp }

        let bytes (body: byte[]) : S2AppendRecord =
            BytesRecord
                { Body = body
                  Headers = []
                  Timestamp = None }

        let bytesWith (body: byte[]) (headers: (byte[] * byte[]) list) (timestamp: DateTime option) : S2AppendRecord =
            BytesRecord
                { Body = body
                  Headers = headers
                  Timestamp = timestamp }

    [<RequireQualifiedAccess>]
    module Basins =

        let list (request: S2PageRequest option) : Effect<S2ListBasinsResponse, S2Error, Context> =
            withClient (fun client ->
                tryPromise (fun () -> S2Sdk.listBasins client.Raw (pageRequest request) S2Sdk.undefinedObj) (fun raw ->
                    { Basins =
                        S2Sdk.prop<obj> raw "basins"
                        |> S2Sdk.arrayFrom<obj>
                        |> Array.map basinInfo
                        |> Array.toList
                      HasMore = S2Sdk.prop<bool> raw "hasMore" }))

        let ensure (basinName: string) : Effect<unit, S2Error, Context> =
            withClient (fun client ->
                let args = createObj [ "basin" ==> basinName ]
                tryPromise (fun () -> S2Sdk.ensureBasin client.Raw args S2Sdk.undefinedObj) (fun _ -> ()))

    [<RequireQualifiedAccess>]
    module Streams =

        let list (basinName: string) (request: S2PageRequest option) : Effect<S2ListStreamsResponse, S2Error, Context> =
            withClient (fun client ->
                let basin = S2Sdk.basin client.Raw basinName

                tryPromise (fun () -> S2Sdk.listStreams basin (pageRequest request) S2Sdk.undefinedObj) (fun raw ->
                    { Streams =
                        S2Sdk.prop<obj> raw "streams"
                        |> S2Sdk.arrayFrom<obj>
                        |> Array.map streamInfo
                        |> Array.toList
                      HasMore = S2Sdk.prop<bool> raw "hasMore" }))

        let create (request: S2CreateStreamRequest) : Effect<S2StreamInfo, S2Error, Context> =
            withClient (fun client ->
                let basin = S2Sdk.basin client.Raw request.Basin

                let args =
                    [ Some("stream" ==> request.Stream); boxOption "config" request.Config ]
                    |> List.choose id
                    |> createObj

                tryPromise (fun () -> S2Sdk.createStream basin args (requestOptions request.RequestOptions)) streamInfo)

        let ensure (target: S2StreamRef) : Effect<unit, S2Error, Context> =
            withClient (fun client ->
                let basin = S2Sdk.basin client.Raw target.Basin
                let args = createObj [ "stream" ==> target.Stream ]
                tryPromise (fun () -> S2Sdk.ensureStream basin args S2Sdk.undefinedObj) (fun _ -> ()))

        let delete (target: S2StreamRef) : Effect<unit, S2Error, Context> =
            withClient (fun client ->
                let basin = S2Sdk.basin client.Raw target.Basin
                let args = createObj [ "stream" ==> target.Stream ]
                tryPromise (fun () -> S2Sdk.deleteStream basin args S2Sdk.undefinedObj) id)

    [<RequireQualifiedAccess>]
    module Stream =

        let checkTail (target: S2StreamRef) : Effect<S2TailResponse, S2Error, Context> =
            withClient (fun client ->
                let stream = streamHandle target client
                tryPromise (fun () -> S2Sdk.checkTail stream S2Sdk.undefinedObj) tailResponse)

        let tail (target: S2StreamRef) : Effect<S2StreamPosition, S2Error, Context> =
            checkTail target |> Effect.map (fun response -> response.Tail)

        let append (request: S2AppendRequest) : Effect<S2AppendAck, S2Error, Context> =
            withClient (fun client ->
                let stream = streamHandle request.Target client
                let input = appendInput request

                tryPromise (fun () -> S2Sdk.append stream input (requestOptions request.RequestOptions)) appendAck)

        let appendRecords
            (target: S2StreamRef)
            (records: S2AppendRecord list)
            (options: S2AppendOptions option)
            : Effect<S2AppendAck, S2Error, Context> =
            append
                { Target = target
                  Records = records
                  Options = options
                  RequestOptions = None }

        let appendString
            (target: S2StreamRef)
            (body: string)
            (options: S2AppendOptions option)
            : Effect<S2AppendAck, S2Error, Context> =
            appendRecords target [ AppendRecord.string body ] options

        let appendJsonString
            (target: S2StreamRef)
            (body: string)
            (matchSeqNum: float option)
            : Effect<S2AppendAck, S2Error, Context> =
            appendString
                target
                body
                (Some
                    { S2AppendOptions.Empty with
                        MatchSeqNum = matchSeqNum })

        let read (request: S2ReadRequest) : Effect<S2ReadBatch, S2Error, Context> =
            withClient (fun client ->
                let stream = streamHandle request.Target client
                let input = readInput request
                let options = readOptions request

                tryPromise (fun () -> S2Sdk.read stream input options) (readBatch request.Format))

        let readStrings
            (target: S2StreamRef)
            (start: S2ReadStart option)
            (stop: S2ReadStop option)
            : Effect<S2ReadBatch, S2Error, Context> =
            read
                { Target = target
                  Start = start
                  Clamp = None
                  Stop = stop
                  IgnoreCommandRecords = None
                  Format = ReadString
                  RequestOptions = None }

        let readSession (request: S2ReadRequest) : Effect.Stream<S2ReadRecord, S2Error, Context> =
            Effect.Stream.unwrap (
                withClient (fun client ->
                    let stream = streamHandle request.Target client
                    let input = readInput request
                    let options = readOptions request

                    tryPromise (fun () -> S2Sdk.readSession stream input options) (fun iterable ->
                        Effect.Stream.fromAsyncIterableJS iterable toError
                        |> Effect.Stream.map (readRecord request.Format)))
            )
