namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Fable.Core.JS
open Firegrid.Core

// ============================================================
// http.ts — awakeable HTTP client + fluent HTTP handler (routing).
//
// Plain async / Promise logic ported faithfully via Fable.Promise. Effects are
// run with `Effect.runPromise`. REDUCTION: `decodeInput`/`encodeOutput` use
// effect/Schema codecs in the TS; here they are pass-through (the descriptor's
// schema, held as `obj`, is not applied). The `AwakeableHttpClientError` is a
// JS tagged error rather than a Schema TaggedError.
// ============================================================

/// `AwakeableHttpClientOptions`. `Headers` source: a HeadersInit object or a
/// thunk returning HeadersInit (or a promise of it). Held as `obj`.
type AwakeableHttpClientOptions =
    { BaseUrl: obj // string | URL
      Fetch: obj option // typeof fetch
      Headers: obj option }

/// `AwakeableHttpClient`.
type AwakeableHttpClient =
    { Reject: string -> obj -> Promise<ExternalSignalDelivery>
      Resolve: string -> obj -> Promise<ExternalSignalDelivery> }

/// `FluentHttpHandlerOptions`.
type FluentHttpHandlerOptions =
    { Binding: InvocationBinding
      Definitions: Definition[]
      ExternalSignals: ExternalSignalBinding option }

/// `RouteMatch`.
type private RouteMatch =
    { Handler: string
      Key: string option
      Kind: DefinitionKind
      Mode: string // "call" | "send"
      Name: string }

/// `RouteTarget`.
type private RouteTarget =
    { Definition: Definition
      Descriptor: HandlerDescriptor option
      Route: RouteMatch }

/// `ExternalEventRoute`.
type private ExternalEventRoute =
    { Action: string // "reject" | "resolve"
      Id: string }

[<RequireQualifiedAccess>]
module Http =

    [<Emit("$0")>]
    let private asUnitEffect (_value: obj) : Effect<obj, exn, unit> = jsNative

    [<Emit("(() => { throw ($0); })()")>]
    let private throwValue (_value: obj) : 'a = jsNative

    // ── AwakeableHttpClientError (tagged JS error) ────────────────────

    [<Emit("""(() => {
  const e = new Error($1);
  e.name = 'AwakeableHttpClientError';
  e._tag = 'AwakeableHttpClientError';
  e.body = $0;
  e.status = $2;
  return e;
})()""")>]
    let private makeAwakeableHttpClientError (_body: obj) (_message: string) (_status: int) : exn = jsNative

    // ── awakeable URL / headers ───────────────────────────────────────

    /// `awakeableUrl(baseUrl, id, action)`.
    let private awakeableUrl (baseUrl: obj) (id: string) (action: string) : FluentSdk.Url =
        let url = FluentSdk.newUrl baseUrl
        let pathname = FluentSdk.urlPathname url

        let basePath =
            if FluentSdk.endsWith pathname "/" then
                FluentSdk.sliceRange pathname 0 (pathname.Length - 1)
            else
                pathname

        FluentSdk.setUrlPathname
            url
            (sprintf "%s/firegrid/awakeables/%s/%s" basePath (FluentSdk.encodeURIComponent id) action)

        FluentSdk.setUrlSearch url ""
        url

    // `await x` where x may be a value or a promise (Fable awaits both via `let!`).
    [<Emit("Promise.resolve($0)")>]
    let private awaitable (_value: obj) : Promise<obj> = jsNative

    /// `awakeableHeaders(source)`.
    let private awakeableHeaders (source: obj option) : Promise<FluentSdk.Headers> =
        promise {
            let headers = FluentSdk.newHeaders (createObj [ "content-type" ==> "application/json" ])

            let! resolved =
                match source with
                | None -> awaitable FluentSdk.undefinedValue
                | Some src ->
                    if FluentSdk.isFunction src then
                        // `await source()` — handles sync or promise returns.
                        awaitable ((unbox<unit -> obj> src) ())
                    else
                        awaitable src

            if not (FluentSdk.isUndefined resolved) then
                FluentSdk.headersForEach (FluentSdk.newHeaders resolved) (fun value key -> FluentSdk.headersSet headers key value)

            return headers
        }

    /// `createAwakeableHttpClient(options)`.
    let createAwakeableHttpClient (options: AwakeableHttpClientOptions) : AwakeableHttpClient =
        let fetchImpl =
            match options.Fetch with
            | Some f -> f
            | None -> box FluentSdk.globalFetch

        let post (id: string) (action: string) (body: obj) : Promise<ExternalSignalDelivery> =
            promise {
                let! headers = awakeableHeaders options.Headers

                let init =
                    createObj
                        [ "body" ==> FluentSdk.jsonStringify body
                          "headers" ==> headers
                          "method" ==> "POST" ]

                let! response = FluentSdk.callFetch fetchImpl (awakeableUrl options.BaseUrl id action) init

                // `await response.json().catch(() => undefined)`
                let! payload =
                    promise {
                        try
                            let! p = FluentSdk.responseJson response
                            return p
                        with _ ->
                            return FluentSdk.undefinedValue
                    }

                if not (FluentSdk.responseOk response) then
                    let status = FluentSdk.responseStatus response

                    return
                        throwValue (
                            box (
                                makeAwakeableHttpClientError
                                    payload
                                    (sprintf "awakeable HTTP delivery failed with status %d" status)
                                    status
                            )
                        )
                else
                    return unbox<ExternalSignalDelivery> payload
            }

        { Reject = fun id reason -> post id "reject" (createObj [ "reason" ==> reason ])
          Resolve = fun id value -> post id "resolve" (createObj [ "value" ==> value ]) }

    // ── result helper (Ok/Error tagged like the TS `Result<A>`) ───────

    type private Result =
        | Ok of obj
        | Err of string

    // ── routing ───────────────────────────────────────────────────────

    let keyFor (kind: DefinitionKind) (name: string) (handler: string) : string =
        sprintf "%s:%s:%s" kind name handler

    let private isDefinitionKind (value: string) : bool =
        value = "service" || value = "workflow" || value = "object"

    /// `createRegistry(definitions)`.
    let private createRegistry (definitions: Definition[]) : System.Collections.Generic.Dictionary<string, RouteTarget> =
        let registry = System.Collections.Generic.Dictionary<string, RouteTarget>()

        definitions
        |> Array.iter (fun definition ->
            FluentSdk.objectKeys definition.Handlers
            |> Array.iter (fun handler ->
                let descriptor =
                    let d = FluentSdk.prop<obj> definition.Handlers handler
                    if FluentSdk.isUndefined d then None else Some(Definitions.descriptorOfObj d)

                registry.[keyFor definition.Kind definition.Name handler] <-
                    { Definition = definition
                      Descriptor = descriptor
                      Route =
                        { Handler = handler
                          Key = None
                          Kind = definition.Kind
                          Mode = "call"
                          Name = definition.Name } }))

        registry

    let private pathParts (url: FluentSdk.Url) : string[] =
        FluentSdk.split (FluentSdk.urlPathname url) "/"
        |> Array.filter (fun part -> part.Length > 0)
        |> Array.map FluentSdk.decodeURIComponent

    let private partAt (parts: string[]) (index: int) : string option =
        if index < parts.Length then Some parts.[index] else None

    /// `parseExternalEventRoute(url)`.
    let private parseExternalEventRoute (url: FluentSdk.Url) : ExternalEventRoute option =
        let parts = pathParts url

        if partAt parts 0 <> Some "firegrid" || partAt parts 1 <> Some "awakeables" then
            None
        else
            let id = partAt parts 2
            let action = partAt parts 3

            match id, action with
            | Some id, Some action when (action = "resolve" || action = "reject") && parts.Length = 4 ->
                Some { Action = action; Id = id }
            | _ -> None

    /// `parseRoute(url)`.
    let private parseRoute (url: FluentSdk.Url) : RouteMatch option =
        let parts = pathParts url

        match partAt parts 0 with
        | Some mode when mode = "call" || mode = "send" ->
            match partAt parts 1 with
            | Some kind when isDefinitionKind kind ->
                if kind = "object" then
                    match partAt parts 2, partAt parts 3, partAt parts 4 with
                    | Some name, Some key, Some handler when parts.Length = 5 ->
                        Some
                            { Handler = handler
                              Key = Some key
                              Kind = kind
                              Mode = mode
                              Name = name }
                    | _ -> None
                else
                    match partAt parts 2, partAt parts 3 with
                    | Some name, Some handler when parts.Length = 4 ->
                        Some
                            { Handler = handler
                              Key = None
                              Kind = kind
                              Mode = mode
                              Name = name }
                    | _ -> None
            | _ -> None
        | _ -> None

    // ── body / effect helpers ─────────────────────────────────────────

    let private parseJsonBody (request: FluentSdk.Request) : Promise<Result> =
        promise {
            try
                let! value = FluentSdk.requestJson request
                return Ok value
            with cause ->
                let message =
                    if FluentSdk.isError (box cause) then
                        FluentSdk.errorMessage (box cause)
                    else
                        "failed to parse request JSON"

                return Err message
        }

    let private errorMessage (cause: obj) : string =
        if FluentSdk.isError cause then
            FluentSdk.errorMessage cause
        else
            FluentSdk.stringValue cause

    /// `runEffect(effect)` — run to a `Result`.
    let private runEffect (effect: Effect<'a, exn, 'r>) : Promise<Result> =
        promise {
            try
                let! value = Effect.runPromise (asUnitEffect (box effect))
                return Ok value
            with cause ->
                return Err(errorMessage (box cause))
        }

    /// `decodeInput(target, value)` — REDUCTION: schema decode is pass-through.
    let private decodeInput (target: RouteTarget) (value: obj) : Promise<Result> =
        promise {
            match target.Descriptor |> Option.bind (fun d -> d.Input) with
            | None -> return Ok value
            | Some _schema ->
                // Pass-through: the input schema (held as `obj`) is not applied.
                return Ok value
        }

    /// `encodeOutput(target, value)` — REDUCTION: schema encode is pass-through.
    let private encodeOutput (target: RouteTarget) (value: obj) : Promise<Result> =
        promise {
            match target.Descriptor |> Option.bind (fun d -> d.Output) with
            | None -> return Ok value
            | Some _schema -> return Ok value
        }

    /// `invoke(binding, mode, request)`.
    let private invoke (binding: InvocationBinding) (mode: string) (request: CallRequest) : Promise<Result> =
        if mode = "call" then
            runEffect (binding.Call request)
        else
            runEffect (binding.Send request)

    let private readRunId (request: FluentSdk.Request) : string option =
        let url = FluentSdk.newUrl (FluentSdk.requestUrl request)
        let fromQuery = FluentSdk.searchParamsGet url "runId"

        if not (FluentSdk.isNullish fromQuery) then
            Some(FluentSdk.stringValue fromQuery)
        else
            let fromHeader = FluentSdk.headersGet (FluentSdk.requestHeaders request) "x-firegrid-run-id"

            if FluentSdk.isNullish fromHeader then
                None
            else
                Some(FluentSdk.stringValue fromHeader)

    let private jsonResponse (body: obj) (status: int) : FluentSdk.Response =
        FluentSdk.newResponse
            (FluentSdk.jsonStringify body)
            (createObj
                [ "headers" ==> createObj [ "content-type" ==> "application/json" ]
                  "status" ==> status ])

    let private errorResponse (error: string) (status: int) : FluentSdk.Response =
        jsonResponse (createObj [ "error" ==> error ]) status

    let private errorResponseWithMessage (error: string) (message: string) (status: int) : FluentSdk.Response =
        jsonResponse (createObj [ "error" ==> error; "message" ==> message ]) status

    /// `externalEventPayload(action, body)`.
    let private externalEventPayload (action: string) (body: obj) : obj =
        if FluentSdk.isObject body then
            if action = "resolve" && FluentSdk.hasKey body "value" then
                FluentSdk.prop<obj> body "value"
            elif action = "reject" && FluentSdk.hasKey body "reason" then
                FluentSdk.prop<obj> body "reason"
            else
                body
        else
            body

    /// `handleExternalEventRoute(options, request, route)`.
    let private handleExternalEventRoute
        (options: FluentHttpHandlerOptions)
        (request: FluentSdk.Request)
        (route: ExternalEventRoute)
        : Promise<FluentSdk.Response> =
        promise {
            match options.ExternalSignals with
            | None -> return errorResponse "external_signals_not_configured" 500
            | Some signals ->
                let! body = parseJsonBody request

                match body with
                | Err message -> return errorResponseWithMessage "invalid_json" message 400
                | Ok bodyValue ->
                    let value = externalEventPayload route.Action bodyValue

                    let effect =
                        if route.Action = "resolve" then
                            ExternalEvents.resolveAwakeableWithBinding signals route.Id value None
                        else
                            ExternalEvents.rejectAwakeableWithBinding signals route.Id value None

                    let! result = runEffect effect

                    match result with
                    | Err message -> return errorResponseWithMessage "external_signal_delivery_failed" message 500
                    | Ok value -> return jsonResponse value 202
        }

    /// `createFluentHttpHandler(options)` — returns `(Request) => Promise<Response>`
    /// (Request/Response are the JS globals, typed `obj` at the public boundary).
    let createFluentHttpHandler (options: FluentHttpHandlerOptions) : (obj -> Promise<obj>) =
        let registry = createRegistry options.Definitions

        fun (request: obj) ->
            promise {
                if FluentSdk.requestMethod request <> "POST" then
                    return errorResponse "method_not_allowed" 405
                else
                    let url = FluentSdk.newUrl (FluentSdk.requestUrl request)

                    match parseExternalEventRoute url with
                    | Some externalRoute ->
                        let! resp = handleExternalEventRoute options request externalRoute
                        return resp
                    | None ->
                        match parseRoute url with
                        | None -> return errorResponse "not_found" 404
                        | Some route ->
                            match registry.TryGetValue(keyFor route.Kind route.Name route.Handler) with
                            | false, _ -> return errorResponse "not_found" 404
                            | true, target ->
                                if route.Kind = "object" && route.Key.IsNone then
                                    return errorResponse "object_key_required" 400
                                else
                                    let! body = parseJsonBody request

                                    match body with
                                    | Err message -> return errorResponseWithMessage "invalid_json" message 400
                                    | Ok bodyValue ->
                                        let! input = decodeInput target bodyValue

                                        match input with
                                        | Err message -> return errorResponseWithMessage "invalid_input" message 400
                                        | Ok inputValue ->
                                            let runId = readRunId request

                                            let requestEnvelope: CallRequest =
                                                { Kind = route.Kind
                                                  Name = route.Name
                                                  Handler = route.Handler
                                                  Key = route.Key
                                                  Input = inputValue
                                                  RunId = runId
                                                  IdempotencyKey = None
                                                  DelayMs = None
                                                  Metadata = None
                                                  Descriptor = target.Descriptor }

                                            let! result = invoke options.Binding route.Mode requestEnvelope

                                            match result with
                                            | Err message ->
                                                return
                                                    errorResponseWithMessage "fluent_invocation_failed" message 500
                                            | Ok resultValue ->
                                                if route.Mode = "send" then
                                                    return jsonResponse resultValue 202
                                                else
                                                    let! output = encodeOutput target resultValue

                                                    match output with
                                                    | Err message ->
                                                        return
                                                            errorResponseWithMessage "invalid_output" message 500
                                                    | Ok outputValue ->
                                                        return
                                                            jsonResponse
                                                                (createObj [ "output" ==> outputValue ])
                                                                200
            }
