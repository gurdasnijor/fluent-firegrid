namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Fable.Core.JS
open Firegrid.Core
open Firegrid.Runtime

// ============================================================
// bindTanStack.ts — wire fluent definitions into the Runtime.
//
// REDUCTIONS:
//  * `decodeHandlerInput` / `decodeHandlerOutput` use effect/Schema codecs in
//    the TS; here they are pass-through (the descriptor schemas, held as `obj`,
//    are not applied). Documented in summary.
//  * The generator handler (`(input) => FluentGenerator`) is an `obj` JS
//    generator. It is driven via a faithful JS generator runner that, for each
//    yielded Effect, runs it (with FluentDurableContext provided) and resumes
//    the generator with the result — mirroring `Effect.gen`. See `Sdk`-style
//    driver below.
// ============================================================

/// `FluentWorkflowInput`.
type FluentWorkflowInput =
    { Input: obj
      Key: string option
      StateContext: obj option }

/// `FluentDefinitionBindingContext`.
type FluentDefinitionBindingContext =
    { Definition: Definition
      HandlerName: string
      Input: FluentWorkflowInput }

/// `FluentDefinitionBindingOptions`. `externalSignals`/`invocationBinding` may be
/// a value or a thunk returning an optional value; held as `obj option` and
/// resolved by the `*From` helpers (mirroring `typeof x === "function"`).
type FluentDefinitionBindingOptions =
    { ExternalSignals: obj option
      InvocationBinding: obj option
      StateBackendFor: (FluentDefinitionBindingContext -> ObjectStateBackend option) option }

    static member Empty =
        { ExternalSignals = None
          InvocationBinding = None
          StateBackendFor = None }

/// `FluentRuntimeHost` — the host with `runtime.startRun` / `runtime.deliverSignal`.
/// Held as `obj`; methods are called via interop to preserve the exact arg shapes.
type FluentRuntimeHost = obj

[<RequireQualifiedAccess>]
module BindTanStack =

    // ── identity ──────────────────────────────────────────────────────

    /// `workflowIdForHandler(definition, handler)`.
    let workflowIdForHandler (kind: DefinitionKind) (name: string) (handler: string) : string =
        sprintf "%s:%s:%s" kind name handler

    /// `workflowIdForRequest(request)`.
    let private workflowIdForRequest (request: CallRequest) : string =
        sprintf "%s:%s:%s" request.Kind request.Name request.Handler

    // ── option resolution (value | thunk) ─────────────────────────────

    let private resolveThunkable (value: obj option) : obj option =
        match value with
        | None -> None
        | Some v ->
            if FluentSdk.isFunction v then
                let result = (unbox<unit -> obj> v) ()
                if FluentSdk.isNullish result then None else Some result
            else
                Some v

    let private invocationBindingFrom (options: FluentDefinitionBindingOptions) : InvocationBinding option =
        resolveThunkable options.InvocationBinding |> Option.map unbox<InvocationBinding>

    let private externalSignalsFrom (options: FluentDefinitionBindingOptions) : ExternalSignalBinding option =
        resolveThunkable options.ExternalSignals |> Option.map unbox<ExternalSignalBinding>

    // ── schema decode (reduced to pass-through) ───────────────────────

    let private decodeHandlerInput
        (_definition: Definition)
        (_handlerName: string)
        (_descriptor: HandlerDescriptor option)
        (input: obj)
        : Effect<obj, exn, Context> =
        // REDUCTION: descriptor.input Schema decode is pass-through.
        Effect.succeed input

    let private decodeHandlerOutput
        (_definition: Definition)
        (_handlerName: string)
        (_descriptor: HandlerDescriptor option)
        (output: obj)
        : Effect<obj, exn, Context> =
        // REDUCTION: descriptor.output Schema decode is pass-through.
        Effect.succeed output

    // ── generator handler driver ──────────────────────────────────────
    // Drives a JS generator that yields Effects, running each yielded Effect via
    // the provided `runEffect` callback (which runs with FluentDurableContext
    // provided) and resuming the generator with the result. Faithful to the
    // `Effect.gen(() => handler(input))` semantics.

    [<Emit("""(() => {
  const gen = ($0)($1);
  const step = (input, isError) => {
    const res = isError ? gen.throw(input) : gen.next(input);
    if (res.done) return Promise.resolve(res.value);
    return ($2)(res.value).then((v) => step(v, false), (e) => step(e, true));
  };
  return step(undefined, false);
})()""")>]
    let private driveGenerator
        (_handler: obj)
        (_input: obj)
        (_runEffect: obj -> Promise<obj>)
        : Promise<obj> =
        jsNative

    [<Emit("$0")>]
    let private asEffectWithContext (_value: obj) : Effect<obj, exn, Context> = jsNative

    // `await x` where x may be a value or promise.
    [<Emit("Promise.resolve($0)")>]
    let private awaitable (_value: obj) : Promise<obj> = jsNative

    // The runtime hands a JS ctx with lowercase methods (step/sleep/now/...).
    // Adapt it into the F# `TanStackWorkflowContext` record via interop, mapping
    // JS member names. `now` is optional.
    let private adaptJsCtx (ctx: obj) : TanStackWorkflowContext =
        { RunId =
            let r = FluentSdk.prop<obj> ctx "runId"
            if FluentSdk.isNullish r then None else Some(FluentSdk.stringValue r)
          Step = fun id fn options -> (FluentSdk.prop<obj> ctx "step" |> unbox<string -> (StepContext -> obj) -> StepOptions option -> Promise<obj>>) id fn options
          Sleep = fun ms options -> (FluentSdk.prop<obj> ctx "sleep" |> unbox<float -> SleepOptions option -> Promise<unit>>) ms options
          SleepUntil = fun ts options -> (FluentSdk.prop<obj> ctx "sleepUntil" |> unbox<float -> SleepOptions option -> Promise<unit>>) ts options
          Now =
            let n = FluentSdk.prop<obj> ctx "now"
            if FluentSdk.isNullish n then
                None
            else
                Some(fun options -> (unbox<DeterministicValueOptions option -> Promise<float>> n) options)
          WaitForEvent = fun name options -> (FluentSdk.prop<obj> ctx "waitForEvent" |> unbox<string -> WaitForEventOptions option -> Promise<obj>>) name options }

    /// Provide a concrete `FluentDurableContextService` and run an effect to a promise.
    let private runWithContext (ctxValue: FluentDurableContextService) (effect: Effect<obj, exn, Context>) : Promise<obj> =
        let layer = Layer.effect FluentDurableContext.tag (Effect.succeed ctxValue)
        Effect.runPromise (Layer.provide layer effect)

    // ── schedules ──────────────────────────────────────────────────────

    /// `scheduledInput(input)` — value or async thunk wrapping into FluentWorkflowInput.
    let private scheduledInput (input: obj option) : obj option =
        match input with
        | None -> Some(box (createObj [ "input" ==> FluentSdk.undefinedValue ]))
        | Some v ->
            if FluentSdk.isFunction v then
                // `async () => ({ input: await input() })`
                let thunk: unit -> Promise<obj> =
                    fun () ->
                        promise {
                            let! resolved = awaitable ((unbox<unit -> obj> v) ())
                            return box (createObj [ "input" ==> resolved ])
                        }

                Some(box thunk)
            else
                Some(box (createObj [ "input" ==> v ]))

    /// `scheduleForWorkflow(entry, workflowId)`.
    let private scheduleForWorkflow (entry: FluentScheduleDefinition) (workflowId: string) : WorkflowScheduleDefinition =
        { Id = entry.Id |> Option.map (fun id -> sprintf "%s:%s" workflowId id)
          Schedule = entry.Schedule
          OverlapPolicy = entry.OverlapPolicy
          Input = scheduledInput entry.Input
          Enabled = entry.Enabled }

    /// `schedulesForHandler(definition, handlerName, workflowId)`.
    let private schedulesForHandler
        (definition: Definition)
        (handlerName: string)
        (workflowId: string)
        : WorkflowScheduleDefinition[] option =
        if definition.Kind <> "workflow" then
            None
        else
            match definition.Schedules with
            | None -> None
            | Some schedules ->
                let matched =
                    schedules
                    |> Array.filter (fun entry -> entry.Handler = handlerName)
                    |> Array.map (fun entry -> scheduleForWorkflow entry workflowId)

                if matched.Length = 0 then None else Some matched

    // ── bind ──────────────────────────────────────────────────────────

    /// `bindFluentDefinitions(definitions, options?)` — builds a
    /// `WorkflowRegistrationMap` (JS object keyed by workflowId).
    let bindFluentDefinitions
        (definitions: Definition[])
        (options: FluentDefinitionBindingOptions)
        : WorkflowRegistrationMap =
        let entries =
            definitions
            |> Array.collect (fun definition ->
                FluentSdk.objectKeys definition.HandlerFns
                |> Array.map (fun handlerName ->
                    let handler = FluentSdk.prop<obj> definition.HandlerFns handlerName
                    let workflowId = workflowIdForHandler definition.Kind definition.Name handlerName

                    // createWorkflow({ id: workflowId }).handler(async (ctx) => ...)
                    let builder = DefineWorkflow.createWorkflow (CreateWorkflowConfig.Create workflowId)

                    let workflowHandler (ctx: Ctx) : Promise<obj> =
                        promise {
                            // `ctx.input as FluentWorkflowInput`
                            let inputRaw = FluentSdk.prop<obj> ctx "input"

                            let input: FluentWorkflowInput =
                                { Input = FluentSdk.prop<obj> inputRaw "input"
                                  Key =
                                    let k = FluentSdk.prop<obj> inputRaw "key"
                                    if FluentSdk.isNullish k then None else Some(FluentSdk.stringValue k)
                                  StateContext =
                                    let s = FluentSdk.prop<obj> inputRaw "stateContext"
                                    if FluentSdk.isNullish s then None else Some s }

                            let state =
                                match options.StateBackendFor with
                                | Some f ->
                                    f
                                        { Definition = definition
                                          HandlerName = handlerName
                                          Input = input }
                                | None -> None

                            let descriptor =
                                let d = FluentSdk.prop<obj> definition.Handlers handlerName
                                if FluentSdk.isUndefined d then None else Some(Definitions.descriptorOfObj d)

                            let externalSignals = externalSignalsFrom options
                            let binding = invocationBindingFrom options

                            let tanCtx = adaptJsCtx ctx

                            let ctxValue =
                                Context.fluentContextFromTanStack
                                    tanCtx
                                    { Binding = binding
                                      ExternalSignals = externalSignals
                                      Key = input.Key
                                      State = state }

                            // runEffect: run a yielded effect with the durable context provided.
                            let runEffect (eff: obj) : Promise<obj> =
                                runWithContext ctxValue (asEffectWithContext eff)

                            // Effect.gen: decode input → run generator → decode output.
                            let! handlerInput =
                                runWithContext ctxValue (decodeHandlerInput definition handlerName descriptor input.Input)

                            let! output = driveGenerator handler handlerInput runEffect

                            let! decoded =
                                runWithContext ctxValue (decodeHandlerOutput definition handlerName descriptor output)

                            return decoded
                        }

                    let workflow = builder.Handler workflowHandler

                    // registration: { load: async () => workflow, ...schedules }
                    let registration = FluentSdk.emptyObject ()
                    FluentSdk.setProp registration "load" (box (fun () -> awaitable (box workflow)))

                    match schedulesForHandler definition handlerName workflowId with
                    | Some schedules -> FluentSdk.setProp registration "schedules" (box schedules)
                    | None -> ()

                    (workflowId, registration)))

        FluentSdk.objectFromEntries entries

    // ── runtime bindings ──────────────────────────────────────────────

    [<Emit("$0.runtime")>]
    let private hostRuntime (_host: FluentRuntimeHost) : obj = jsNative

    [<Emit("$0.deliverSignal")>]
    let private runtimeDeliverSignal (_runtime: obj) : obj = jsNative

    [<Emit("$0.startRun")>]
    let private runtimeStartRun (_runtime: obj) : obj = jsNative

    [<Emit("$0($1)")>]
    let private callMethod (_fn: obj) (_args: obj) : Promise<WorkflowRuntimeRunResult> = jsNative

    /// `createTanStackExternalSignalBinding(host, options?)`.
    let createTanStackExternalSignalBinding
        (host: FluentRuntimeHost)
        (now: (unit -> float) option)
        : ExternalSignalBinding =
        let nowFn = now |> Option.defaultValue FluentSdk.nowMillis
        let runtime = hostRuntime host

        { DeliverSignal =
            fun request ->
                let deliver = runtimeDeliverSignal runtime

                if FluentSdk.isNullish deliver then
                    Effect.fail (
                        FluentFiregridError.create "external signal delivery requires runtime.deliverSignal"
                    )
                else
                    Effect.tryPromiseJS
                        (fun () ->
                            let args = createObj [ "name" ==> request.Name ]

                            match request.Metadata with
                            | Some m -> FluentSdk.setProp args "meta" m
                            | None -> ()

                            FluentSdk.setProp args "now" (box (nowFn ()))
                            FluentSdk.setProp args "payload" request.Payload
                            FluentSdk.setProp args "runId" (box request.RunId)
                            FluentSdk.setProp args "signalId" (box request.SignalId)

                            match request.StepId with
                            | Some s -> FluentSdk.setProp args "stepId" (box s)
                            | None -> ()

                            callMethod deliver args)
                        (fun cause -> FluentFiregridError.createWithCause "external signal delivery failed" (box cause))
                    |> Effect.map (fun result ->
                        { Kind = result.Kind
                          RunId = result.RunId
                          WorkflowId = result.WorkflowId }) }

    /// `createTanStackRuntimeBinding(host, options?)`.
    let createTanStackRuntimeBinding (host: FluentRuntimeHost) (now: (unit -> float) option) : InvocationBinding =
        let mutable nextRun = 0
        let nowFn = now |> Option.defaultValue FluentSdk.nowMillis
        let runtime = hostRuntime host

        let runIdFor (request: CallRequest) : string =
            match request.RunId with
            | Some r -> r
            | None ->
                let n = nextRun
                nextRun <- nextRun + 1
                sprintf "%s:%s:%s:%d" request.Kind request.Name request.Handler n

        let start (request: CallRequest) : Effect<WorkflowRuntimeRunResult, exn, Context> =
            match request.DelayMs with
            | Some delay when delay > 0.0 ->
                Effect.fail (
                    FluentFiregridError.create
                        "delayed fluent invocations require a binding with durable delayed-send support"
                )
            | _ ->
                Effect.tryPromiseJS
                    (fun () ->
                        let fluentInput = createObj [ "input" ==> request.Input ]

                        match request.Key with
                        | Some k -> FluentSdk.setProp fluentInput "key" (box k)
                        | None -> ()

                        let args =
                            createObj
                                [ "input" ==> fluentInput
                                  "now" ==> nowFn ()
                                  "runId" ==> runIdFor request
                                  "workflowId" ==> workflowIdForRequest request ]

                        callMethod (runtimeStartRun runtime) args)
                    (fun cause ->
                        FluentFiregridError.createWithCause "fluent TanStack binding failed to start run" (box cause))

        { Call =
            fun request ->
                start request
                |> Effect.flatMap (fun result ->
                    if result.Kind = "completed" then
                        let output =
                            match result.Run with
                            | Some run -> run.Output |> Option.defaultValue FluentSdk.undefinedValue
                            | None -> FluentSdk.undefinedValue

                        Effect.succeed output
                    else
                        Effect.fail (
                            FluentFiregridError.create (
                                sprintf
                                    "fluent call %s.%s did not complete synchronously: %s"
                                    request.Name
                                    request.Handler
                                    result.Kind
                            )
                        ))
          Send =
            fun request ->
                let invocationId = runIdFor request

                start { request with RunId = Some invocationId }
                |> Effect.map (fun result ->
                    let output =
                        match result.Run with
                        | Some run -> run.Output
                        | None -> None

                    { Handler = None
                      InvocationId = invocationId
                      Key = None
                      Kind = None
                      Name = None
                      Output = output }) }
