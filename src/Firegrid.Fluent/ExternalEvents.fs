namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Core

// ============================================================
// externalEvents.ts — awakeable tokens, awakeable / workflowEvent,
// resolve / reject / resolveWorkflowEvent, deliver helpers.
// ============================================================

/// `AwakeableToken`.
type AwakeableToken =
    { Tag: string // "FiregridAwakeable"
      Name: string
      RunId: string
      SignalId: string
      StepId: string }

/// `Awakeable<T>` — effect-yielding handle.
type Awakeable =
    { Await: Effect<obj, exn, Context>
      Id: string
      Promise: Effect<obj, exn, Context>
      Effect: Effect<obj, exn, Context> }

/// `AwakeableOptions`.
type AwakeableOptions =
    { Id: string option
      Name: string option }

    static member Empty = { Id = None; Name = None }

/// `AwakeableResolveOptions` (and `AwakeableRejectOptions`, identical shape).
type AwakeableResolveOptions =
    { Metadata: obj option
      SignalId: string option }

    static member Empty = { Metadata = None; SignalId = None }

type AwakeableRejectOptions = AwakeableResolveOptions

/// `WorkflowEventReference`.
type WorkflowEventReference =
    { Name: string
      RunId: string
      SignalId: string option
      StepId: string option }

/// `WorkflowEvent<T>` (the externalEvents one) — extends `WorkflowEventReference`.
type FluentWorkflowEvent =
    { Await: Effect<obj, exn, Context>
      Promise: Effect<obj, exn, Context>
      Effect: Effect<obj, exn, Context>
      Name: string
      RunId: string
      SignalId: string option
      StepId: string }

/// `WorkflowEventOptions`.
type WorkflowEventOptions =
    { Id: string option }

    static member Empty = { Id = None }

/// `ResolveWorkflowEventOptions`.
type ResolveWorkflowEventOptions =
    { Metadata: obj option
      SignalId: string option }

    static member Empty = { Metadata = None; SignalId = None }

/// `AwakeableRejected` — `Data.TaggedError("AwakeableRejected")`.
[<RequireQualifiedAccess>]
module AwakeableRejected =

    [<Emit("""(() => {
  const e = new Error('AwakeableRejected');
  e.name = 'AwakeableRejected';
  e._tag = 'AwakeableRejected';
  e.id = $0;
  e.reason = $1;
  return e;
})()""")>]
    let create (_id: string) (_reason: obj) : exn = jsNative

    [<Emit("$0 instanceof Error && $0._tag === 'AwakeableRejected'")>]
    let is (_value: obj) : bool = jsNative

[<RequireQualifiedAccess>]
module ExternalEvents =

    let private tokenPrefix = "ffg_awakeable:"

    let private awakeableSignalName (stepId: string) : string = sprintf "__firegrid_awakeable:%s" stepId

    let private workflowEventSignalName (name: string) : string = sprintf "__firegrid_workflow_event:%s" name

    /// `encodeToken(token)` = `${prefix}${encodeURIComponent(JSON.stringify(token))}`.
    let private encodeToken (token: AwakeableToken) : string =
        let o =
            createObj
                [ "_tag" ==> token.Tag
                  "name" ==> token.Name
                  "runId" ==> token.RunId
                  "signalId" ==> token.SignalId
                  "stepId" ==> token.StepId ]

        sprintf "%s%s" tokenPrefix (FluentSdk.encodeURIComponent (FluentSdk.jsonStringify o))

    [<Emit("(() => { throw new Error($0); })()")>]
    let private throwError (_message: string) : 'a = jsNative

    /// `decodeAwakeableToken(id)` — strict validation, throws on invalid.
    let decodeAwakeableToken (id: string) : AwakeableToken =
        if not (FluentSdk.startsWith id tokenPrefix) then
            throwError "invalid Firegrid awakeable token"

        let json = FluentSdk.decodeURIComponent (FluentSdk.sliceFrom id tokenPrefix.Length)
        let parsed = FluentSdk.jsonParse<obj> json

        let getStr (key: string) = FluentSdk.prop<obj> parsed key

        let validString (v: obj) = FluentSdk.isString v

        if
            FluentSdk.stringValue (getStr "_tag") <> "FiregridAwakeable"
            || not (validString (getStr "name"))
            || not (validString (getStr "runId"))
            || not (validString (getStr "signalId"))
            || not (validString (getStr "stepId"))
        then
            throwError "invalid Firegrid awakeable token payload"

        { Tag = "FiregridAwakeable"
          Name = FluentSdk.stringValue (getStr "name")
          RunId = FluentSdk.stringValue (getStr "runId")
          SignalId = FluentSdk.stringValue (getStr "signalId")
          StepId = FluentSdk.stringValue (getStr "stepId") }

    /// `awakeable(options?)`.
    let awakeable (options: AwakeableOptions) : Effect<Awakeable, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.RunId with
            | None -> Effect.fail (FluentFiregridError.create "awakeable requires a durable run id")
            | Some runId ->
                let name = options.Name |> Option.defaultValue "awakeable"

                let stepId =
                    match options.Id with
                    | Some id -> id
                    | None ->
                        match ctx.SignalOperationId with
                        | Some f -> f { Kind = "awakeable"; Name = name }
                        | None -> sprintf "%s:awakeable:%s" runId name

                let signalName = awakeableSignalName stepId

                let token =
                    encodeToken
                        { Tag = "FiregridAwakeable"
                          Name = signalName
                          RunId = runId
                          SignalId = sprintf "awakeable:%s" stepId
                          StepId = stepId }

                let waitOptions: WaitForEventOptions =
                    { WaitForEventOptions.Empty with Id = Some stepId }

                let effect =
                    ctx.WaitForSignal signalName (Some waitOptions)
                    |> Effect.flatMap (fun payload ->
                        if FluentSdk.stringValue (FluentSdk.prop<obj> payload "_tag") = "AwakeableRejected" then
                            Effect.fail (AwakeableRejected.create token (FluentSdk.prop<obj> payload "reason"))
                        else
                            Effect.succeed (FluentSdk.prop<obj> payload "value"))

                Effect.succeed
                    { Await = effect
                      Effect = effect
                      Id = token
                      Promise = effect })

    /// `workflowEvent(name, options?)`.
    let workflowEvent (name: string) (options: WorkflowEventOptions) : Effect<FluentWorkflowEvent, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.RunId with
            | None -> Effect.fail (FluentFiregridError.create "workflowEvent requires a durable run id")
            | Some runId ->
                let stepId =
                    match options.Id with
                    | Some id -> id
                    | None ->
                        match ctx.SignalOperationId with
                        | Some f -> f { Kind = "workflowEvent"; Name = name }
                        | None -> sprintf "%s:workflowEvent:%s" runId name

                let signalName = workflowEventSignalName name

                let waitOptions: WaitForEventOptions =
                    { WaitForEventOptions.Empty with Id = Some stepId }

                let effect = ctx.WaitForSignal signalName (Some waitOptions)

                Effect.succeed
                    { Await = effect
                      Effect = effect
                      Name = name
                      Promise = effect
                      RunId = runId
                      SignalId = Some(sprintf "workflow-event:%s:%s" runId name)
                      StepId = stepId })

    // ── delivery helpers ──────────────────────────────────────────────

    /// `deliverWorkflowEvent(binding, reference, value, options)`.
    let private deliverWorkflowEvent
        (binding: ExternalSignalBinding)
        (reference: WorkflowEventReference)
        (value: obj)
        (options: ResolveWorkflowEventOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        let signalId =
            match options |> Option.bind (fun o -> o.SignalId) with
            | Some s -> s
            | None ->
                match reference.SignalId with
                | Some s -> s
                | None -> sprintf "workflow-event:%s:%s" reference.RunId reference.Name

        binding.DeliverSignal
            { Name = workflowEventSignalName reference.Name
              Payload = value
              RunId = reference.RunId
              SignalId = signalId
              StepId = reference.StepId
              Metadata = options |> Option.bind (fun o -> o.Metadata) }

    /// `deliverAwakeable(binding, id, payload, options)`.
    let private deliverAwakeable
        (binding: ExternalSignalBinding)
        (id: string)
        (payload: obj)
        (options: AwakeableResolveOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        let tokenResult =
            Effect.sync (fun () ->
                try
                    Ok(decodeAwakeableToken id)
                with cause ->
                    Error(FluentFiregridError.createWithCause "invalid awakeable token" (box cause)))

        tokenResult
        |> Effect.flatMap (function
            | Error err -> Effect.fail err
            | Ok token ->
                let signalId =
                    match options |> Option.bind (fun o -> o.SignalId) with
                    | Some s -> s
                    | None -> token.SignalId

                binding.DeliverSignal
                    { Name = token.Name
                      Payload = payload
                      RunId = token.RunId
                      SignalId = signalId
                      StepId = Some token.StepId
                      Metadata = options |> Option.bind (fun o -> o.Metadata) })

    let private resolvedPayload (value: obj) : obj =
        createObj [ "_tag" ==> "AwakeableResolved"; "value" ==> value ]

    let private rejectedPayload (reason: obj) : obj =
        createObj [ "_tag" ==> "AwakeableRejected"; "reason" ==> reason ]

    // ── resolveAwakeable (binding + ambient overloads) ────────────────

    let resolveAwakeableWithBinding
        (binding: ExternalSignalBinding)
        (id: string)
        (value: obj)
        (options: AwakeableResolveOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        deliverAwakeable binding id (resolvedPayload value) options

    /// `resolveAwakeable(id, value, options?)` — ambient context.
    let resolveAwakeable
        (id: string)
        (value: obj)
        (options: AwakeableResolveOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.ExternalSignals with
            | None -> Effect.fail (FluentFiregridError.create "resolveAwakeable requires an external signal binding")
            | Some signals -> deliverAwakeable signals id (resolvedPayload value) options)

    // ── rejectAwakeable ───────────────────────────────────────────────

    let rejectAwakeableWithBinding
        (binding: ExternalSignalBinding)
        (id: string)
        (reason: obj)
        (options: AwakeableRejectOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        deliverAwakeable binding id (rejectedPayload reason) options

    /// `rejectAwakeable(id, reason, options?)` — ambient context.
    let rejectAwakeable
        (id: string)
        (reason: obj)
        (options: AwakeableRejectOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.ExternalSignals with
            | None -> Effect.fail (FluentFiregridError.create "rejectAwakeable requires an external signal binding")
            | Some signals -> deliverAwakeable signals id (rejectedPayload reason) options)

    // ── resolveWorkflowEvent ──────────────────────────────────────────

    let resolveWorkflowEventWithBinding
        (binding: ExternalSignalBinding)
        (reference: WorkflowEventReference)
        (value: obj)
        (options: ResolveWorkflowEventOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        deliverWorkflowEvent binding reference value options

    /// `resolveWorkflowEvent(reference, value, options?)` — ambient context.
    let resolveWorkflowEvent
        (reference: WorkflowEventReference)
        (value: obj)
        (options: ResolveWorkflowEventOptions option)
        : Effect<ExternalSignalDelivery, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.ExternalSignals with
            | None ->
                Effect.fail (FluentFiregridError.create "resolveWorkflowEvent requires an external signal binding")
            | Some signals -> deliverWorkflowEvent signals reference value options)
