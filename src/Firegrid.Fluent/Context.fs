namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Core

// ============================================================
// context.ts (+ binding/request types from clients.ts that context
// structurally depends on — defined here to break the TS import cycle
// between context.ts and clients.ts).
// ============================================================

/// `CallRequest<Input>` (clients.ts) — the invocation envelope.
type CallRequest =
    { Kind: DefinitionKind
      Name: string
      Handler: string
      Key: string option
      Input: obj
      RunId: string option
      IdempotencyKey: string option
      DelayMs: float option
      Metadata: obj option
      Descriptor: HandlerDescriptor option }

    static member Create(kind, name, handler, input) =
        { Kind = kind
          Name = name
          Handler = handler
          Key = None
          Input = input
          RunId = None
          IdempotencyKey = None
          DelayMs = None
          Metadata = None
          Descriptor = None }

/// `SendRequest<Input> = CallRequest<Input>`.
type SendRequest = CallRequest

/// `SendReference<Output>`.
type SendReference =
    { Handler: string option
      InvocationId: string
      Key: string option
      Kind: DefinitionKind option
      Name: string option
      Output: obj option }

/// `InvocationBinding<Error, Requirements>` — `call` and `send`. The error type
/// across the fluent surface is Core's `FluentFiregridError` (`exn`).
type InvocationBinding =
    { Call: CallRequest -> Effect<obj, exn, Context>
      Send: SendRequest -> Effect<SendReference, exn, Context> }

/// `ExternalSignalDeliveryRequest<Payload>`.
type ExternalSignalDeliveryRequest =
    { RunId: string
      SignalId: string
      StepId: string option
      Name: string
      Payload: obj
      Metadata: obj option }

/// `ExternalSignalDelivery`.
type ExternalSignalDelivery =
    { Kind: string
      RunId: string
      WorkflowId: string option }

/// `ExternalSignalBinding<Error, Requirements>`.
type ExternalSignalBinding =
    { DeliverSignal: ExternalSignalDeliveryRequest -> Effect<ExternalSignalDelivery, exn, Context> }

// ── state wait option bags ────────────────────────────────────────────

/// `StateWaitBackendOptions`.
type StateWaitBackendOptions =
    { EnvironmentVersion: string option
      Name: string
      SignalName: string
      TimeoutAt: float option
      TimeoutMs: float option
      WaitId: string option }

/// `StateIndexWaitBackendOptions extends StateWaitBackendOptions`.
type StateIndexWaitBackendOptions =
    { EnvironmentVersion: string option
      Name: string
      SignalName: string
      TimeoutAt: float option
      TimeoutMs: float option
      WaitId: string option
      Index: string[]
      IndexKey: string
      Vars: obj }

/// `SignalOperationIdentityInput`.
type SignalOperationIdentityInput =
    { Kind: string // "awakeable" | "workflowEvent"
      Name: string }

/// `StateOperationIdentityInput`.
type StateOperationIdentityInput =
    { Kind: string // "get" | "set" | "delete" | "waitFor"
      Table: string
      Key: string }

/// `RunActionContext`. `Signal` is the JS `AbortSignal`, typed `obj` here
/// (Core's `CoreSdk.AbortSignal` alias is internal to the Core assembly).
type RunActionContext =
    { Id: string
      Attempt: int
      Signal: obj }

/// `RunAction<A>` — `(context) => A | PromiseLike<A> | Effect<A, unknown, never>`.
/// Held as an `obj`-returning function; the durable context normalizes it.
type RunAction = RunActionContext -> obj

/// `ObjectStateBackend` — `Option.Option<unknown>` is `obj option` in F#.
type ObjectStateBackend =
    { Get: string -> string -> (string option) -> Effect<obj option, exn, Context> // (table, key, readId)
      Set: string -> string -> obj -> (string option) -> Effect<unit, exn, Context> // (table, key, value, opId)
      Delete: string -> string -> (string option) -> Effect<unit, exn, Context> // (table, key, opId)
      WaitFor: (string -> string -> StatePredicate -> StateWaitBackendOptions -> Effect<obj option, exn, Context>) option
      WaitForIndex: (string -> StatePredicate -> StateIndexWaitBackendOptions -> Effect<obj option, exn, Context>) option }

/// `FluentDurableContextService`.
type FluentDurableContextService =
    { Binding: InvocationBinding option
      ExternalSignals: ExternalSignalBinding option
      Key: string option
      RunId: string option
      State: ObjectStateBackend option
      SignalOperationId: (SignalOperationIdentityInput -> string) option
      StateOperationId: (StateOperationIdentityInput -> string) option
      Step: string -> RunAction -> StepOptions option -> Effect<obj, exn, Context>
      Now: (DeterministicValueOptions option -> Effect<float, exn, Context>) option
      Sleep: float -> SleepOptions option -> Effect<unit, exn, Context>
      SleepUntil: float -> SleepOptions option -> Effect<unit, exn, Context>
      WaitForSignal: string -> WaitForEventOptions option -> Effect<obj, exn, Context> }

[<RequireQualifiedAccess>]
module FluentDurableContext =

    /// `FluentDurableContext` service tag
    /// (`@firegrid/fluent/context/FluentDurableContext`).
    let tag: Tag<FluentDurableContextService> =
        Tag.make<FluentDurableContextService> "@firegrid/fluent/context/FluentDurableContext"

    /// `Effect.service` for the context tag.
    let service: Effect<FluentDurableContextService, exn, Context> = Effect.service tag

    /// `FluentDurableContext.pipe(Effect.flatMap(f))` helper.
    let withContext (f: FluentDurableContextService -> Effect<'A, exn, Context>) : Effect<'A, exn, Context> =
        service |> Effect.flatMap f

/// `TanStackWorkflowContext` — the host ctx handed in. Methods return JS
/// promises (plain async, mirroring the TanStack durable ctx).
type TanStackWorkflowContext =
    { RunId: string option
      Step: string -> (StepContext -> obj) -> StepOptions option -> JS.Promise<obj>
      Sleep: float -> SleepOptions option -> JS.Promise<unit>
      SleepUntil: float -> SleepOptions option -> JS.Promise<unit>
      Now: (DeterministicValueOptions option -> JS.Promise<float>) option
      WaitForEvent: string -> WaitForEventOptions option -> JS.Promise<obj> }

/// Options bag for `fluentContextFromTanStack`.
type FluentContextFromTanStackOptions =
    { Binding: InvocationBinding option
      ExternalSignals: ExternalSignalBinding option
      Key: string option
      State: ObjectStateBackend option }

    static member Empty =
        { Binding = None
          ExternalSignals = None
          Key = None
          State = None }

[<RequireQualifiedAccess>]
module Context =

    // `Effect.isEffect(value)` — whether a RunAction returned an Effect. EffSharp
    // Effects are tagged; we delegate to EffSharp's runtime check via interop.
    [<Emit("$0 != null && (typeof $0 === 'object' || typeof $0 === 'function') && (('_op' in $0) || ('_tag' in $0 && String($0._tag).startsWith('Effect')) || ('pipe' in $0 && '_id' in $0))")>]
    let private looksLikeEffect (_value: obj) : bool = jsNative

    /// Run an EffSharp Effect to a JS promise (used to normalize a RunAction's
    /// Effect result back into the TanStack ctx's promise contract).
    [<Emit("$0")>]
    let private asEffect (_value: obj) : Effect<obj, exn, unit> = jsNative

    let private runEffectToPromise (value: obj) : JS.Promise<obj> =
        Effect.runPromise (asEffect value)

    [<Emit("Promise.resolve($0)")>]
    let private promiseResolve (_value: obj) : JS.Promise<obj> = jsNative

    /// `fluentContextFromTanStack(ctx, options)`.
    let fluentContextFromTanStack
        (ctx: TanStackWorkflowContext)
        (options: FluentContextFromTanStackOptions)
        : FluentDurableContextService =
        let mutable nextSignalOperation = 0
        let mutable nextStateOperation = 0
        let runId = ctx.RunId |> Option.defaultValue "unknown-run"

        { Binding = options.Binding
          ExternalSignals = options.ExternalSignals
          Key = options.Key
          RunId = Some runId
          State = options.State
          SignalOperationId =
            Some(fun input ->
                let n = nextSignalOperation
                nextSignalOperation <- nextSignalOperation + 1
                sprintf "%s:signal:%d:%s:%s" runId n input.Kind input.Name)
          StateOperationId =
            Some(fun input ->
                let n = nextStateOperation
                nextStateOperation <- nextStateOperation + 1
                sprintf "%s:state:%d:%s:%s:%s" runId n input.Kind input.Table input.Key)
          Now =
            match ctx.Now with
            | None -> None
            | Some now ->
                Some(fun options ->
                    Effect.tryPromiseJS
                        (fun () -> now options)
                        (fun cause -> FluentFiregridError.createWithCause "now failed" (box cause)))
          Sleep =
            fun ms options ->
                Effect.tryPromiseJS
                    (fun () -> ctx.Sleep ms options)
                    (fun cause -> FluentFiregridError.createWithCause (sprintf "sleep(%g) failed" ms) (box cause))
          SleepUntil =
            fun timestamp options ->
                Effect.tryPromiseJS
                    (fun () -> ctx.SleepUntil timestamp options)
                    (fun cause ->
                        FluentFiregridError.createWithCause (sprintf "sleepUntil(%g) failed" timestamp) (box cause))
          WaitForSignal =
            fun name options ->
                Effect.tryPromiseJS
                    (fun () -> ctx.WaitForEvent name options)
                    (fun cause -> FluentFiregridError.createWithCause (sprintf "waitForSignal(%s) failed" name) (box cause))
          Step =
            fun name action options ->
                Effect.tryPromiseJS
                    (fun () ->
                        ctx.Step
                            name
                            (fun (stepContext: StepContext) ->
                                let runActionContext: RunActionContext =
                                    { Id = stepContext.Id
                                      Attempt = stepContext.Attempt
                                      Signal = box stepContext.Signal }

                                let value = action runActionContext
                                // `Effect.isEffect(value) ? Effect.runPromise(value) : Promise.resolve(value)`
                                if looksLikeEffect value then
                                    box (runEffectToPromise value)
                                else
                                    box (promiseResolve value))
                            options)
                    (fun cause -> FluentFiregridError.createWithCause (sprintf "step %s failed" name) (box cause)) }
