namespace Firegrid.Fluent

open Effect
open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Core

// ============================================================
// clients.ts — invocation requests, clients, generic call/send, attach.
//
// REDUCTION: the TS encodes call/send mode, ambient-vs-binding selection, and
// object-key currying entirely in the type system with overloaded call
// signatures. Here that machinery collapses to runtime dispatch (string mode,
// explicit functions, and `key`-aware object client builders), preserving the
// observable behavior. Heavy type-level generics are dropped.
// ============================================================

/// `DurationLike = number | { days?, hours?, milliseconds?, minutes?, seconds? }`.
/// Held as `obj` (a number or a duration object).
type DurationLike = obj

/// `InvocationOptions`.
type InvocationOptions =
    { Delay: DurationLike option
      IdempotencyKey: string option
      Metadata: obj option
      RunId: string option }

    static member Empty =
        { Delay = None
          IdempotencyKey = None
          Metadata = None
          RunId = None }

/// `CallOptions = SendOptions = InvocationOptions`.
type CallOptions = InvocationOptions
type SendOptions = InvocationOptions

/// `InvocationHandle<Output, Error, Requirements> extends SendReference`.
type InvocationHandle =
    { Reference: SendReference
      Attach: unit -> Effect<obj, exn, Context>
      OutputEffect: unit -> Effect<obj, exn, Context> }

/// `GenericInvocationRequest<Input> extends InvocationOptions`.
type GenericInvocationRequest =
    { Handler: string
      Input: obj
      Key: string option
      Kind: DefinitionKind
      Name: string
      Delay: DurationLike option
      IdempotencyKey: string option
      Metadata: obj option
      RunId: string option }

/// `AttachableReference<Output>` — a `SendReference` carrying `handler/kind/name`
/// (required) and an optional `input`. Modeled as `SendReference` plus the
/// request-shaped fields.
type AttachableReference =
    { Handler: string
      InvocationId: string
      Kind: DefinitionKind
      Name: string
      Key: string option
      Input: obj option
      Output: obj option }

/// `ClientMode = "call" | "send"`.
type ClientMode = string

[<RequireQualifiedAccess>]
module Clients =

    /// `duration(input)`.
    let duration (input: DurationLike) : float =
        if FluentSdk.isNumber input then
            FluentSdk.numberValue input
        else
            let pick (key: string) =
                let v = FluentSdk.prop<obj> input key
                if FluentSdk.isNullish v then 0.0 else FluentSdk.numberValue v

            (pick "milliseconds")
            + (pick "seconds") * 1_000.0
            + (pick "minutes") * 60_000.0
            + (pick "hours") * 3_600_000.0
            + (pick "days") * 86_400_000.0

    [<Emit("(() => { throw new Error($0); })()")>]
    let private throwError (_message: string) : 'a = jsNative

    /// `normalizeDuration(input)`.
    let normalizeDuration (input: DurationLike) : float =
        let ms = duration input

        if not (FluentSdk.isFinite ms) || ms < 0.0 then
            throwError "fluent invocation delay must be a non-negative finite duration"
        else
            ms

    /// `rpc` — option-identity helpers.
    let rpc =
        {| callOpts = (fun (options: CallOptions) -> options)
           duration = duration
           opts = (fun (options: InvocationOptions) -> options)
           sendOpts = (fun (options: SendOptions) -> options) |}

    /// `methodNames(descriptors)` = `Object.keys(descriptors)`.
    let methodNames (descriptors: obj) : string[] = FluentSdk.objectKeys descriptors

    /// `requestFor(definition, key, handler, input, options)`.
    let requestFor
        (definition: Definition)
        (key: string option)
        (handler: string)
        (input: obj)
        (options: InvocationOptions option)
        : CallRequest =
        let runId =
            match options with
            | Some o ->
                match o.RunId with
                | Some _ as r -> r
                | None -> o.IdempotencyKey
            | None -> None

        let descriptor =
            let d = FluentSdk.prop<obj> definition.Handlers handler
            if FluentSdk.isUndefined d then None else Some(Definitions.descriptorOfObj d)

        { Kind = definition.Kind
          Name = definition.Name
          Handler = handler
          Key = key
          Input = input
          RunId = runId
          IdempotencyKey = options |> Option.bind (fun o -> o.IdempotencyKey)
          DelayMs = options |> Option.bind (fun o -> o.Delay) |> Option.map normalizeDuration
          Metadata = options |> Option.bind (fun o -> o.Metadata)
          Descriptor = descriptor }

    /// `genericRequest(request)`.
    let genericRequest (request: GenericInvocationRequest) : CallRequest =
        let runId =
            match request.RunId with
            | Some _ as r -> r
            | None -> request.IdempotencyKey

        { Kind = request.Kind
          Name = request.Name
          Handler = request.Handler
          Key = request.Key
          Input = request.Input
          RunId = runId
          IdempotencyKey = request.IdempotencyKey
          DelayMs = request.Delay |> Option.map normalizeDuration
          Metadata = request.Metadata
          Descriptor = None }

    /// `invocationHandle(binding, request, reference)`.
    let invocationHandle (binding: InvocationBinding) (request: CallRequest) (reference: SendReference) : InvocationHandle =
        let attach () =
            binding.Call { request with RunId = Some reference.InvocationId }

        { Reference = reference
          Attach = attach
          OutputEffect = attach }

    /// `invoke(binding, mode, request)`.
    let invoke (binding: InvocationBinding) (mode: ClientMode) (request: CallRequest) : Effect<obj, exn, Context> =
        if mode = "call" then
            binding.Call request
        else
            binding.Send request
            |> Effect.map (fun reference -> box (invocationHandle binding request reference))

    /// `invocation(binding, reference)`.
    let invocation (binding: InvocationBinding) (reference: AttachableReference) : InvocationHandle =
        let request: CallRequest =
            { Kind = reference.Kind
              Name = reference.Name
              Handler = reference.Handler
              Key = reference.Key
              Input = reference.Input |> Option.defaultValue FluentSdk.undefinedValue
              RunId = None
              IdempotencyKey = None
              DelayMs = None
              Metadata = None
              Descriptor = None }

        let sendRef: SendReference =
            { Handler = Some reference.Handler
              InvocationId = reference.InvocationId
              Key = reference.Key
              Kind = Some reference.Kind
              Name = Some reference.Name
              Output = reference.Output }

        invocationHandle binding request sendRef

    // ── generic call / send / attach ──────────────────────────────────
    // The TS splits each into ambient-context and explicit-binding overloads.
    // Here those become two explicit functions per operation.

    let genericCallWithBinding (binding: InvocationBinding) (request: GenericInvocationRequest) : Effect<obj, exn, Context> =
        binding.Call (genericRequest request)

    /// `genericCall(request)` — ambient context binding.
    let genericCall (request: GenericInvocationRequest) : Effect<obj, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.Binding with
            | None -> Effect.fail (FluentFiregridError.create "genericCall requires an invocation binding")
            | Some binding -> binding.Call (genericRequest request))

    let genericSendWithBinding
        (binding: InvocationBinding)
        (request: GenericInvocationRequest)
        : Effect<InvocationHandle, exn, Context> =
        let callRequest = genericRequest request

        binding.Send callRequest
        |> Effect.map (fun reference -> invocationHandle binding callRequest reference)

    /// `genericSend(request)` — ambient context binding.
    let genericSend (request: GenericInvocationRequest) : Effect<InvocationHandle, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.Binding with
            | None -> Effect.fail (FluentFiregridError.create "genericSend requires an invocation binding")
            | Some binding ->
                let req = genericRequest request

                binding.Send req
                |> Effect.map (fun reference -> invocationHandle binding req reference))

    let attachWithBinding (binding: InvocationBinding) (reference: AttachableReference) : Effect<obj, exn, Context> =
        (invocation binding reference).Attach()

    /// `attach(reference)` — ambient context binding.
    let attach (reference: AttachableReference) : Effect<obj, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.Binding with
            | None -> Effect.fail (FluentFiregridError.create "attach requires an invocation binding")
            | Some binding -> (invocation binding reference).Attach())

    // ── client factory family ─────────────────────────────────────────
    // Each client is a JS object keyed by handler name, each value a function
    // `(input, options?) => Effect<...>`. REDUCTION: typed per-handler shapes
    // collapse to a runtime `obj` dispatch object built from `_handlers` keys.

    /// Parse a raw JS options arg (a JS object or `undefined`) into
    /// `InvocationOptions option` so JS callers may omit the second argument.
    let invocationOptionsOfObj (options: obj) : InvocationOptions option =
        if FluentSdk.isNullish options then
            None
        else
            let optStr (key: string) =
                let v = FluentSdk.prop<obj> options key
                if FluentSdk.isNullish v then None else Some(FluentSdk.stringValue v)

            let optObj (key: string) =
                let v = FluentSdk.prop<obj> options key
                if FluentSdk.isUndefined v then None else Some v

            Some
                { Delay = optObj "delay"
                  IdempotencyKey = optStr "idempotencyKey"
                  Metadata = optObj "metadata"
                  RunId = optStr "runId" }

    /// `bindInvocationBinding(mode)(binding, definition, key?)`.
    let bindInvocationBinding (mode: ClientMode) (binding: InvocationBinding) (definition: Definition) (key: string option) : obj =
        let entries =
            methodNames definition.Handlers
            |> Array.map (fun handler ->
                let fn (input: obj) (options: obj) : Effect<obj, exn, Context> =
                    invoke binding mode (requestFor definition key handler input (invocationOptionsOfObj options))

                (handler, box (System.Func<obj, obj, Effect<obj, exn, Context>>(fn))))

        FluentSdk.objectFromEntries entries

    /// `bindAmbientContext(mode)(definition, key?)`.
    let bindAmbientContext (mode: ClientMode) (definition: Definition) (key: string option) : obj =
        let entries =
            methodNames definition.Handlers
            |> Array.map (fun handler ->
                let fn (input: obj) (options: obj) : Effect<obj, exn, Context> =
                    FluentDurableContext.withContext (fun ctx ->
                        match ctx.Binding with
                        | None ->
                            Effect.fail (
                                FluentFiregridError.create (
                                    sprintf
                                        "fluent ambient client %s.%s requires an invocation binding"
                                        definition.Name
                                        handler
                                )
                            )
                        | Some binding ->
                            invoke binding mode (requestFor definition key handler input (invocationOptionsOfObj options)))

                (handler, box (System.Func<obj, obj, Effect<obj, exn, Context>>(fn))))

        FluentSdk.objectFromEntries entries

    // ── explicit-binding clients ──────────────────────────────────────

    /// `client = bindInvocationBinding("call")`.
    let client (binding: InvocationBinding) (definition: Definition) (key: string option) : obj =
        bindInvocationBinding "call" binding definition key

    /// `sendClient = bindInvocationBinding("send")`.
    let sendClient (binding: InvocationBinding) (definition: Definition) (key: string option) : obj =
        bindInvocationBinding "send" binding definition key

    // ── contextual (ambient-or-binding) clients ───────────────────────
    // `contextualClient` chooses ambient (definition only) vs binding (binding +
    // definition) based on presence of the binding. Split into explicit funcs.

    /// Ambient service/workflow client (`serviceClient(definition)`).
    let serviceClient (definition: Definition) : obj = bindAmbientContext "call" definition None

    /// Binding service/workflow client (`serviceClient(binding, definition, key?)`).
    let serviceClientWithBinding (binding: InvocationBinding) (definition: Definition) (key: string option) : obj =
        bindInvocationBinding "call" binding definition key

    let workflowClient = serviceClient
    let workflowClientWithBinding = serviceClientWithBinding

    /// Ambient send service/workflow client.
    let sendServiceClient (definition: Definition) : obj = bindAmbientContext "send" definition None

    let sendServiceClientWithBinding (binding: InvocationBinding) (definition: Definition) (key: string option) : obj =
        bindInvocationBinding "send" binding definition key

    let serviceSendClient = sendServiceClient
    let serviceSendClientWithBinding = sendServiceClientWithBinding
    let sendWorkflowClient = sendServiceClient
    let sendWorkflowClientWithBinding = sendServiceClientWithBinding
    let workflowSendClient = sendServiceClient
    let workflowSendClientWithBinding = sendServiceClientWithBinding

    // ── object (keyed) clients ────────────────────────────────────────
    // `keyedContextualClient` returns either a `Client` (when a key is known) or
    // a `(key) => Client` curried form. Split into explicit functions.

    /// `objectClient(binding, definition, key)` — bound + keyed → Client.
    let objectClientWithBindingKey (binding: InvocationBinding) (definition: Definition) (key: string) : obj =
        bindInvocationBinding "call" binding definition (Some key)

    /// `objectClient(binding, definition)` — bound, returns `(key) => Client`.
    let objectClientWithBinding (binding: InvocationBinding) (definition: Definition) : (string -> obj) =
        fun key -> bindInvocationBinding "call" binding definition (Some key)

    /// `objectClient(definition, key)` — ambient + keyed → Client.
    let objectClientWithKey (definition: Definition) (key: string) : obj =
        bindAmbientContext "call" definition (Some key)

    /// `objectClient(definition)` — ambient, returns `(key) => Client`.
    let objectClient (definition: Definition) : (string -> obj) =
        fun key -> bindAmbientContext "call" definition (Some key)

    /// `sendObjectClient(binding, definition, key)`.
    let sendObjectClientWithBindingKey (binding: InvocationBinding) (definition: Definition) (key: string) : obj =
        bindInvocationBinding "send" binding definition (Some key)

    /// `sendObjectClient(binding, definition)`.
    let sendObjectClientWithBinding (binding: InvocationBinding) (definition: Definition) : (string -> obj) =
        fun key -> bindInvocationBinding "send" binding definition (Some key)

    /// `sendObjectClient(definition, key)`.
    let sendObjectClientWithKey (definition: Definition) (key: string) : obj =
        bindAmbientContext "send" definition (Some key)

    /// `sendObjectClient(definition)`.
    let sendObjectClient (definition: Definition) : (string -> obj) =
        fun key -> bindAmbientContext "send" definition (Some key)

    let objectSendClient = sendObjectClient
    let objectSendClientWithKey = sendObjectClientWithKey
    let objectSendClientWithBinding = sendObjectClientWithBinding
    let objectSendClientWithBindingKey = sendObjectClientWithBindingKey
