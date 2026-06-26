namespace Firegrid.Fluent

open Effect
open Fable.Core
open Firegrid.Core

// ============================================================
// run.ts — run / sleep / sleepUntil / waitForSignal / objectKey
// ============================================================

/// `RunOptions extends StepOptions` plus `name?`.
type RunOptions =
    { Name: string option
      // ── inherited StepOptions ──
      Meta: WorkflowMetadata option
      Retry: StepRetryOptions option
      Timeout: float option }

    static member Empty =
        { Name = None
          Meta = None
          Retry = None
          Timeout = None }

[<RequireQualifiedAccess>]
module Run =

    /// Read a JS function's `.name` (the TS uses `action.name`).
    [<Emit("$0 && $0.name ? String($0.name) : ''")>]
    let private fnName (_action: obj) : string = jsNative

    let private toStepOptions (options: RunOptions option) : StepOptions option =
        options
        |> Option.map (fun o ->
            { Meta = o.Meta
              Retry = o.Retry
              Timeout = o.Timeout })

    /// `actionName(action, options)` = `options?.name ?? action.name`.
    let private actionName (action: RunAction) (options: RunOptions option) : string option =
        match options |> Option.bind (fun o -> o.Name) with
        | Some n -> Some n
        | None ->
            let n = fnName (box action)
            // TS returns `action.name` which may be "" — treated below.
            Some n

    /// `run(action, options?)`.
    let run (action: RunAction) (options: RunOptions option) : Effect<obj, exn, Context> =
        let name = actionName action options

        match name with
        | None ->
            Effect.fail (FluentFiregridError.create "run(action, options) requires options.name or a named action")
        | Some "" ->
            Effect.fail (FluentFiregridError.create "run(action, options) requires options.name or a named action")
        | Some name ->
            FluentDurableContext.withContext (fun ctx -> ctx.Step name action (toStepOptions options))

    /// `sleep(ms, options?)`.
    let sleep (ms: float) (options: SleepOptions option) : Effect<unit, exn, Context> =
        FluentDurableContext.withContext (fun ctx -> ctx.Sleep ms options)

    /// `sleepUntil(timestamp, options?)`.
    let sleepUntil (timestamp: float) (options: SleepOptions option) : Effect<unit, exn, Context> =
        FluentDurableContext.withContext (fun ctx -> ctx.SleepUntil timestamp options)

    /// `waitForSignal(name, options?)`.
    let waitForSignal (name: string) (options: WaitForEventOptions option) : Effect<obj, exn, Context> =
        FluentDurableContext.withContext (fun ctx -> ctx.WaitForSignal name options)

    /// `objectKey` — fails unless used in a keyed object handler.
    let objectKey: Effect<string, exn, Context> =
        FluentDurableContext.withContext (fun ctx ->
            match ctx.Key with
            | None -> Effect.fail (FluentFiregridError.create "objectKey can only be used in keyed object handlers")
            | Some key -> Effect.succeed key)
