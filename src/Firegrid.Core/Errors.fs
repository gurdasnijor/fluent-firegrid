namespace Firegrid.Core

open Fable.Core
open Fable.Core.JsInterop

/// Wire-safe serialized Error (`SerializedError` in types.ts).
type SerializedError =
    { Name: string
      Message: string
      Stack: string option }

[<RequireQualifiedAccess>]
module SerializedError =

    /// Build a JS-object representation (`{ name, message, stack? }`) for the log.
    let toObj (e: SerializedError) : obj =
        let o = createObj [ "name" ==> e.Name; "message" ==> e.Message ]

        match e.Stack with
        | Some stack -> CoreSdk.setProp o "stack" (box stack)
        | None -> ()

        o

    let ofObj (o: obj) : SerializedError =
        { Name = CoreSdk.errorName o
          Message = CoreSdk.errorMessage o
          Stack =
            let s = CoreSdk.prop<obj> o "stack"
            if CoreSdk.isNullish s then None else Some(CoreSdk.stringValue s) }

/// Internal JS-class shims so the engine can `instanceof`-check thrown errors
/// exactly the way the TypeScript does (e.g. `err instanceof LogConflictError`).
[<RequireQualifiedAccess>]
module internal ErrorsSdk =

    [<Emit("""(() => {
  const e = new Error($2);
  e.name = 'LogConflictError';
  e._tag = 'LogConflictError';
  e.attemptedIndex = $1;
  if ($3 !== undefined) e.existing = $3;
  e.runId = $0;
  return e;
})()""")>]
    let makeLogConflict (_runId: string) (_attemptedIndex: float) (_message: string) (_existing: obj) : exn = jsNative

    [<Emit("$0 instanceof Error && $0._tag === 'LogConflictError'")>]
    let isLogConflict (_value: obj) : bool = jsNative

    [<Emit("""(() => {
  const e = new Error($0);
  e.name = 'StepTimeoutError';
  e._tag = 'StepTimeoutError';
  e.stepId = $1;
  e.timeoutMs = $2;
  return e;
})()""")>]
    let makeStepTimeout (_message: string) (_stepId: string) (_timeoutMs: float) : exn = jsNative

    [<Emit("""(() => {
  const e = new Error('Workflow paused — this error is for engine use only.');
  e.name = 'WorkflowPaused';
  e._tag = 'WorkflowPaused';
  return e;
})()""")>]
    let makeWorkflowPaused () : exn = jsNative

    [<Emit("$0 instanceof Error && $0._tag === 'WorkflowPaused'")>]
    let isWorkflowPaused (_value: obj) : bool = jsNative

/// `FluentFiregridError` — exported tagged error.
[<RequireQualifiedAccess>]
module FluentFiregridError =

    [<Emit("""(() => {
  const e = new Error($0);
  e.name = 'FluentFiregridError';
  e._tag = 'FluentFiregridError';
  if ($1 !== undefined) e.cause = $1;
  return e;
})()""")>]
    let private make (_message: string) (_cause: obj) : exn = jsNative

    let create (message: string) : exn = make message CoreSdk.undefinedValue
    let createWithCause (message: string) (cause: obj) : exn = make message cause

/// `LogConflictError` — thrown by `RunStore.appendEvent` on CAS conflict.
[<RequireQualifiedAccess>]
module LogConflictError =

    /// `Log conflict for run ${runId} at index ${attemptedIndex}: another writer has already committed.`
    let create (runId: string) (attemptedIndex: float) (existing: obj option) : exn =
        let message =
            sprintf "Log conflict for run %s at index %g: another writer has already committed." runId attemptedIndex

        let existingObj =
            match existing with
            | Some e -> e
            | None -> CoreSdk.undefinedValue

        ErrorsSdk.makeLogConflict runId attemptedIndex message existingObj

    let is (value: obj) : bool = ErrorsSdk.isLogConflict value

/// `StepTimeoutError` — thrown when a `ctx.step({ timeout })` exceeds its budget.
[<RequireQualifiedAccess>]
module StepTimeoutError =

    /// `Step "${stepId}" exceeded ${timeoutMs}ms timeout.`
    let create (stepId: string) (timeoutMs: float) : exn =
        let message = sprintf "Step \"%s\" exceeded %gms timeout." stepId timeoutMs
        ErrorsSdk.makeStepTimeout message stepId timeoutMs

/// `WorkflowPaused` — internal sentinel thrown by a paused primitive.
[<RequireQualifiedAccess>]
module WorkflowPaused =

    let create () : exn = ErrorsSdk.makeWorkflowPaused ()
    let is (value: obj) : bool = ErrorsSdk.isWorkflowPaused value
