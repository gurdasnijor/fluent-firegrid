namespace Firegrid.Core

open Fable.Core
open Fable.Core.JsInterop

/// Server request parsing (`server/parse-request.ts`).

/// Params to spread into `runWorkflow(...)`.
type WorkflowRequestParams =
    { Approval: ApprovalResult option
      SignalDelivery: SignalDelivery option
      Input: obj option
      RunId: string option
      Abort: bool option }

[<RequireQualifiedAccess>]
module ParseRequest =

    /// `WorkflowRequestParseError` JS class (name = "WorkflowRequestParseError").
    [<Emit("""(() => {
  const e = new Error($0);
  e.name = 'WorkflowRequestParseError';
  e._tag = 'WorkflowRequestParseError';
  if ($1 !== undefined) e.cause = $1;
  return e;
})()""")>]
    let private makeParseError (_message: string) (_cause: obj) : exn = jsNative

    let parseError (message: string) : exn = makeParseError message CoreSdk.undefinedValue

    let parseErrorWithCause (message: string) (cause: obj) : exn = makeParseError message cause

    let private optObj (o: obj) (key: string) : obj option =
        let v = CoreSdk.prop<obj> o key
        if CoreSdk.isUndefined v then None else Some v

    let private optStr (o: obj) (key: string) : string option =
        let v = CoreSdk.prop<obj> o key
        if CoreSdk.isNullish v then None else Some(CoreSdk.stringValue v)

    let private optBool (o: obj) (key: string) : bool option =
        let v = CoreSdk.prop<obj> o key
        if CoreSdk.isUndefined v then None else Some(unbox<bool> v)

    let private approvalOfObj (o: obj) : ApprovalResult =
        { Approved = CoreSdk.prop<bool> o "approved"
          ApprovalId = CoreSdk.prop<string> o "approvalId"
          Feedback = optStr o "feedback"
          Meta =
            let m = CoreSdk.prop<obj> o "meta"
            if CoreSdk.isNullish m then None else Some m }

    let private signalOfObj (o: obj) : SignalDelivery =
        { SignalId = CoreSdk.prop<string> o "signalId"
          StepId = optStr o "stepId"
          Name = CoreSdk.prop<string> o "name"
          Payload = CoreSdk.prop<obj> o "payload"
          Meta =
            let m = CoreSdk.prop<obj> o "meta"
            if CoreSdk.isNullish m then None else Some m }

    /// `parseWorkflowRequest(request)`.
    let parseWorkflowRequest (request: obj) : JS.Promise<WorkflowRequestParams> =
        promise {
            let! outcome = CoreSdk.requestJsonResult request

            let raw =
                if CoreSdk.prop<bool> outcome "ok" then
                    CoreSdk.prop<obj> outcome "value"
                else
                    let err = CoreSdk.prop<obj> outcome "error"
                    let message = if CoreSdk.isError err then CoreSdk.errorMessage err else "Invalid JSON body"
                    raise (parseErrorWithCause message err)

            // typeof raw !== "object" || raw === null || Array.isArray(raw)
            if not (CoreSdk.isTypeofObject raw) || isNull raw || CoreSdk.isArray raw then
                return raise (parseError "Workflow request body must be a JSON object.")
            else
                // `signal` wins over `approval`.
                let signal = optObj raw "signal"

                let signalDelivery = signal |> Option.map signalOfObj

                let approval =
                    match signal with
                    | Some _ -> None
                    | None -> optObj raw "approval" |> Option.map approvalOfObj

                return
                    { Approval = approval
                      SignalDelivery = signalDelivery
                      Input = optObj raw "input"
                      RunId = optStr raw "runId"
                      Abort = optBool raw "abort" }
        }
