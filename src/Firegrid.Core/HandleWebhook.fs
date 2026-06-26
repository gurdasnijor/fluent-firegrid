namespace Firegrid.Core

open Fable.Core

/// Webhook-driven invocation (`engine/handle-webhook.ts`).

type WebhookApproval =
    { ApprovalId: string
      Approved: bool
      Feedback: string option }

type WebhookPayload =
    { RunId: string
      SignalDelivery: SignalDelivery option
      Approval: WebhookApproval option }

type HandleWebhookOptions =
    { Workflow: AnyWorkflowDefinition
      RunStore: RunStore
      Payload: WebhookPayload
      Publish: (string -> WorkflowEvent -> JS.Promise<unit>) option }

[<RequireQualifiedAccess>]
module HandleWebhook =

    /// `handleWorkflowWebhook(options)` — drive one webhook invocation to its
    /// next pause/completion, returning the events appended during it.
    let handleWorkflowWebhook (options: HandleWebhookOptions) : JS.Promise<WorkflowEvent[]> =
        let payload = options.Payload

        let approval =
            payload.Approval
            |> Option.map (fun a ->
                { Approved = a.Approved
                  ApprovalId = a.ApprovalId
                  Feedback = a.Feedback
                  Meta = None })

        let runOptions =
            { RunWorkflow.RunWorkflowOptions.Create(options.Workflow, options.RunStore) with
                RunId = Some payload.RunId
                SignalDelivery = payload.SignalDelivery
                Approval = approval
                Publish = options.Publish }

        RunWorkflow.runWorkflowCollect runOptions
