namespace Firegrid.Core

open Fable.Core

/// Cross-version routing (`registry/select-version.ts`).

type WorkflowRegistry =
    { Add: WorkflowDefinition -> unit
      ForRun: string -> RunStore -> JS.Promise<WorkflowDefinition option>
      Get: string -> string option -> WorkflowDefinition option
      All: unit -> WorkflowDefinition[] }

[<RequireQualifiedAccess>]
module Registry =

    /// `selectWorkflowVersion(versions, runId, runStore)`.
    let selectWorkflowVersion
        (versions: WorkflowDefinition[])
        (runId: string)
        (runStore: RunStore)
        : JS.Promise<WorkflowDefinition option> =
        promise {
            let! runState = runStore.GetRunState runId

            match runState with
            | None -> return None
            | Some rs ->
                match rs.WorkflowVersion with
                | Some version ->
                    // Exact match by id + version, or undefined for a versioned run.
                    return
                        versions
                        |> Array.tryFind (fun v -> v.Id = rs.WorkflowId && v.Version = Some version)
                | None ->
                    // Legacy fallback: match by id + no version declared.
                    return
                        versions
                        |> Array.tryFind (fun v -> v.Id = rs.WorkflowId && v.Version = None)
        }

    /// `createWorkflowRegistry({ default? })`.
    let createWorkflowRegistry (defaultWorkflow: WorkflowDefinition option) : WorkflowRegistry =
        let entries = ResizeArray<WorkflowDefinition>()

        { Add =
            fun workflow ->
                let dupe =
                    entries
                    |> Seq.tryFind (fun e -> e.Id = workflow.Id && e.Version = workflow.Version)

                match dupe with
                | Some _ ->
                    let versionLabel = workflow.Version |> Option.defaultValue "(none)"

                    failwithf
                        "Workflow \"%s\" version \"%s\" is already registered."
                        workflow.Id
                        versionLabel
                | None -> entries.Add workflow
          ForRun =
            fun runId runStore ->
                promise {
                    let! matched = selectWorkflowVersion (entries.ToArray()) runId runStore

                    return
                        match matched with
                        | Some m -> Some m
                        | None -> defaultWorkflow
                }
          Get =
            fun id version ->
                entries
                |> Seq.tryFind (fun e -> e.Id = id && e.Version = version)
          All = fun () -> entries.ToArray() }
