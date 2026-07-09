namespace Firegrid.Foundation.Proofs

open Fable.Core
open Firegrid.Log

module Property =
    type NegativeControlDraft<'result> =
        { Workload: (WorkloadContext -> Async<'result>) option
          Verifiers: Check<'result> list
          ExpectedFailure: string option }

    type PropertyDraft<'result> =
        { Resources: ResourceSpec list
          Workload: (WorkloadContext -> Async<'result>) option
          Verifiers: Check<'result> list
          NegativeControls: NegativeControlSpec<'result> list
          RequiresNegativeControl: bool
          TimeoutMs: int option }

    type ResolvedResources =
        { S2: S2Resource option
          Hosts: Map<string, HostResource>
          KillHosts: Map<string, unit -> Async<bool>>
          PauseHosts: Map<string, unit -> Async<bool>>
          ResumeHosts: Map<string, unit -> Async<bool>>
          Releases: (unit -> Async<unit>) list }

    let private unsupportedResources resources =
        let s2ResourceCount =
            resources
            |> List.filter (function
                | S2LiveFromEnv
                | S2Lite _
                | S2LiteDedicated _ -> true
                | _ -> false)
            |> List.length

        let duplicateHosts =
            resources
            |> List.choose (function
                | ProcessHost host -> Some host.Name
                | _ -> None)
            |> List.countBy id
            |> List.choose (fun (name, count) ->
                if count > 1 then
                    Some(sprintf "processHost '%s' is declared more than once" name)
                else
                    None)

        let unsupported =
            if s2ResourceCount > 1 then
                [ "only one S2 resource can be declared per property" ]
            else
                []

        unsupported @ duplicateHosts

    let private checkReport name result =
        match result with
        | Ok() ->
            { Name = name
              Passed = true
              Message = None }
        | Error message ->
            { Name = name
              Passed = false
              Message = Some message }

    let private runWorkload work ctx =
        async {
            try
                let! result = work ctx
                return Ok result
            with e ->
                return Error e.Message
        }

    /// Bound a workload by wall clock (corpus-harness mechanic): a
    /// partially-green surface that parks forever fails the LAW — it must
    /// never hang the suite. The abandoned promise cannot reject unhandled
    /// (runWorkload catches), so the race is safe to walk away from.
    let private runWorkloadBounded (timeoutMs: int option) work ctx =
        match timeoutMs with
        | None -> runWorkload work ctx
        | Some millis ->
            async {
                let promise = runWorkload work ctx |> Async.StartAsPromise
                let! winner = Reports.raceTimeout promise millis |> Async.AwaitPromise

                if isNull winner then
                    return! Async.AwaitPromise promise
                else
                    return Error(sprintf "workload timed out after %dms" millis)
            }

    let private runChecks (trial: CompletedTrial<'result>) (checks: Check<'result> list) =
        async {
            let reports = ResizeArray<CheckReport>()

            for check in checks do
                let! result = check.RunCheck trial
                reports.Add(checkReport check.Name result)

            return List.ofSeq reports
        }

    let private emptyResources =
        { S2 = None
          Hosts = Map.empty
          KillHosts = Map.empty
          PauseHosts = Map.empty
          ResumeHosts = Map.empty
          Releases = [] }

    let private releaseResources resources =
        async {
            for release in resources.Releases |> List.rev do
                try
                    do! release ()
                with _ ->
                    ()
        }

    /// Resolve declared resources INTO the caller's cell as they come up, so
    /// a mid-resolution failure leaves the already-started resources visible
    /// for release (a red law must fail as a law, never leak a child process
    /// or crash the suite).
    let private resolveResources
        (store: TraceStore)
        (sharedS2: S2Resource option)
        resources
        (cell: ResolvedResources ref)
        =
        async {
            for resource in resources do
                let resolved = cell.Value

                match resource with
                | S2LiveFromEnv ->
                    let s2 =
                        { Client = S2Cli.connect ()
                          Kind = "s2LiveFromEnv"
                          Endpoint = None
                          LocalRoot = None }

                    cell.Value <- { resolved with S2 = Some s2 }

                    do! Reports.emitSpan store "verification.s2.live.connected" [ "resource.kind", "s2LiveFromEnv" ]
                | S2Lite _ ->
                    // The runner boots ONE s2-lite per invocation; trials are
                    // isolated by trial-scoped basins (CorpusSupport). No
                    // release: the shared instance outlives every trial (its
                    // lifecycle spans live at runner scope).
                    match sharedS2 with
                    | Some shared -> cell.Value <- { resolved with S2 = Some shared }
                    | None ->
                        return
                            failwith
                                "s2Lite resolves to the runner's shared s2-lite, but the runner did not boot one (declare s2LiteDedicated for a private instance)"
                | S2LiteDedicated localRoot ->
                    let! s2Lite = Firegrid.Foundation.Proofs.S2Lite.start store localRoot

                    cell.Value <-
                        { resolved with
                            S2 = Some s2Lite.Resource
                            Releases = s2Lite.Stop :: resolved.Releases }
                | ProcessHost hostSpec ->
                    // Hosts see the S2 endpoint only when the S2 resource is
                    // declared before them (declaration order is resolution
                    // order, matching the SDD's Runner Order).
                    let! host = ProcessHost.start store resolved.S2 hostSpec

                    cell.Value <-
                        { resolved with
                            Hosts = resolved.Hosts |> Map.add hostSpec.Name host.Resource
                            KillHosts = resolved.KillHosts |> Map.add hostSpec.Name host.Kill
                            PauseHosts = resolved.PauseHosts |> Map.add hostSpec.Name host.Pause
                            ResumeHosts = resolved.ResumeHosts |> Map.add hostSpec.Name host.Resume
                            Releases = host.Stop :: resolved.Releases }
        }

    let private workloadContext
        (trialId: string)
        (seed: int)
        (store: TraceStore)
        (resources: ResolvedResources)
        (faults: ResizeArray<FaultEvent>)
        =
        let nextOperation =
            let counter = ref 0

            fun () ->
                counter.Value <- counter.Value + 1
                counter.Value

        let nextFault =
            let counter = ref 0

            fun kind target signal accepted ->
                counter.Value <- counter.Value + 1

                { FaultId = sprintf "%s-fault-%d" trialId counter.Value
                  Kind = kind
                  Target = target
                  Signal = signal
                  Accepted = accepted
                  OperationIndex = counter.Value }

        let hostFault (kind: string) (signal: string) (registry: Map<string, unit -> Async<bool>>) (name: string) =
            async {
                match registry |> Map.tryFind name with
                | Some inject ->
                    let! accepted = inject ()

                    faults.Add(nextFault kind name (Some signal) (Some accepted))
                | None -> return failwithf "processHost '%s' is not supervised by the verification runner" name
            }

        { TrialId = trialId
          Root = store.Root
          Traces = store
          Seed = seed
          S2 = resources.S2
          Hosts = resources.Hosts
          Faults =
            { KillHost = fun name -> hostFault "hostKill" "SIGKILL" resources.KillHosts name
              PauseHost = fun name -> hostFault "hostPause" "SIGSTOP" resources.PauseHosts name
              ResumeHost = fun name -> hostFault "hostResume" "SIGCONT" resources.ResumeHosts name }
          NextOperationId = nextOperation
          EmitSpan = Reports.emitSpan store }

    let private runNegativeControl
        (proofName: string)
        (propertyName: string)
        (config: RunnerConfig)
        (timeoutMs: int option)
        (positiveWorkload: WorkloadContext -> Async<'result>)
        (propertyResources: ResourceSpec list)
        (traces: TraceStore)
        (control: NegativeControlSpec<'result>)
        =
        async {
            let trialId = Reports.trialId (propertyName + "-negative")
            let store = Reports.traceStore config.Root trialId
            let faults = ResizeArray<FaultEvent>()
            let resourcesCell = ref emptyResources

            let! result =
                async {
                    let! setup = Async.Catch(resolveResources store config.SharedS2 propertyResources resourcesCell)

                    match setup with
                    | Choice2Of2 error -> return Error("resource setup failed: " + error.Message)
                    | Choice1Of2() ->
                        let ctx = workloadContext trialId config.Seed store resourcesCell.Value faults
                        let work = control.Workload |> Option.defaultValue positiveWorkload
                        return! runWorkloadBounded timeoutMs work ctx
                }

            do! releaseResources resourcesCell.Value

            let completed =
                { ProofName = proofName
                  PropertyName = propertyName
                  TrialId = trialId
                  Result = result
                  Traces = store
                  Faults = List.ofSeq faults }

            let! checkReports = runChecks completed control.Verifiers

            let failedChecks =
                checkReports
                |> List.filter (fun report -> not report.Passed)
                |> List.map (fun report -> report.Name)

            let matchedExpectedFailure =
                match control.ExpectedFailure with
                | None -> not (List.isEmpty failedChecks)
                | Some expected -> failedChecks |> List.exists (fun failed -> failed.Contains expected)

            let passed = matchedExpectedFailure

            do!
                Reports.emitSpan
                    traces
                    "verification.negative_control"
                    [ "proof.property", propertyName
                      "negative.name", control.Name
                      "negative.passed", string passed ]

            return
                { Name = control.Name
                  Passed = passed
                  ExpectedFailure = control.ExpectedFailure
                  FailedChecks = failedChecks
                  Faults = List.ofSeq faults
                  Message =
                    if passed then
                        None
                    else
                        Some "negative control did not fail for the expected reason" }
        }

    let private runProperty (proofName: string) (config: RunnerConfig) (spec: PropertySpec<'result>) =
        async {
            let trialId = config.TrialId |> Option.defaultValue (Reports.trialId spec.Name)

            let store = Reports.traceStore config.Root trialId
            let reportPath = Reports.join [ store.Root; "report.json" ]
            let faults = ResizeArray<FaultEvent>()

            do!
                Reports.emitSpan
                    store
                    "verification.property.start"
                    [ "proof.name", proofName; "proof.property", spec.Name ]

            let unsupported = unsupportedResources spec.Resources

            let! workloadResult =
                if List.isEmpty unsupported then
                    async {
                        let resourcesCell = ref emptyResources
                        let! setup = Async.Catch(resolveResources store config.SharedS2 spec.Resources resourcesCell)

                        let! result =
                            match setup with
                            | Choice2Of2 error -> async { return Error("resource setup failed: " + error.Message) }
                            | Choice1Of2() ->
                                workloadContext trialId config.Seed store resourcesCell.Value faults
                                |> runWorkloadBounded spec.TimeoutMs spec.Workload

                        do! releaseResources resourcesCell.Value
                        return result
                    }
                else
                    async { return Error(String.concat "; " unsupported) }

            let completed =
                { ProofName = proofName
                  PropertyName = spec.Name
                  TrialId = trialId
                  Result = workloadResult
                  Traces = store
                  Faults = List.ofSeq faults }

            let! checks = runChecks completed spec.Verifiers

            let! negativeControls =
                spec.NegativeControls
                |> List.map (
                    runNegativeControl proofName spec.Name config spec.TimeoutMs spec.Workload spec.Resources store
                )
                |> Async.Sequential

            let missingNegativeControl =
                spec.RequiresNegativeControl && Array.isEmpty negativeControls

            let failedChecks = checks |> List.exists (fun check -> not check.Passed)

            let failedNegativeControls =
                negativeControls |> Array.exists (fun control -> not control.Passed)

            let workloadFailed = Result.isError workloadResult

            let passed =
                not workloadFailed
                && not failedChecks
                && not failedNegativeControls
                && not missingNegativeControl

            let checks =
                if missingNegativeControl then
                    { Name = "negative control required"
                      Passed = false
                      Message = Some "property requires a negative control but none were declared" }
                    :: checks
                else
                    checks

            do!
                Reports.emitSpan
                    store
                    "verification.property.finish"
                    [ "proof.name", proofName
                      "proof.property", spec.Name
                      "verification.passed", string passed ]

            let report =
                { ProofName = proofName
                  PropertyName = spec.Name
                  TrialId = trialId
                  Passed = passed
                  WorkloadFailed = workloadFailed
                  Faults = List.ofSeq faults
                  Checks = checks
                  NegativeControls = Array.toList negativeControls
                  ReplayCommand =
                    sprintf
                        "node apps/proofs/dist/Main.js proof run %s --report-dir %s --trial-id %s"
                        spec.Name
                        config.Root
                        trialId
                  ReportPath = reportPath }

            Reports.writePropertyReport report
            return report
        }

    type NegativeControlBuilder<'result>(name: string) =
        member _.Yield(_) : NegativeControlDraft<'result> =
            { Workload = None
              Verifiers = []
              ExpectedFailure = None }

        [<CustomOperation("workload")>]
        member _.Workload(state: NegativeControlDraft<'result>, work) = { state with Workload = Some work }

        [<CustomOperation("verify")>]
        member _.Verify(state: NegativeControlDraft<'result>, verifiers) =
            { state with
                Verifiers = state.Verifiers @ verifiers }

        [<CustomOperation("verify")>]
        member _.Verify(state: NegativeControlDraft<'result>, factory: Verifiers<'result> -> Check<'result> list) =
            { state with
                Verifiers = state.Verifiers @ factory (Verification.verifiers ()) }

        [<CustomOperation("expectFailure")>]
        member _.ExpectFailure(state: NegativeControlDraft<'result>, expected) =
            { state with
                ExpectedFailure = Some expected }

        member _.Run(state: NegativeControlDraft<'result>) : NegativeControlSpec<'result> =
            { Name = name
              Workload = state.Workload
              Verifiers = state.Verifiers
              ExpectedFailure = state.ExpectedFailure }

    let makeWith name resources workload verifiers negativeControls requiresNegativeControl timeoutMs =
        if List.isEmpty verifiers then
            failwithf "property '%s' must declare at least one verifier" name

        let spec =
            { Name = name
              Resources = resources
              Workload = workload
              Verifiers = verifiers
              NegativeControls = negativeControls
              RequiresNegativeControl = requiresNegativeControl
              TimeoutMs = timeoutMs }

        { Name = name
          RunProperty = fun config proofName -> runProperty proofName config spec }

    let make name resources workload verifiers negativeControls requiresNegativeControl =
        makeWith name resources workload verifiers negativeControls requiresNegativeControl None

    let negativeControl<'result> name = NegativeControlBuilder<'result> name

    type PropertyBuilder(name: string) =
        member _.Yield(_) : PropertyDraft<unit> =
            { Resources = []
              Workload = None
              Verifiers = []
              NegativeControls = []
              RequiresNegativeControl = false
              TimeoutMs = None }

        [<CustomOperation("resource")>]
        member _.Resource(state: PropertyDraft<'result>, resource) =
            { state with
                Resources = state.Resources @ [ resource ] }

        [<CustomOperation("resources")>]
        member _.Resources(state: PropertyDraft<'result>, resources) =
            { state with
                Resources = state.Resources @ resources }

        [<CustomOperation("s2LiveFromEnv")>]
        member _.S2LiveFromEnv(state: PropertyDraft<'result>) =
            { state with
                Resources = state.Resources @ [ S2LiveFromEnv ] }

        [<CustomOperation("s2Lite")>]
        member _.S2Lite(state: PropertyDraft<'result>, root) =
            { state with
                Resources = state.Resources @ [ S2Lite root ] }

        [<CustomOperation("s2LiteDedicated")>]
        member _.S2LiteDedicated(state: PropertyDraft<'result>, root) =
            { state with
                Resources = state.Resources @ [ S2LiteDedicated root ] }

        [<CustomOperation("processHost")>]
        member _.ProcessHost(state: PropertyDraft<'result>, host) =
            { state with
                Resources = state.Resources @ [ ProcessHost host ] }

        [<CustomOperation("timeoutMs")>]
        member _.TimeoutMs(state: PropertyDraft<'result>, millis: int) = { state with TimeoutMs = Some millis }

        [<CustomOperation("workload")>]
        member _.Workload(state: PropertyDraft<'previous>, workload: WorkloadContext -> Async<'result>) =
            { Resources = state.Resources
              Workload = Some workload
              Verifiers = []
              NegativeControls = []
              RequiresNegativeControl = state.RequiresNegativeControl
              TimeoutMs = state.TimeoutMs }

        [<CustomOperation("verify")>]
        member _.Verify(state: PropertyDraft<'result>, verifiers: Check<'result> list) =
            { state with
                Verifiers = state.Verifiers @ verifiers }

        [<CustomOperation("verify")>]
        member _.Verify(state: PropertyDraft<'result>, factory: Verifiers<'result> -> Check<'result> list) =
            { state with
                Verifiers = state.Verifiers @ factory (Verification.verifiers ()) }

        [<CustomOperation("negativeControl")>]
        member _.NegativeControl(state: PropertyDraft<'result>, control) =
            { state with
                NegativeControls = state.NegativeControls @ [ control ] }

        [<CustomOperation("requiresNegativeControl")>]
        member _.RequiresNegativeControl(state: PropertyDraft<'result>) =
            { state with
                RequiresNegativeControl = true }

        member _.Run(state: PropertyDraft<'result>) =
            match state.Workload with
            | None -> failwithf "property '%s' must declare a workload" name
            | Some workload ->
                makeWith
                    name
                    state.Resources
                    workload
                    state.Verifiers
                    state.NegativeControls
                    state.RequiresNegativeControl
                    state.TimeoutMs

    let property name = PropertyBuilder name

[<AutoOpen>]
module PropertySyntax =
    let property name = Property.property name

    let propertyWithChecks name workload verifiers =
        Property.make name [] workload verifiers [] false

    let propertyWith name resources workload verifiers =
        Property.make name resources workload verifiers [] false

    let propertyWithControls name resources workload verifiers controls requiresNegativeControl =
        Property.make name resources workload verifiers controls requiresNegativeControl

    let propertyWithVerifiers name workload factory =
        Property.make name [] workload (factory (Verification.verifiers ())) [] false

    let propertyWithResourcesAndVerifiers name resources workload factory =
        Property.make name resources workload (factory (Verification.verifiers ())) [] false

    let propertyWithControlsAndVerifiers name resources workload factory controls requiresNegativeControl =
        Property.make name resources workload (factory (Verification.verifiers ())) controls requiresNegativeControl

    let negativeControl<'result> name = Property.negativeControl<'result> name
