/// Runner: proof selection, execution, human/CI output, replay, and the
/// ratchet targets mode. Ported from the eff-firegrid MVP's Runner.fs and
/// adapted to this repo's Reports/Property infrastructure and the
/// targets-README.md suite protocol.
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Fable.Core.JsInterop

module Runner =
    [<Emit("console.log($0)")>]
    let private stdout (_message: string) : unit = jsNative

    [<Emit("console.error($0)")>]
    let private stderr (_message: string) : unit = jsNative

    let private path: obj = importAll "node:path"

    [<Emit("$0.dirname($1)")>]
    let private dirnameWith (_path: obj) (_value: string) : string = jsNative

    let private parentDir value = dirnameWith path value

    /// One shared s2-lite per runner invocation (P0.3c): boot before the
    /// first trial, tear down after the last, lifecycle spans at runner
    /// scope. Trials are isolated by trial-scoped basins, so a single server
    /// instance is sufficient — no proof faults the s2 server itself.
    let private withSharedS2 (config: RunnerConfig) (body: RunnerConfig -> Async<int>) : Async<int> =
        async {
            let store = Reports.traceStore config.Root (Reports.trialId "s2-shared")
            let! shared = S2Lite.start store ""

            stderr (
                sprintf
                    "s2-lite shared instance up at %s (one boot per runner invocation)"
                    (shared.Resource.Endpoint |> Option.defaultValue "<unknown>")
            )

            let! outcome = Async.Catch(body { config with SharedS2 = Some shared.Resource })

            do! shared.Stop()

            match outcome with
            | Choice1Of2 code -> return code
            | Choice2Of2 error -> return raise error
        }

    let private matchesFilter (filter: string option) (proof: ProofSpec) =
        match filter with
        | None -> true
        | Some value ->
            proof.Name.Contains value
            || proof.Properties |> List.exists (fun property -> property.Name.Contains value)

    /// Bounded worker pool over a queue of jobs (P0.3c ruling 4). Node's
    /// single-threaded event loop makes the shared index safe: there is no
    /// await between the read and the increment.
    let private runPool (concurrency: int) (jobs: (unit -> Async<unit>) list) : Async<unit> =
        match jobs with
        | [] -> async { return () }
        | _ ->
            let queue = List.toArray jobs
            let next = ref 0

            let worker (_: int) =
                let rec loop () =
                    async {
                        let index = next.Value

                        if index >= queue.Length then
                            return ()
                        else
                            next.Value <- index + 1
                            do! queue.[index] ()
                            return! loop ()
                    }

                loop ()

            [ 1 .. max 1 (min concurrency queue.Length) ]
            |> List.map worker
            |> Async.Parallel
            |> Async.Ignore

    /// Split a suite into the concurrent pool and the timing-sensitive
    /// serial tail (registry-tagged; runs AFTER the pool drains, one at a
    /// time). At concurrency 1 there is no split: everything runs in
    /// registry order — the serial escape hatch reproduces the pre-pool
    /// behavior exactly.
    let private partitionForPool (config: RunnerConfig) (serialTags: string list) (proofs: ProofSpec list) =
        if config.Concurrency <= 1 then
            proofs, []
        else
            proofs
            |> List.partition (fun proof -> not (serialTags |> List.contains proof.Name))

    let listProofs (proofs: ProofSpec list) =
        for proof in proofs do
            let description = proof.Description |> Option.defaultValue ""
            stdout (sprintf "%s - %s" proof.Name description)

            for property in proof.Properties do
                stdout (sprintf "  %s" property.Name)

    let private reportProperty (log: string -> unit) (report: PropertyReport) =
        for check in report.Checks do
            if check.Passed then
                log (sprintf "  + %s" check.Name)
            else
                log (sprintf "  x %s" check.Name)

                match check.Message with
                | Some message -> log (sprintf "      %s" message)
                | None -> ()

        for control in report.NegativeControls do
            if control.Passed then
                log (sprintf "  + negative: %s" control.Name)
            else
                log (sprintf "  x negative: %s" control.Name)

                match control.Message with
                | Some message -> log (sprintf "      %s" message)
                | None -> ()

        log (sprintf "  report: %s" report.ReportPath)

    /// Human/CI mode: run every proof matching the filter; the exit code
    /// carries failures (unlike targets mode, where the ratchet judges the
    /// emitted result lines). Concurrency > 1 buffers each proof's output
    /// and flushes it as one block on completion (completion order);
    /// concurrency 1 streams in registry order exactly as before.
    let run (config: RunnerConfig) (serialTags: string list) (proofs: ProofSpec list) : Async<int> =
        async {
            let selected = proofs |> List.filter (matchesFilter config.ProofFilter)

            if List.isEmpty selected then
                stderr "no proofs matched filter"
                return 1
            else
                return!
                    withSharedS2 config (fun config ->
                        async {
                            let mutable passed = 0
                            let mutable failed = 0

                            let runProof (proof: ProofSpec) =
                                async {
                                    let buffer = ResizeArray<string>()
                                    let log = if config.Concurrency <= 1 then stdout else buffer.Add

                                    // Lifecycle markers straight to stderr, unbuffered:
                                    // pooled output flushes only on completion, so a
                                    // wedged law would otherwise never appear in the
                                    // log — the tail must name what was in flight
                                    // (C4 CI diagnosability ruling; diagnostics-only
                                    // harness amendment, stderr marker lines only).
                                    stderr (sprintf "law start: %s" proof.Name)
                                    log (sprintf "### %s" proof.Name)

                                    for property in proof.Properties do
                                        let! report = property.RunProperty config proof.Name

                                        if report.Passed then
                                            passed <- passed + 1
                                        else
                                            failed <- failed + 1

                                        reportProperty log report

                                    stderr (sprintf "law end: %s" proof.Name)

                                    for line in buffer do
                                        stdout line
                                }

                            let pooled, serialTail = partitionForPool config serialTags selected

                            do!
                                runPool
                                    config.Concurrency
                                    (pooled |> List.map (fun proof -> fun () -> runProof proof))

                            for proof in serialTail do
                                do! runProof proof

                            stdout (sprintf "%d properties passed, %d failed" passed failed)
                            return if failed = 0 then 0 else 1
                        })
        }

    /// Replay a recorded trial: reuse the recorded trial id, preserve the
    /// trial directory (the root is derived from the report path,
    /// <root>/<trial-id>/report.json), and re-enter the compiled runner with
    /// the recorded property selected.
    let replay (config: RunnerConfig) (reportPath: string) (proofs: ProofSpec list) : Async<int> =
        let spec = Reports.readReplaySpec reportPath
        let root = parentDir (parentDir reportPath)

        stderr (sprintf "replay report: %s" spec.ReportPath)
        stderr (sprintf "replay property: %s trial: %s" spec.PropertyName spec.TrialId)
        stderr (sprintf "recorded replay command: %s" spec.ReplayCommand)

        // A replay selects one recorded property: run it serially (its own
        // shared s2-lite instance still boots, matching runner semantics).
        run
            { config with
                Root = root
                ProofFilter = Some spec.PropertyName
                TrialId = Some spec.TrialId
                Preserve = true
                Concurrency = 1 }
            []
            proofs

    /// Ratchet targets mode (targets-README.md): one { "id", "pass" } JSON
    /// line per proof on stdout, all diagnostics on stderr, exit 0 when the
    /// suite itself ran to completion — per-proof failures live in the
    /// result lines, not the exit code.
    let targets (config: RunnerConfig) (serialTags: string list) (proofs: ProofSpec list) : Async<int> =
        withSharedS2 config (fun config ->
            async {
                // One pool job per proof; a proof's properties (and its
                // negative-control trials) run sequentially INSIDE its job,
                // so controls count against the same pool slot. The result
                // line is printed only here at completion — one atomic
                // stdout line per proof, completion order (the ratchet is
                // order-insensitive) — with the proof's buffered
                // diagnostics flushed to stderr just before it. Concurrency
                // 1 streams diagnostics live, exactly the pre-pool
                // behavior.
                let runProof (proof: ProofSpec) =
                    async {
                        let buffer = ResizeArray<string>()
                        let log = if config.Concurrency <= 1 then stderr else buffer.Add
                        let mutable pass = true

                        // Lifecycle markers straight to stderr, unbuffered:
                        // pooled output flushes only on completion, so a
                        // wedged law would otherwise never appear in the
                        // log — the tail must name what was in flight
                        // (C4 CI diagnosability ruling; diagnostics-only
                        // harness amendment, stderr marker lines only).
                        stderr (sprintf "law start: %s" proof.Name)

                        for property in proof.Properties do
                            // A property that CRASHES the runner (outside the
                            // workload/check paths, which already catch) still fails
                            // as a law — the suite must run to completion and emit
                            // one result line per registered proof.
                            let! outcome = Async.Catch(property.RunProperty config proof.Name)

                            match outcome with
                            | Choice1Of2 report ->
                                if not report.Passed then
                                    pass <- false

                                reportProperty log report
                            | Choice2Of2 error ->
                                pass <- false
                                log (sprintf "  x property '%s' crashed: %s" property.Name error.Message)

                        stderr (sprintf "law end: %s (%s)" proof.Name (if pass then "ok" else "FAILED"))

                        for line in buffer do
                            stderr line

                        stdout (sprintf "{ \"id\": \"%s\", \"pass\": %b }" proof.Name pass)
                    }

                let pooled, serialTail = partitionForPool config serialTags proofs

                do! runPool config.Concurrency (pooled |> List.map (fun proof -> fun () -> runProof proof))

                for proof in serialTail do
                    do! runProof proof

                return 0
            })
