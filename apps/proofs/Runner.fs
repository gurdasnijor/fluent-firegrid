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

    let private matchesFilter (filter: string option) (proof: ProofSpec) =
        match filter with
        | None -> true
        | Some value ->
            proof.Name.Contains value
            || proof.Properties |> List.exists (fun property -> property.Name.Contains value)

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
    /// emitted result lines).
    let run (config: RunnerConfig) (proofs: ProofSpec list) : Async<int> =
        async {
            let selected = proofs |> List.filter (matchesFilter config.ProofFilter)

            if List.isEmpty selected then
                stderr "no proofs matched filter"
                return 1
            else
                let mutable passed = 0
                let mutable failed = 0

                for proof in selected do
                    stdout (sprintf "### %s" proof.Name)

                    for property in proof.Properties do
                        let! report = property.RunProperty config proof.Name

                        if report.Passed then
                            passed <- passed + 1
                        else
                            failed <- failed + 1

                        reportProperty stdout report

                stdout (sprintf "%d properties passed, %d failed" passed failed)
                return if failed = 0 then 0 else 1
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

        run
            { config with
                Root = root
                ProofFilter = Some spec.PropertyName
                TrialId = Some spec.TrialId
                Preserve = true }
            proofs

    /// Ratchet targets mode (targets-README.md): one { "id", "pass" } JSON
    /// line per proof on stdout, all diagnostics on stderr, exit 0 when the
    /// suite itself ran to completion — per-proof failures live in the
    /// result lines, not the exit code.
    let targets (config: RunnerConfig) (proofs: ProofSpec list) : Async<int> =
        async {
            for proof in proofs do
                let mutable pass = true

                for property in proof.Properties do
                    let! report = property.RunProperty config proof.Name

                    if not report.Passed then
                        pass <- false

                    reportProperty stderr report

                stdout (sprintf "{ \"id\": \"%s\", \"pass\": %b }" proof.Name pass)

            return 0
        }
