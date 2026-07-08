/// CLI entry for the apps/proofs harness (proof-runner SDD "Commands"):
///
///   proof list
///   proof run <all|filter> [--report-dir <dir>] [--trial-id <id>] [--seed <n>]
///   proof replay <report.json>
///   proof targets p0-harness
///
/// Targets mode speaks the targets-README.md suite protocol: result JSONL on
/// stdout, diagnostics on stderr, exit 0 when the suite ran.
namespace Firegrid.Foundation.Proofs

open Fable.Core

module Main =
    [<Emit("process.argv.slice(2)")>]
    let private argv () : string array = jsNative

    [<Emit("process.cwd()")>]
    let private cwd () : string = jsNative

    [<Emit("process.exitCode = $0")>]
    let private setExitCode (_code: int) : unit = jsNative

    [<Emit("console.error($0)")>]
    let private stderr (_message: string) : unit = jsNative

    let private usage =
        "Usage: node dist/Main.js proof list | proof run <all|filter> [--report-dir <dir>] [--trial-id <id>] [--seed <n>] | proof replay <report.json> | proof targets <suite>"

    let private argValue name (args: string list) =
        args
        |> List.tryPick (fun value ->
            let prefix = name + "="

            if value.StartsWith(prefix) then
                Some(value.Substring(prefix.Length))
            else
                None)
        |> Option.orElseWith (fun () ->
            args
            |> List.pairwise
            |> List.tryPick (fun (left, right) -> if left = name then Some right else None))

    let private reportDir args =
        argValue "--report-dir" args
        |> Option.defaultValue (Reports.join [ cwd (); ".verification-reports"; "p0-harness" ])

    let private seed args =
        argValue "--seed" args
        |> Option.bind (fun text ->
            match System.Int32.TryParse text with
            | true, value -> Some value
            | false, _ -> None)
        |> Option.defaultValue 0

    let private config filter args =
        { Root = reportDir args
          ProofFilter = filter
          TrialId = argValue "--trial-id" args
          Preserve = true
          Seed = seed args }

    let private main () =
        async {
            match argv () |> Array.toList with
            | [ "proof"; "list" ] ->
                Runner.listProofs Registry.all
                return 0
            | "proof" :: "run" :: name :: rest ->
                let filter = if name = "all" then None else Some name
                return! Runner.run (config filter rest) Registry.all
            | "proof" :: "replay" :: reportPath :: rest ->
                return! Runner.replay (config None rest) reportPath Registry.all
            | [ "proof"; "targets"; suite ] ->
                match Registry.suites |> List.tryFind (fun spec -> spec.Suite = suite) with
                | Some spec ->
                    let root = Reports.join [ cwd (); ".verification-reports"; suite ]

                    return!
                        Runner.targets
                            { Root = root
                              ProofFilter = None
                              TrialId = None
                              Preserve = true
                              Seed = 0 }
                            spec.Proofs
                | None ->
                    let known =
                        Registry.suites |> List.map (fun spec -> spec.Suite) |> String.concat ", "

                    stderr ("unknown targets suite: " + suite + " (known: " + known + ")")
                    return 1
            | _ ->
                stderr usage
                return 1
        }

    main ()
    |> Async.StartAsPromise
    |> Promise.map (fun code -> setExitCode code)
    |> ignore
