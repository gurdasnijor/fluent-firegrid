namespace Firegrid.Foundation.Proofs

open Fable.Core
open Fable.Core.JsInterop

module Program =
    [<Emit("process.argv.slice(2)")>]
    let private argv () : string array = jsNative

    [<Emit("process.cwd()")>]
    let private cwd () : string = jsNative

    [<Emit("process.exitCode = $0")>]
    let private setExitCode (_code: int) : unit = jsNative

    [<Emit("console.log($0)")>]
    let private log (_message: string) : unit = jsNative

    [<Emit("console.error($0)")>]
    let private error (_message: string) : unit = jsNative

    let private proofs =
        [ FoundationSubjectHistoryProof.proof
          FoundationStateViewProof.proof
          FoundationKvStoreProof.proof
          FoundationDurableKernelProof.proof ]

    let private usage =
        "Usage: node dist/Program.js proof list | proof run <all|proof-name> [--report-dir <dir>] [--trial-id <id>] [--seed <n>]"

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
        |> Option.defaultValue (Reports.join [ cwd (); ".verification-reports"; "foundation-proofs" ])

    let private seed args =
        argValue "--seed" args
        |> Option.bind (fun text ->
            match System.Int32.TryParse text with
            | true, value -> Some value
            | false, _ -> None)
        |> Option.defaultValue 0

    let private runProof (config: RunnerConfig) (proof: ProofSpec) =
        async {
            let mutable passed = true

            for property in proof.Properties do
                let! report = property.RunProperty config proof.Name

                if report.Passed then
                    log (sprintf "proof %s property %s: passed" proof.Name report.PropertyName)
                else
                    passed <- false
                    error (sprintf "proof %s property %s: failed; report %s" proof.Name report.PropertyName report.ReportPath)

            return passed
        }

    let private runProofs name args =
        async {
            let selected =
                if name = "all" then
                    proofs
                else
                    proofs |> List.filter (fun proof -> proof.Name = name)

            if List.isEmpty selected then
                error (sprintf "unknown proof: %s" name)
                return 1
            else
                let config =
                    { Root = reportDir args
                      ProofFilter = if name = "all" then None else Some name
                      TrialId = argValue "--trial-id" args
                      Preserve = true
                      Seed = seed args }

                let mutable passed = true

                for proof in selected do
                    let! proofPassed = runProof config proof
                    passed <- passed && proofPassed

                return if passed then 0 else 1
        }

    let private main () =
        async {
            let args = argv () |> Array.toList

            match args with
            | [ "proof"; "list" ] ->
                proofs
                |> List.iter (fun proof ->
                    let description = proof.Description |> Option.defaultValue ""
                    log (sprintf "%s - %s" proof.Name description))

                return 0
            | "proof" :: "run" :: name :: rest -> return! runProofs name rest
            | _ ->
                error usage
                return 1
        }

    main ()
    |> Async.StartAsPromise
    |> Promise.map (fun code ->
        setExitCode code
        ())
    |> ignore
