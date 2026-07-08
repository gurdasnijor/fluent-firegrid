/// Runner-owned host processes: the spec/lifecycle shape comes from the
/// eff-firegrid MVP's ProcessHost.fs (declared there but never exercised by
/// a proof); the spawn/stdio/kill mechanics come from this repo's corpus
/// harness (src/Firegrid.Durable.Corpus/Harness.fs + Node.fs), which proved
/// them under the kill-heavy T1 laws.
///
/// Runner Order (proof-runner SDD): the runner starts declared hosts,
/// injects standard FIREGRID_* trial environment, waits for the readiness
/// probe, and emits verification.host.start/ready/stop/kill lifecycle spans.
/// Child stdout is routed to OUR stderr because under ratchet targets mode
/// this process's stdout is reserved for result lines.
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Fable.Core.JsInterop

module ProcessHost =
    type private ChildProcess =
        abstract pid: int
        abstract kill: signal: string -> bool

    type Instance =
        { Resource: HostResource
          Stop: unit -> Async<unit>
          Kill: unit -> Async<bool> }

    [<Import("spawn", "node:child_process")>]
    let private spawn (_command: string) (_args: string array) (_options: obj) : ChildProcess = jsNative

    [<Emit("Object.assign({}, process.env, $0)")>]
    let private mergeEnv (_env: obj) : obj = jsNative

    [<Emit("fetch($0).then(r => r.ok).catch(() => false)")>]
    let private fetchOk (_url: string) : JS.Promise<bool> = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleep (_millis: int) : JS.Promise<unit> = jsNative

    let private waitUntilReady name url attempts intervalMillis =
        let rec loop remaining =
            async {
                let! ready = fetchOk url |> Async.AwaitPromise

                if ready then
                    return ()
                elif remaining <= 0 then
                    return failwithf "processHost '%s' did not become ready at %s" name url
                else
                    do! sleep intervalMillis |> Async.AwaitPromise
                    return! loop (remaining - 1)
            }

        loop attempts

    let private options (store: TraceStore) (s2: S2Resource option) (spec: ProcessHostSpec) =
        let s2Env =
            match s2 |> Option.bind (fun resource -> resource.Endpoint) with
            | Some endpoint -> [ "S2_ENDPOINT", endpoint ]
            | None -> []

        let env =
            createObj
                [ yield! spec.Env |> List.map (fun (key, value) -> key ==> value)
                  "FIREGRID_HOST_ID" ==> spec.Name
                  "FIREGRID_TRIAL_ID" ==> store.TrialId
                  "FIREGRID_TRIAL_ROOT" ==> store.Root
                  "FIREGRID_SPANS_JSONL" ==> store.SpansJsonl
                  yield! s2Env |> List.map (fun (key, value) -> key ==> value) ]

        createObj
            [ // Child stdout -> our stderr: stdout is the ratchet protocol
              // channel (corpus-proven routing).
              "stdio" ==> [| box "ignore"; box 2; box 2 |]
              "env" ==> mergeEnv env
              match spec.Cwd with
              | Some cwd -> "cwd" ==> cwd
              | None -> () ]

    let start (store: TraceStore) (s2: S2Resource option) (spec: ProcessHostSpec) : Async<Instance> =
        async {
            let proc = spawn spec.Command (spec.Args |> List.toArray) (options store s2 spec)
            let running = ref true

            let terminate spanName signal =
                async {
                    if running.Value then
                        running.Value <- false
                        let accepted = proc.kill signal

                        do!
                            Reports.emitSpan
                                store
                                spanName
                                [ "host.name", spec.Name
                                  "host.pid", string proc.pid
                                  "verification.signal", signal
                                  "verification.accepted", string accepted ]

                        return accepted
                    else
                        return false
                }

            do!
                Reports.emitSpan
                    store
                    "verification.host.start"
                    [ "host.name", spec.Name
                      "host.pid", string proc.pid
                      "host.command", spec.Command ]

            try
                match spec.ReadinessUrl with
                | Some url ->
                    do!
                        waitUntilReady
                            spec.Name
                            url
                            (spec.ReadinessAttempts |> Option.defaultValue 120)
                            (spec.ReadinessIntervalMillis |> Option.defaultValue 100)

                    do!
                        Reports.emitSpan
                            store
                            "verification.host.ready"
                            [ "host.name", spec.Name
                              "host.pid", string proc.pid
                              "host.readiness_url", url ]
                | None -> ()

                return
                    { Resource =
                        { Name = spec.Name
                          ProcessId = proc.pid
                          ReadinessUrl = spec.ReadinessUrl }
                      Stop =
                        fun () ->
                            async {
                                let! _ = terminate "verification.host.stop" "SIGTERM"
                                return ()
                            }
                      Kill = fun () -> terminate "verification.host.kill" "SIGKILL" }
            with error ->
                running.Value <- false
                proc.kill "SIGKILL" |> ignore
                return raise error
        }
