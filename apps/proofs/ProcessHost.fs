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
          Kill: unit -> Async<bool>
          Pause: unit -> Async<bool>
          Resume: unit -> Async<bool> }

    [<Import("spawn", "node:child_process")>]
    let private spawn (_command: string) (_args: string array) (_options: obj) : ChildProcess = jsNative

    [<Emit("Object.assign({}, process.env, $0)")>]
    let private mergeEnv (_env: obj) : obj = jsNative

    [<Emit("fetch($0).then(r => r.ok).catch(() => false)")>]
    let private fetchOk (_url: string) : JS.Promise<bool> = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleep (_millis: int) : JS.Promise<unit> = jsNative

    /// Mutable exit marker for a spawned child: lets the readiness loop fail
    /// fast when the host dies before becoming ready (the bind-collision
    /// signature under concurrent trials) instead of burning the readiness
    /// window.
    type private ExitWatch =
        abstract exited: bool

    [<Emit("(p => { const s = { exited: false }; p.once('exit', () => { s.exited = true; }); return s; })($0)")>]
    let private watchExit (_proc: ChildProcess) : ExitWatch = jsNative

    let private waitUntilReady (watch: ExitWatch) name url attempts intervalMillis =
        let rec loop remaining =
            async {
                if watch.exited then
                    return failwithf "processHost '%s' exited before becoming ready at %s (port bind collision?)" name url
                else
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

    /// One spawn attempt. When the host has a readiness URL and dies before
    /// readiness (the bind-collision signature — host ports are chosen by
    /// the law's frozen spec, so a respawn re-races the same port after the
    /// transient holder is gone), the caller retries up to 5 attempts total
    /// (P0.3c ruling 7). Alive-but-unready hosts keep today's failure
    /// semantics.
    let rec private startAttempt
        (store: TraceStore)
        (s2: S2Resource option)
        (spec: ProcessHostSpec)
        (attemptsLeft: int)
        : Async<Instance> =
        async {
            let proc = spawn spec.Command (spec.Args |> List.toArray) (options store s2 spec)
            let watch = watchExit proc
            let running = ref true

            let terminate spanName signal =
                async {
                    if running.Value then
                        running.Value <- false
                        let accepted = proc.kill signal

                        // A SIGTERM cannot reach a SIGSTOPped host: chase it
                        // with SIGCONT so a paused host wakes to act on the
                        // pending TERM (no-op for a running host). SIGKILL
                        // needs no chaser — it acts on stopped processes.
                        if signal = "SIGTERM" then
                            proc.kill "SIGCONT" |> ignore

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

            // Pause/Resume (SIGSTOP/SIGCONT) follow the kill pattern but do
            // NOT mark the host stopped: a paused host is still supervised
            // (Stop/Kill at release must still reach it).
            let signalHost spanName signal =
                async {
                    if running.Value then
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

            let instance =
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
                  Kill = fun () -> terminate "verification.host.kill" "SIGKILL"
                  Pause = fun () -> signalHost "verification.host.pause" "SIGSTOP"
                  Resume = fun () -> signalHost "verification.host.resume" "SIGCONT" }

            match spec.ReadinessUrl with
            | Some url ->
                let! readiness =
                    Async.Catch(
                        waitUntilReady
                            watch
                            spec.Name
                            url
                            (spec.ReadinessAttempts |> Option.defaultValue 120)
                            (spec.ReadinessIntervalMillis |> Option.defaultValue 100)
                    )

                match readiness with
                | Choice1Of2() ->
                    do!
                        Reports.emitSpan
                            store
                            "verification.host.ready"
                            [ "host.name", spec.Name
                              "host.pid", string proc.pid
                              "host.readiness_url", url ]

                    return instance
                | Choice2Of2 error ->
                    running.Value <- false
                    proc.kill "SIGKILL" |> ignore

                    if watch.exited && attemptsLeft > 1 then
                        return! startAttempt store s2 spec (attemptsLeft - 1)
                    else
                        return raise error
            | None -> return instance
        }

    let start (store: TraceStore) (s2: S2Resource option) (spec: ProcessHostSpec) : Async<Instance> =
        startAttempt store s2 spec 5
