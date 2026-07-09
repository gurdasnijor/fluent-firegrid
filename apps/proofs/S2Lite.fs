namespace Firegrid.Foundation.Proofs

open Firegrid.Log
open Fable.Core
open Fable.Core.JsInterop

module S2Lite =
    type private ChildProcess =
        abstract kill: signal: string -> bool

    type Instance =
        { Resource: S2Resource
          Stop: unit -> Async<unit> }

    [<Import("spawn", "node:child_process")>]
    let private spawn (_command: string) (_args: string array) (_options: obj) : ChildProcess = jsNative

    [<Emit("fetch($0).then(() => true).catch(() => false)")>]
    let private fetchReady (_url: string) : JS.Promise<bool> = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleep (_millis: int) : JS.Promise<unit> = jsNative

    [<Emit("20000 + Math.floor(Math.random() * 20000)")>]
    let private randomPort () : int = jsNative

    [<Emit("process.env.S2_BIN || (process.env.HOME ? process.env.HOME + '/.s2/bin/s2' : 's2')")>]
    let private defaultBin () : string = jsNative

    /// Mutable exit marker for a spawned child: lets the readiness loop fail
    /// fast when the process dies before becoming ready (the bind-collision
    /// signature) instead of burning the full readiness window.
    type private ExitWatch =
        abstract exited: bool

    [<Emit("(p => { const s = { exited: false }; p.once('exit', () => { s.exited = true; }); return s; })($0)")>]
    let private watchExit (_proc: ChildProcess) : ExitWatch = jsNative

    let private waitUntilReady (watch: ExitWatch) endpoint =
        let rec loop remaining =
            async {
                if watch.exited then
                    return failwithf "s2 lite exited before becoming ready at %s (port bind collision?)" endpoint
                else
                    let! ready = fetchReady endpoint |> Async.AwaitPromise

                    if ready then
                        return ()
                    elif remaining <= 0 then
                        return failwithf "s2 lite did not become ready at %s" endpoint
                    else
                        do! sleep 100 |> Async.AwaitPromise
                        return! loop (remaining - 1)
            }

        loop 100

    let start (store: TraceStore) (requestedRoot: string) =
        let localRoot =
            if requestedRoot.Trim() = "" then
                Reports.join [ store.Root; "s2-lite" ]
            else
                requestedRoot

        Reports.ensureDir localRoot

        // Port-bind resilience (P0.3c ruling 7): when the process dies
        // before readiness — the bind-collision signature — retry on a NEW
        // random port, up to 5 attempts. An alive-but-unready server keeps
        // today's failure semantics (no retry).
        let rec attempt (remaining: int) =
            async {
                let port = randomPort ()
                let endpoint = sprintf "http://127.0.0.1:%d" port

                let proc =
                    spawn
                        (defaultBin ())
                        [| "lite"; "--port"; string port; "--local-root"; localRoot |]
                        (createObj [ "stdio" ==> "ignore" ])

                let watch = watchExit proc
                let! readiness = Async.Catch(waitUntilReady watch endpoint)

                match readiness with
                | Choice1Of2() ->
                    let client =
                        S2.connectWith
                            { S2.ConnectOptions.create "s2-lite-proof-runner" with
                                AccountEndpoint = Some endpoint
                                BasinEndpoint = Some endpoint }

                    do!
                        Reports.emitSpan
                            store
                            "verification.s2.lite.started"
                            [ "resource.kind", "s2Lite"
                              "s2.endpoint", endpoint
                              "s2.local_root", localRoot ]

                    return
                        { Resource =
                            { Client = client
                              Kind = "s2Lite"
                              Endpoint = Some endpoint
                              LocalRoot = Some localRoot }
                          Stop =
                            fun () ->
                                async {
                                    proc.kill "SIGTERM" |> ignore

                                    do!
                                        Reports.emitSpan
                                            store
                                            "verification.s2.lite.stopped"
                                            [ "resource.kind", "s2Lite"
                                              "s2.endpoint", endpoint
                                              "s2.local_root", localRoot ]
                                } }
                | Choice2Of2 error ->
                    proc.kill "SIGKILL" |> ignore

                    if watch.exited && remaining > 1 then
                        return! attempt (remaining - 1)
                    else
                        return raise error
            }

        attempt 5
