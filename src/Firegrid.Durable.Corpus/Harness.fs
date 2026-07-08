/// Corpus harness: assertions, side-channel counters, the s2-lite
/// environment, child-host processes, and the law registry. Everything here
/// is test INFRASTRUCTURE — laws touch the platform only through
/// `Firegrid.Durable`'s public surface. `Firegrid.Log`'s `S2` module is used
/// solely to construct the `S2.Basin` values that surface signatures
/// (`Worker.run`, `Client.connect`) require.
namespace Firegrid.Durable.Corpus

open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Log

/// One law of the T1 corpus. `Run` throws (with a reason) to fail; the
/// runner emits `{ "id": ..., "pass": ... }` per the T0 ratchet protocol.
type Law =
    { Id: string
      TimeoutMs: int
      Run: unit -> Async<unit> }

module Expect =
    let isTrue (label: string) (condition: bool) =
        if not condition then
            failwith ("expectation failed: " + label)

    let equal (label: string) (expected: 'a) (actual: 'a) =
        if expected <> actual then
            failwith (sprintf "expectation failed: %s — expected %A, got %A" label expected actual)

/// File-backed side channels: step-handler execution evidence that survives
/// process kills and is shared with child host processes.
module Counter =
    let private file (scratch: string) (name: string) =
        Node.join [ scratch; name + ".count" ]

    let read (scratch: string) (name: string) : int =
        let p = file scratch name
        if Node.exists p then int (Node.readFile p) else 0

    let bump (scratch: string) (name: string) : unit =
        Node.ensureDir scratch
        let p = file scratch name
        let current = if Node.exists p then int (Node.readFile p) else 0
        Node.writeFile p (string (current + 1))

    let writeOnce (scratch: string) (name: string) (value: string) : unit =
        Node.ensureDir scratch
        let p = Node.join [ scratch; name + ".value" ]
        if not (Node.exists p) then Node.writeFile p value

    let readValue (scratch: string) (name: string) : string option =
        let p = Node.join [ scratch; name + ".value" ]
        if Node.exists p then Some(Node.readFile p) else None

    let appendLine (scratch: string) (name: string) (line: string) : unit =
        Node.ensureDir scratch
        Node.appendFile (Node.join [ scratch; name + ".lines" ]) (line + "\n")

    let readLines (scratch: string) (name: string) : string list =
        let p = Node.join [ scratch; name + ".lines" ]
        if Node.exists p then
            Node.readFile p
            |> fun text -> text.Split('\n')
            |> Array.toList
            |> List.filter (fun line -> line <> "")
        else
            []

module Harness =
    /// s2-lite processes and child hosts spawned during the run — killed at
    /// the end of the run even when a law times out mid-flight.
    let spawned = ResizeArray<Node.ChildProcess>()

    let killAll () =
        for proc in spawned do
            try proc.kill "SIGKILL" |> ignore with _ -> ()

    let mutable private runRoot: string option = None

    let private root () =
        match runRoot with
        | Some r -> r
        | None ->
            let r = Node.join [ Node.cwd (); ".corpus-scratch"; "run-" + Node.entropy () ]
            Node.ensureDir r
            runRoot <- Some r
            r

    /// Per-law scratch directory (pure local fs; no platform dependency).
    let scratchFor (slug: string) : string =
        let dir = Node.join [ root (); slug ]
        Node.ensureDir dir
        dir

    type Env =
        { Basin: S2.Basin
          BasinName: string
          Endpoint: string }

    let private waitReady (endpoint: string) =
        let rec loop remaining =
            async {
                let! ready = Node.fetchReady endpoint |> Async.AwaitPromise

                if ready then return ()
                elif remaining <= 0 then return failwith ("s2 lite did not become ready at " + endpoint)
                else
                    do! Node.sleep 100
                    return! loop (remaining - 1)
            }

        loop 100

    let connectClient (endpoint: string) =
        S2.connectWith
            { S2.ConnectOptions.create "t1-corpus" with
                AccountEndpoint = Some endpoint
                BasinEndpoint = Some endpoint }

    /// Spin up a private s2-lite + basin for one law, run the law's body
    /// against it, and tear the process down afterwards. Laws touch the
    /// surface (which throws `notYet` while red) BEFORE calling this, so a
    /// red run never needs the s2 binary.
    let withEnv (slug: string) (body: Env -> Async<unit>) : Async<unit> =
        async {
            let localRoot = Node.join [ scratchFor slug; "s2" ]
            Node.ensureDir localRoot
            let port = Node.randomPort ()
            let endpoint = sprintf "http://127.0.0.1:%d" port

            let proc =
                Node.spawn
                    (Node.s2Bin ())
                    [| "lite"; "--port"; string port; "--local-root"; localRoot |]
                    (createObj [ "stdio" ==> "ignore" ])

            spawned.Add proc

            try
                do! waitReady endpoint
                let client = connectClient endpoint
                let basinName = "t1-" + Node.entropy ()
                let! _ = client |> S2.createBasin basinName
                let basin = client |> S2.basin basinName

                do!
                    body
                        { Basin = basin
                          BasinName = basinName
                          Endpoint = endpoint }

                proc.kill "SIGTERM" |> ignore
            with error ->
                proc.kill "SIGKILL" |> ignore
                return raise error
        }

    /// Spawn this same corpus binary as a child HOST process for a scenario
    /// (see `Program.fs` child dispatch). The child connects to the law's
    /// s2-lite, registers the scenario's definitions, and runs a worker until
    /// the parent kills it.
    let spawnChildHost (env: Env) (scenario: string) (ns: string) (scratch: string) : Node.ChildProcess =
        let childEnv =
            createObj
                [ "T1C_ENDPOINT" ==> env.Endpoint
                  "T1C_BASIN" ==> env.BasinName
                  "T1C_NS" ==> ns
                  "T1C_SCRATCH" ==> scratch ]

        let proc =
            Node.spawn
                (Node.nodePath ())
                [| Node.scriptPath (); "child"; scenario |]
                // Child stdout is routed to OUR stderr: under the T0 ratchet
                // runner, this process's stdout is reserved for result lines.
                (createObj
                    [ "stdio" ==> [| box "ignore"; box 2; box 2 |]
                      "env" ==> Node.withProcessEnv childEnv ])

        spawned.Add proc
        proc

    /// Child-side: reconstruct the basin from the env the parent passed.
    let childBasin () : S2.Basin =
        let client = connectClient (Node.env "T1C_ENDPOINT")
        client |> S2.basin (Node.env "T1C_BASIN")

    /// Child-side: park forever (the parent decides when this host dies).
    let foreverChild () : Async<int> =
        let rec loop () =
            async {
                do! Node.sleep 1000
                return! loop ()
            }

        loop ()

    /// Poll a predicate until it holds or the deadline lapses.
    let until (label: string) (deadlineMs: int) (probe: unit -> Async<bool>) : Async<unit> =
        let rec loop remaining =
            async {
                let! ok = probe ()

                if ok then return ()
                elif remaining <= 0 then return failwith ("timed out waiting for: " + label)
                else
                    do! Node.sleep 100
                    return! loop (remaining - 100)
            }

        loop deadlineMs

    let untilCount (scratch: string) (name: string) (atLeast: int) (deadlineMs: int) : Async<unit> =
        until
            (sprintf "side-channel %s >= %d" name atLeast)
            deadlineMs
            (fun () -> async { return Counter.read scratch name >= atLeast })

    /// Bound a law by wall clock so a partially-green surface cannot hang the
    /// corpus run (a red surface fails fast on its first `notYet`).
    let withTimeout (millis: int) (work: Async<unit>) : Async<unit> =
        async {
            let promise = work |> Async.StartAsPromise
            let! winner = Node.raceTimeout promise millis |> Async.AwaitPromise

            if not (isNull winner) then
                return failwith (sprintf "law timed out after %dms" millis)
        }
