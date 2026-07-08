/// ═══════════════════════════════════════════════════════════════════════
/// Support code for the migrated T1 corpus laws (Packet 0.2): file-backed
/// side channels shared with child scenario hosts, deadline polling, basin
/// bootstrap, law-shaped checks, and the child-host env contract. Test
/// INFRASTRUCTURE only — the system under test is reached exclusively
/// through Firegrid.Durable's public surface. Ported from
/// src/Firegrid.Durable.Corpus/{Node,Harness}.fs; the corpus's own child
/// process / s2-lite / kill mechanics are replaced by harness resources
/// (s2Lite, processHost, ctx.Faults).
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Foundation.Proofs

open Fable.Core
open Fable.Core.JsInterop
open Firegrid.Log

module CorpusNode =
    let private fs: obj = importAll "node:fs"

    [<Emit("$0.existsSync($1)")>]
    let private existsWith (_fs: obj) (_path: string) : bool = jsNative

    [<Emit("$0.readFileSync($1, 'utf8')")>]
    let private readFileWith (_fs: obj) (_path: string) : string = jsNative

    let exists (path: string) = existsWith fs path
    let readFile (path: string) = readFileWith fs path

    [<Emit("process.env[$0] || ''")>]
    let env (_name: string) : string = jsNative

    [<Emit("process.argv[1]")>]
    let entryScript () : string = jsNative

    [<Emit("process.execPath")>]
    let nodeBin () : string = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleepPromise (_millis: int) : JS.Promise<unit> = jsNative

    let sleep (millis: int) = sleepPromise millis |> Async.AwaitPromise

/// File-backed side channels: step-handler execution evidence that survives
/// process kills and is shared with child scenario hosts (corpus mechanic,
/// unchanged).
module Counter =
    let private file (scratch: string) (name: string) =
        Reports.join [ scratch; name + ".count" ]

    let read (scratch: string) (name: string) : int =
        let path = file scratch name

        if CorpusNode.exists path then
            int (CorpusNode.readFile path)
        else
            0

    let bump (scratch: string) (name: string) : unit =
        Reports.ensureDir scratch
        let path = file scratch name

        let current =
            if CorpusNode.exists path then
                int (CorpusNode.readFile path)
            else
                0

        Reports.write path (string (current + 1))

    let writeOnce (scratch: string) (name: string) (value: string) : unit =
        Reports.ensureDir scratch
        let path = Reports.join [ scratch; name + ".value" ]

        if not (CorpusNode.exists path) then
            Reports.write path value

    let readValue (scratch: string) (name: string) : string option =
        let path = Reports.join [ scratch; name + ".value" ]

        if CorpusNode.exists path then
            Some(CorpusNode.readFile path)
        else
            None

    let appendLine (scratch: string) (name: string) (line: string) : unit =
        Reports.ensureDir scratch
        Reports.append (Reports.join [ scratch; name + ".lines" ]) (line + "\n")

    let readLines (scratch: string) (name: string) : string list =
        let path = Reports.join [ scratch; name + ".lines" ]

        if CorpusNode.exists path then
            CorpusNode.readFile path
            |> fun text -> text.Split('\n')
            |> Array.toList
            |> List.filter (fun line -> line <> "")
        else
            []

/// Law-shaped checks over a workload's observation record: the corpus's
/// Expect.equal/isTrue as harness Check values (expected/actual detail in
/// the failure message).
module LawCheck =
    let isTrue (name: string) (predicate: 'result -> bool) : Check<'result> = Expect.workload name predicate

    let holds (name: string) (predicate: 'result -> bool) (detail: 'result -> string) : Check<'result> =
        { Name = name
          RunCheck =
            fun trial ->
                async {
                    match trial.Result with
                    | Error error -> return Error("workload failed: " + error)
                    | Ok result ->
                        if predicate result then
                            return Ok()
                        else
                            return Error(detail result)
                } }

    let equal (name: string) (project: 'result -> 'value) (expected: 'value) : Check<'result> =
        { Name = name
          RunCheck =
            fun trial ->
                async {
                    match trial.Result with
                    | Error error -> return Error("workload failed: " + error)
                    | Ok result ->
                        let actual = project result

                        if actual = expected then
                            return Ok()
                        else
                            return Error(sprintf "expected %A, got %A" expected actual)
                } }

module CorpusSupport =
    /// Per-trial scratch shared with child hosts: the workload derives it
    /// from ctx.Root; children derive it from the FIREGRID_TRIAL_ROOT the
    /// runner injects — the same directory.
    let scratchOf (ctx: WorkloadContext) : string = Reports.join [ ctx.Root; "scratch" ]

    let childScratch () : string =
        Reports.join [ CorpusNode.env "FIREGRID_TRIAL_ROOT"; "scratch" ]

    /// Poll a predicate until it holds or the deadline lapses (corpus
    /// mechanic, unchanged).
    let until (label: string) (deadlineMs: int) (probe: unit -> Async<bool>) : Async<unit> =
        let rec loop remaining =
            async {
                let! ok = probe ()

                if ok then
                    return ()
                elif remaining <= 0 then
                    return failwith ("timed out waiting for: " + label)
                else
                    do! CorpusNode.sleep 100
                    return! loop (remaining - 100)
            }

        loop deadlineMs

    let untilCount (scratch: string) (name: string) (atLeast: int) (deadlineMs: int) : Async<unit> =
        until (sprintf "side-channel %s >= %d" name atLeast) deadlineMs (fun () ->
            async { return Counter.read scratch name >= atLeast })

    /// Idempotent basin bootstrap: the workload AND any child scenario host
    /// both ensure the (fixed, per-law) basin on the trial's private
    /// s2-lite; whichever runs first creates it, the other's ensure/create
    /// is a no-op.
    let ensureBasin (endpoint: string) (basinName: string) : Async<S2.Basin> =
        async {
            let client =
                S2.connectWith
                    { S2.ConnectOptions.create "t1-corpus" with
                        AccountEndpoint = Some endpoint
                        BasinEndpoint = Some endpoint }

            do!
                async {
                    try
                        let! _ = client |> S2.ensureBasin basinName
                        return ()
                    with _ ->
                        // ensure unsupported by this s2-lite build: fall back
                        // to create-if-missing (a loser of the create race
                        // proceeds; real connectivity failures resurface at
                        // the law's first append with their own message).
                        try
                            let! _ = client |> S2.createBasin basinName
                            return ()
                        with _ ->
                            return ()
                }

            return client |> S2.basin basinName
        }

    let workloadBasin (ctx: WorkloadContext) (basinName: string) : Async<S2.Basin> =
        let s2 = WorkloadContext.requireS2 ctx

        match s2.Endpoint with
        | Some endpoint -> ensureBasin endpoint basinName
        | None -> failwith "corpus law requires an s2Lite resource (an S2 endpoint); declare s2Lite before hosts"

    /// Child-side: reconstruct the basin from the env the runner injected
    /// (S2_ENDPOINT from the declared s2 resource, T1C_BASIN from the law's
    /// host spec).
    let childBasin () : Async<S2.Basin> =
        ensureBasin (CorpusNode.env "S2_ENDPOINT") (CorpusNode.env "T1C_BASIN")

    let childNamespace () : string = CorpusNode.env "T1C_NS"

    /// Child-side: park forever (the runner decides when this host dies).
    let foreverChild () : Async<int> =
        let rec loop () =
            async {
                do! CorpusNode.sleep 1000
                return! loop ()
            }

        loop ()

    /// Host spec for a corpus child scenario: this same compiled Main.js
    /// re-entered as `child <scenario>` (the corpus Program.fs child
    /// dispatch, now a runner-owned processHost resource).
    let childHostSpec (hostName: string) (scenario: string) (basinName: string) (ns: string) : ProcessHostSpec =
        { ProcessHostSpec.create hostName (CorpusNode.nodeBin ()) with
            Args = [ CorpusNode.entryScript (); "child"; scenario ]
            Env = [ "T1C_BASIN", basinName; "T1C_NS", ns ] }

    /// Exactly-one-accepted-kill trace evidence for a named host (the
    /// kill-demo SQL, parameterized).
    let killSpanSql (hostName: string) =
        sprintf
            """
SELECT countIf(
  name = 'verification.host.kill'
  AND JSONExtractString(toJSONString(attributes), 'host.name') = '%s'
  AND JSONExtractString(toJSONString(attributes), 'verification.signal') = 'SIGKILL'
  AND JSONExtractString(toJSONString(attributes), 'verification.accepted') = 'true'
) = 1 AS ok
FROM trial_spans
"""
            hostName

    /// The standard law-operation trace match: the law's umbrella
    /// ProofOperation completed ok, exactly once.
    let lawOp (lawId: string) =
        { TraceOperationMatch.named lawId with
            Status = Some "ok"
            Count = Some 1 }
