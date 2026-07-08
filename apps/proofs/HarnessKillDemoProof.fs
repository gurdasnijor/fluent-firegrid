/// P0.1 demonstration self-proof (runner canary `p0.harness-kill-demo`):
/// exercises the full harness surface end to end — a declared processHost
/// with a readiness probe, ctx.Faults.KillHost through the workload, dual
/// evidence (Expect on the workload result AND chdb TraceSql/TraceProof
/// queries over the trial's spans.jsonl, incl. verification.host.kill with
/// the accepted flag), a report-level hostKill fault event, and a negative
/// control whose no-kill variant fails the stopped-after-kill check.
namespace Firegrid.Foundation.Proofs

open Fable.Core

module HarnessKillDemoProof =
    type KillDemoResult =
        { HostPid: int
          ReadyBeforeKill: bool
          StoppedAfterKill: bool }

    [<Emit("20000 + Math.floor(Math.random() * 20000)")>]
    let private randomPort () : int = jsNative

    [<Emit("fetch($0).then(r => r.ok).catch(() => false)")>]
    let private fetchOk (_url: string) : JS.Promise<bool> = jsNative

    [<Emit("new Promise(resolve => setTimeout(resolve, $0))")>]
    let private sleep (_millis: int) : JS.Promise<unit> = jsNative

    [<Emit("process.argv[1]")>]
    let private entryScript () : string = jsNative

    [<Emit("process.execPath")>]
    let private nodeBin () : string = jsNative

    /// dist/Main.js -> ../hosts/demo-host.mjs, so the proof resolves its
    /// host script from the compiled entry location, not the caller's cwd.
    let private hostScript () =
        Reports.join [ entryScript (); ".."; ".."; "hosts"; "demo-host.mjs" ]

    let private port = randomPort ()
    let private readyUrl = sprintf "http://127.0.0.1:%d/ready" port

    let private hostSpec =
        { ProcessHostSpec.create "demo" (nodeBin ()) with
            Args = [ hostScript () ]
            Env = [ "FIREGRID_HOST_PORT", string port ]
            ReadinessUrl = Some readyUrl }

    let private pollUntilDown attempts intervalMillis =
        let rec loop remaining =
            async {
                let! up = fetchOk readyUrl |> Async.AwaitPromise

                if not up then return true
                elif remaining <= 0 then return false
                else
                    do! sleep intervalMillis |> Async.AwaitPromise
                    return! loop (remaining - 1)
            }

        loop attempts

    let private runDemo (kill: bool) (ctx: WorkloadContext) : Async<KillDemoResult> =
        ProofOperation.run
            ctx
            "p0.harness.kill-demo"
            {| Port = port; Kill = kill |}
            { ProofOperationOptions.empty with Key = Some "demo" }
            (async {
                let host = ctx |> WorkloadContext.requireHost "demo"
                let! readyBefore = fetchOk readyUrl |> Async.AwaitPromise

                if kill then
                    do! WorkloadContext.killHost "demo" ctx

                let! stopped = pollUntilDown (if kill then 100 else 15) 100

                return
                    { HostPid = host.ProcessId
                      ReadyBeforeKill = readyBefore
                      StoppedAfterKill = stopped }
            })

    let private killSpanSql =
        """
SELECT countIf(
  name = 'verification.host.kill'
  AND JSONExtractString(toJSONString(attributes), 'host.name') = 'demo'
  AND JSONExtractString(toJSONString(attributes), 'verification.signal') = 'SIGKILL'
  AND JSONExtractString(toJSONString(attributes), 'verification.accepted') = 'true'
) = 1 AS ok
FROM trial_spans
"""

    let private operationMatch =
        { TraceOperationMatch.named "p0.harness.kill-demo" with
            Status = Some "ok"
            Count = Some 1 }

    let private noKillControl =
        negativeControl<KillDemoResult> "no-kill variant fails the stopped-after-kill check" {
            workload (runDemo false)

            verify (fun v -> [ v.Expect.Workload "host stopped after kill" (fun result -> result.StoppedAfterKill) ])

            expectFailure "host stopped after kill"
        }

    let private killDemoProperty =
        property "p0.harness-kill-demo-proof" {
            processHost hostSpec

            workload (runDemo true)

            verify (fun v ->
                [ v.Expect.Workload "host was ready before kill" (fun result -> result.ReadyBeforeKill)
                  v.Expect.Workload "host stopped after kill" (fun result -> result.StoppedAfterKill)
                  v.Host.Started "demo"
                  v.Host.Ready "demo"
                  v.Fault.HostKillAccepted "demo"
                  v.Fault.HostKillReported "demo"
                  v.Trace.Operation "kill-demo operation recorded ok" operationMatch
                  v.Trace.Sql "kill span carries signal and accepted flag" killSpanSql ])

            negativeControl noKillControl
            requiresNegativeControl
        }

    let proof =
        proof "p0.harness-kill-demo" {
            describedAs
                "The harness can declare a runner-owned Node host, kill it through ctx.Faults.KillHost, and prove the kill from dual evidence (workload result + chdb trace queries over spans.jsonl); the no-kill variant fails the stopped-after-kill check."

            property killDemoProperty
        }
