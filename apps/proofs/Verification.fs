namespace Firegrid.Foundation.Proofs

type ExpectVerifiers<'result>() =
    member _.Workload (name: string) (predicate: 'result -> bool) : Check<'result> = Expect.workload name predicate

    member _.WorkloadResult (name: string) (expected: 'result) : Check<'result> =
        Expect.workload name (fun actual -> Reports.json (box actual) = Reports.json (box expected))

    member _.WorkloadResultBy<'value> (name: string) (project: 'result -> 'value) (expected: 'value) : Check<'result> =
        Expect.workload name (fun actual -> Reports.json (box (project actual)) = Reports.json (box expected))

type TraceVerifiers<'result>() =
    member _.SpanExists (label: string) (spanName: string) (attributes: (string * string) list) : Check<'result> =
        TraceExpect.spanExists label spanName attributes

    member _.Sql (name: string) (sql: string) : Check<'result> =
        TraceProof.sql name sql |> TraceProof.asCheck

    member _.Operation (name: string) (matchSpec: TraceOperationMatch) : Check<'result> =
        TraceProof.operation name matchSpec |> TraceProof.asCheck

type HostVerifiers<'result>() =
    member _.Started(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' started" hostName)
            "verification.host.start"
            [ "host.name", hostName ]

    member _.Ready(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' became ready" hostName)
            "verification.host.ready"
            [ "host.name", hostName ]

    member _.Stopped(hostName: string) : Check<'result> =
        TraceExpect.spanExists (sprintf "host '%s' stopped" hostName) "verification.host.stop" [ "host.name", hostName ]

type FaultVerifiers<'result>() =
    member private _.HostFaultReported
        (label: string)
        (kind: string)
        (signal: string)
        (hostName: string)
        : Check<'result> =
        { Name = label
          RunCheck =
            fun trial ->
                async {
                    let reported =
                        trial.Faults
                        |> List.exists (fun fault ->
                            fault.Kind = kind
                            && fault.Target = hostName
                            && fault.Signal = Some signal
                            && fault.Accepted = Some true)

                    if reported then
                        return Ok()
                    else
                        return Error(sprintf "%s fault not reported: %s" kind hostName)
                } }

    member _.HostPaused(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' was paused" hostName)
            "verification.host.pause"
            [ "host.name", hostName
              "verification.signal", "SIGSTOP"
              "verification.accepted", "true" ]

    member _.HostResumed(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' was resumed" hostName)
            "verification.host.resume"
            [ "host.name", hostName
              "verification.signal", "SIGCONT"
              "verification.accepted", "true" ]

    member this.HostPauseReported(hostName: string) : Check<'result> =
        this.HostFaultReported (sprintf "host '%s' pause fault was reported" hostName) "hostPause" "SIGSTOP" hostName

    member this.HostResumeReported(hostName: string) : Check<'result> =
        this.HostFaultReported (sprintf "host '%s' resume fault was reported" hostName) "hostResume" "SIGCONT" hostName

    member _.HostKilled(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' was killed" hostName)
            "verification.host.kill"
            [ "host.name", hostName; "verification.signal", "SIGKILL" ]

    member _.HostKillAccepted(hostName: string) : Check<'result> =
        TraceExpect.spanExists
            (sprintf "host '%s' kill signal was accepted" hostName)
            "verification.host.kill"
            [ "host.name", hostName
              "verification.signal", "SIGKILL"
              "verification.accepted", "true" ]

    member _.HostKillReported(hostName: string) : Check<'result> =
        { Name = sprintf "host '%s' kill fault was reported" hostName
          RunCheck =
            fun trial ->
                async {
                    let reported =
                        trial.Faults
                        |> List.exists (fun fault ->
                            fault.Kind = "hostKill"
                            && fault.Target = hostName
                            && fault.Signal = Some "SIGKILL"
                            && fault.Accepted = Some true)

                    if reported then
                        return Ok()
                    else
                        return Error(sprintf "host kill fault not reported: %s" hostName)
                } }

type Verifiers<'result> =
    { Expect: ExpectVerifiers<'result>
      Trace: TraceVerifiers<'result>
      Host: HostVerifiers<'result>
      Fault: FaultVerifiers<'result> }

module Verification =
    let verifiers<'result> () : Verifiers<'result> =
        { Expect = ExpectVerifiers<'result>()
          Trace = TraceVerifiers<'result>()
          Host = HostVerifiers<'result>()
          Fault = FaultVerifiers<'result>() }
