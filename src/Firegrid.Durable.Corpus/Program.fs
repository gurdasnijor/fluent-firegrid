/// ═══════════════════════════════════════════════════════════════════════
/// T1 red corpus runner — the T0 ratchet protocol.
///
/// `corpus run all` executes every law and emits ONE JSON line per law on
/// stdout: { "id": "<law id>", "pass": <bool> }. Human-readable detail goes
/// to stderr. The process exit code is 0 when the run itself completed —
/// red laws are EXPECTED until greened; the ratchet runner (T0) diffs the
/// emitted lines against `targets.json` and decides CI.
///
/// `child <scenario>` runs this binary as a child HOST process for the
/// kill/zombie laws (see Harness.spawnChildHost).
/// ═══════════════════════════════════════════════════════════════════════
namespace Firegrid.Durable.Corpus

open Fable.Core

module Program =

    let laws: Law list =
        [ CoreLaws.replayDeterminismAcrossKill
          CoreLaws.fanoutAndJoin
          CoreLaws.taggedSelectRace
          CoreLaws.signalToParkedAcrossRestart
          CoreLaws.timerAcrossRestart
          EntityLaws.entityExclusiveSerialization
          EntityLaws.entityZombieFenced
          EntityLaws.entitySharedReadNonblocking
          CoreLaws.typedStepFailure
          CoreLaws.deterministicCurrentTime
          CoreLaws.statusAndResultQuery
          StreamLaws.logAttachByteFaithful
          StreamLaws.threeReadGrades
          StreamLaws.celWait
          FlowLaws.sagaCompensationAcrossKill
          FlowLaws.recoverableCancellation
          FlowLaws.declareImplementRoundtrip
          FlowLaws.childSpawn
          CoreLaws.andbangTeaching
          StreamLaws.goldenWireFixtures
          FlowLaws.eternalContinueAsNew
          CoreLaws.boundedLoopFlatStack ]

    let childScenarios: (string * (unit -> Async<int>)) list =
        [ "replay-host", ReplayScenario.childHost
          "zombie-host", CounterEntity.childHost
          "saga-host", SagaScenario.childHost ]

    let private emit (id: string) (pass: bool) =
        Node.stdout (sprintf "{ \"id\": \"%s\", \"pass\": %b }" id pass)

    let private runLaw (law: Law) : Async<bool> =
        async {
            let started = Node.nowMillis ()

            let! outcome =
                Async.Catch (
                    async {
                        // `law.Run ()` first touches the Firegrid.Durable
                        // surface — while red it throws `notYet` right here.
                        do! Harness.withTimeout law.TimeoutMs (async { do! law.Run () })
                    }
                )

            let elapsed = int (Node.nowMillis () - started)

            match outcome with
            | Choice1Of2 () ->
                emit law.Id true
                Node.stderr (sprintf "[%s] PASS (%dms)" law.Id elapsed)
                return true
            | Choice2Of2 error ->
                emit law.Id false
                Node.stderr (sprintf "[%s] FAIL (%dms) — %s" law.Id elapsed error.Message)
                return false
        }

    let private runAll (selector: string) : Async<int> =
        async {
            let selected =
                if selector = "all" then
                    laws
                else
                    laws |> List.filter (fun law -> law.Id = selector)

            if List.isEmpty selected then
                Node.stderr ("unknown law id: " + selector)
                return 1
            else
                let mutable passed = 0

                for law in selected do
                    let! ok = runLaw law
                    if ok then passed <- passed + 1

                Harness.killAll ()

                Node.stderr (
                    sprintf
                        "corpus: %d/%d green (red laws are the frozen spec; greening happens without editing them)"
                        passed
                        (List.length selected)
                )

                // Completing the run is success; the T0 ratchet judges the lines.
                return 0
        }

    let private usage =
        "Usage: node dist/Program.js corpus list | corpus run <all|law-id> | child <scenario>"

    let private main () =
        async {
            match Node.argv () |> Array.toList with
            | [ "corpus"; "list" ] ->
                for law in laws do
                    Node.stdout law.Id

                return 0
            | [ "corpus"; "run"; selector ] -> return! runAll selector
            | [ "child"; scenario ] ->
                match childScenarios |> List.tryFind (fun (name, _) -> name = scenario) with
                | Some (_, run) -> return! run ()
                | None ->
                    Node.stderr ("unknown child scenario: " + scenario)
                    return 1
            | _ ->
                Node.stderr usage
                return 1
        }

    main ()
    |> Async.StartAsPromise
    |> Promise.map (fun code -> Node.setExitCode code)
    |> ignore
