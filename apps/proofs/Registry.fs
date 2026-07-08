/// Compiled registry of proofs, grouped into named ratchet suites (the
/// Runner/Registry split from the proof-runner SDD). Product proof corpora
/// migrate into this registry in Packets 0.2-0.4.
namespace Firegrid.Foundation.Proofs

module Registry =
    /// The migrated T1 corpus, in manifest order (targets.json t1-durable).
    /// Laws land per milestone (M1 core/delivery/observation, M2 flow/wire,
    /// M3 the red laws); the t1-durable suite command repoints here only in
    /// the M4 atomic swap.
    let corpusProofs: ProofSpec list =
        [ CoreLawProofs.replayDeterminismAcrossKill
          CoreLawProofs.fanoutAndJoin
          CoreLawProofs.taggedSelectRace
          CoreLawProofs.signalToParkedAcrossRestart
          CoreLawProofs.timerAcrossRestart
          CoreLawProofs.typedStepFailure
          CoreLawProofs.deterministicCurrentTime
          CoreLawProofs.statusAndResultQuery
          CoreLawProofs.andbangTeaching
          CoreLawProofs.boundedLoopFlatStack ]

    let suites: SuiteSpec list =
        [ { Suite = "p0-harness"
            Proofs = [ HarnessKillDemoProof.proof ] }
          { Suite = "t1-durable"; Proofs = corpusProofs } ]

    let all: ProofSpec list =
        suites |> List.collect (fun suite -> suite.Proofs)

    /// Child scenario hosts for the kill/zombie laws: this same compiled
    /// binary re-entered as `child <scenario>` (corpus Program.fs dispatch).
    let childScenarios: (string * (unit -> Async<int>)) list =
        [ "replay-host", ReplayScenario.childHost ]
