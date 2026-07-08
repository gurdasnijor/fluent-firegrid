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
          EntityLawProofs.entityExclusiveSerialization
          EntityLawProofs.entityZombieFenced
          EntityLawProofs.entitySharedReadNonblocking
          CoreLawProofs.typedStepFailure
          CoreLawProofs.deterministicCurrentTime
          CoreLawProofs.statusAndResultQuery
          StreamLawProofs.logAttachByteFaithful
          StreamLawProofs.threeReadGrades
          StreamLawProofs.celWait
          FlowLawProofs.sagaCompensationAcrossKill
          FlowLawProofs.recoverableCancellation
          FlowLawProofs.declareImplementRoundtrip
          FlowLawProofs.childSpawn
          CoreLawProofs.andbangTeaching
          StreamLawProofs.goldenWireFixtures
          FlowLawProofs.eternalContinueAsNew
          CoreLawProofs.boundedLoopFlatStack ]

    /// The foundation suite (Packet 0.3b consolidation over the 0.3a
    /// migration). Invariant-family proofs (foundation.fencing, …) register
    /// WHOLE — one ratchet id whose pass is the conjunction of its
    /// instantiations. The kept singles still register one single-property
    /// ProofSpec per property name (docs/proofs-inventory.md section B ids):
    /// `proof targets foundation` emits one { id, pass } line per ratchet id.
    /// Kept proof bodies are untouched — registry hookup only.
    let private perProperty (spec: ProofSpec) : ProofSpec list =
        spec.Properties
        |> List.map (fun property ->
            { Name = property.Name
              Description = spec.Description
              Properties = [ property ] })

    let foundationProofs: ProofSpec list =
        [ FoundationFencingProof.proof ]
        @ ([ FoundationSubjectHistoryProof.proof
             FoundationStateViewProof.proof
             FoundationStateReadsProof.proof
             FoundationSessionHistoryProof.proof
             FoundationKvStoreProof.proof
             FoundationCheckpointProof.proof
             FoundationCheckpointTrimSafetyProof.proof
             FoundationDurableKernelProof.proof
             FoundationDurableDebtsProof.proof
             FoundationParallelActivitiesProof.proof
             FoundationTurnStreamProof.proof
             FoundationSessionLifecycleProof.proof
             FoundationWakePathProof.proof ]
           |> List.collect perProperty)

    let suites: SuiteSpec list =
        [ { Suite = "p0-harness"
            Proofs = [ HarnessKillDemoProof.proof ] }
          { Suite = "t1-durable"; Proofs = corpusProofs }
          { Suite = "foundation"; Proofs = foundationProofs } ]

    let all: ProofSpec list =
        suites |> List.collect (fun suite -> suite.Proofs)

    /// Child scenario hosts for the kill/zombie laws: this same compiled
    /// binary re-entered as `child <scenario>` (corpus Program.fs dispatch).
    let childScenarios: (string * (unit -> Async<int>)) list =
        [ "replay-host", ReplayScenario.childHost
          "zombie-host", CounterEntity.childHost
          "saga-host", SagaScenario.childHost ]
