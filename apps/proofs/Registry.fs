/// Compiled registry of proofs, grouped into named ratchet suites (the
/// Runner/Registry split from the proof-runner SDD). Product proof corpora
/// migrate into this registry in Packets 0.2-0.4.
namespace Firegrid.Foundation.Proofs

module Registry =
    let suites: SuiteSpec list =
        [ { Suite = "p0-harness"
            Proofs = [ HarnessKillDemoProof.proof ] } ]

    let all: ProofSpec list =
        suites |> List.collect (fun suite -> suite.Proofs)
