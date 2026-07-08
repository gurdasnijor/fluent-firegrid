/// Compiled registry of harness self-proofs (the Runner/Registry split from
/// the proof-runner SDD). Product proof corpora migrate into this registry
/// in Packets 0.2-0.4.
namespace Firegrid.Foundation.Proofs

module Registry =
    let all: ProofSpec list = [ HarnessKillDemoProof.proof ]
