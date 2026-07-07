import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import { claudeFixtureReplayProof, claudeSubagentScopingProof, claudeUsageFactsProof } from "./claude-adapter.ts"
import { effectS2CapabilityAProof, firegridLogCapabilityAProof } from "./effect-s2-capability-a.ts"
import { effectS2SubstrateProofs, firegridLogSubstrateProofs } from "./effect-s2-substrate-proofs.ts"
import { harnessFixtureReplayProof, harnessResumeSuppressionProof } from "./harness-fixture-replay.ts"
import { l1VocabularyConformanceProof } from "./l1-vocabulary-conformance.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs,
  firegridLogCapabilityAProof,
  ...firegridLogSubstrateProofs,
  l1VocabularyConformanceProof,
  harnessFixtureReplayProof,
  harnessResumeSuppressionProof,
  claudeSubagentScopingProof,
  claudeFixtureReplayProof,
  claudeUsageFactsProof
] as const

NodeRuntime.runMain(runCli(proofs))
