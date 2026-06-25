import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"
import effectS2FlowCapabilityAStepReplayProof from "./effect-s2-flow-capability-a-step-replay.ts"
import effectS2FlowCapabilityBFenceProof from "./effect-s2-flow-capability-b-fence.ts"
import effectS2FlowCapabilityBStateProof from "./effect-s2-flow-capability-b-state.ts"
import { effectS2SubstrateProofs } from "./effect-s2-substrate-proofs.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs,
  effectS2FlowCapabilityAStepReplayProof,
  effectS2FlowCapabilityBStateProof,
  effectS2FlowCapabilityBFenceProof
] as const

NodeRuntime.runMain(runCli(proofs))
