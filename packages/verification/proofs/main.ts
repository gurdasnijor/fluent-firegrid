import * as NodeRuntime from "@effect/platform-node/NodeRuntime"
import * as Runtime from "effect/Runtime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"
import effectS2FlowCapabilityAIdempotentInvocationProof from "./effect-s2-flow-capability-a-idempotent-invocation.ts"
import effectS2FlowCapabilityAStepReplayProof from "./effect-s2-flow-capability-a-step-replay.ts"
import effectS2FlowCapabilityBLeaseRefreshProof from "./effect-s2-flow-capability-b-lease-refresh.ts"
import effectS2FlowCapabilityBOwnerContentionProof from "./effect-s2-flow-capability-b-contention.ts"
import effectS2FlowCapabilityBFenceProof from "./effect-s2-flow-capability-b-fence.ts"
import effectS2FlowCapabilityBStateProof from "./effect-s2-flow-capability-b-state.ts"
import effectS2FlowCapabilityCDurableSleepProof from "./effect-s2-flow-capability-c-sleep.ts"
import { effectS2SubstrateProofs } from "./effect-s2-substrate-proofs.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs,
  effectS2FlowCapabilityAStepReplayProof,
  effectS2FlowCapabilityAIdempotentInvocationProof,
  effectS2FlowCapabilityBStateProof,
  effectS2FlowCapabilityBFenceProof,
  effectS2FlowCapabilityBOwnerContentionProof,
  effectS2FlowCapabilityBLeaseRefreshProof,
  effectS2FlowCapabilityCDurableSleepProof
] as const

NodeRuntime.runMain(runCli(proofs), {
  teardown: (exit, onExit) =>
    Runtime.defaultTeardown(exit, (code) => {
      onExit(code)
      process.exit(code)
    })
})
