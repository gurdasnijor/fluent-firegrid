import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import { effectS2CapabilityAProof, firegridLogCapabilityAProof } from "./effect-s2-capability-a.ts"
import { effectS2SubstrateProofs, firegridLogSubstrateProofs } from "./effect-s2-substrate-proofs.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs,
  firegridLogCapabilityAProof,
  ...firegridLogSubstrateProofs
] as const

NodeRuntime.runMain(runCli(proofs))
