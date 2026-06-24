import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"
import { effectS2SubstrateProofs } from "./effect-s2-substrate-proofs.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs
] as const

NodeRuntime.runMain(runCli(proofs))
