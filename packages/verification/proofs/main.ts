import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"

const proofs = [
  effectS2CapabilityAProof
] as const

NodeRuntime.runMain(runCli(proofs))
