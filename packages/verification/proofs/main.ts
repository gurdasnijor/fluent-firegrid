import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"
import { effectS2SubstrateProofs } from "./effect-s2-substrate-proofs.ts"
import fluentFiregridHostCrashRestartProof from "./fluent-firegrid-host-crash-restart.ts"
import tanstackWorkflowS2HostCrashRestartProof from "./tanstack-workflow-s2-host-crash-restart.ts"
import tanstackWorkflowS2HostTickProof from "./tanstack-workflow-s2-host-tick.ts"
import tanstackWorkflowS2EventLogProof from "./tanstack-workflow-s2-event-log.ts"
import tanstackWorkflowS2LeasesProof from "./tanstack-workflow-s2-leases.ts"
import tanstackWorkflowS2RunLifecycleProof from "./tanstack-workflow-s2-run-lifecycle.ts"
import tanstackWorkflowS2RuntimeApprovalProof from "./tanstack-workflow-s2-runtime-approval.ts"
import tanstackWorkflowS2RuntimeProof from "./tanstack-workflow-s2-runtime.ts"
import tanstackWorkflowS2RuntimeScheduleSweepProof from "./tanstack-workflow-s2-runtime-schedule-sweep.ts"
import tanstackWorkflowS2RuntimeTimerSweepProof from "./tanstack-workflow-s2-runtime-timer-sweep.ts"
import tanstackWorkflowS2TimersSignalsProof from "./tanstack-workflow-s2-timers-signals.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs,
  fluentFiregridHostCrashRestartProof,
  tanstackWorkflowS2HostCrashRestartProof,
  tanstackWorkflowS2HostTickProof,
  tanstackWorkflowS2EventLogProof,
  tanstackWorkflowS2RunLifecycleProof,
  tanstackWorkflowS2LeasesProof,
  tanstackWorkflowS2TimersSignalsProof,
  tanstackWorkflowS2RuntimeProof,
  tanstackWorkflowS2RuntimeApprovalProof,
  tanstackWorkflowS2RuntimeTimerSweepProof,
  tanstackWorkflowS2RuntimeScheduleSweepProof
] as const

NodeRuntime.runMain(runCli(proofs))
