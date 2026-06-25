import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"
import { effectS2SubstrateProofs } from "./effect-s2-substrate-proofs.ts"
import fluentFiregridS2AwakeableProof from "./fluent-firegrid-s2-awakeable.ts"
import fluentFiregridS2ObjectCrossHostProof from "./fluent-firegrid-s2-object-cross-host.ts"
import fluentFiregridS2ObjectDelayedSendProof from "./fluent-firegrid-s2-object-delayed-send.ts"
import fluentFiregridS2ObjectHandlesProof from "./fluent-firegrid-s2-object-handles.ts"
import fluentFiregridS2ObjectLiveFencingProof from "./fluent-firegrid-s2-object-live-fencing.ts"
import fluentFiregridS2ObjectReplayStateProof from "./fluent-firegrid-s2-object-replay-state.ts"
import fluentFiregridS2ObjectSerializationProof from "./fluent-firegrid-s2-object-serialization.ts"
import fluentFiregridS2ObjectStateWaitProof from "./fluent-firegrid-s2-object-state-wait.ts"
import fluentFiregridS2ObjectStateProof from "./fluent-firegrid-s2-object-state.ts"
import fluentFiregridS2ObjectStaleOwnerProof from "./fluent-firegrid-s2-object-stale-owner.ts"
import fluentFiregridS2ServiceDelayedSendProof from "./fluent-firegrid-s2-service-delayed-send.ts"
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
  fluentFiregridS2AwakeableProof,
  fluentFiregridS2ObjectCrossHostProof,
  fluentFiregridS2ObjectDelayedSendProof,
  fluentFiregridS2ObjectHandlesProof,
  fluentFiregridS2ObjectLiveFencingProof,
  fluentFiregridS2ObjectReplayStateProof,
  fluentFiregridS2ObjectSerializationProof,
  fluentFiregridS2ObjectStateWaitProof,
  fluentFiregridS2ObjectStateProof,
  fluentFiregridS2ObjectStaleOwnerProof,
  fluentFiregridS2ServiceDelayedSendProof,
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
