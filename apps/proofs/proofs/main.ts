import * as NodeRuntime from "@effect/platform-node/NodeRuntime"

import { runCli } from "../src/CliApp.ts"
import effectS2CapabilityAProof from "./effect-s2-capability-a.ts"
import { effectS2SubstrateProofs } from "./effect-s2-substrate-proofs.ts"
import fluentFiregridS2AwakeableProof from "./store-awakeable.ts"
import fluentFiregridS2ObjectIndexWaitProof from "./store-object-index-wait.ts"
import fluentFiregridS2ObjectCrossHostProof from "./store-object-cross-host.ts"
import fluentFiregridS2ObjectDelayedSendProof from "./store-object-delayed-send.ts"
import fluentFiregridS2ObjectHandlesProof from "./store-object-handles.ts"
import fluentFiregridS2ObjectLiveFencingProof from "./store-object-live-fencing.ts"
import fluentFiregridS2ObjectReplayStateProof from "./store-object-replay-state.ts"
import fluentFiregridS2ObjectSerializationProof from "./store-object-serialization.ts"
import fluentFiregridS2ObjectStateWaitProof from "./store-object-state-wait.ts"
import fluentFiregridS2ObjectStateProof from "./store-object-state.ts"
import fluentFiregridS2ObjectStaleOwnerProof from "./store-object-stale-owner.ts"
import fluentFiregridS2ServiceDelayedSendProof from "./store-service-delayed-send.ts"
import fluentFiregridS2WorkflowScheduleProof from "./store-workflow-schedule.ts"
import tanstackWorkflowS2HostCrashRestartProof from "./store-host-crash-restart.ts"
import tanstackWorkflowS2HostTickProof from "./store-host-tick.ts"
import tanstackWorkflowS2EventLogProof from "./store-event-log.ts"
import tanstackWorkflowS2LeasesProof from "./store-leases.ts"
import tanstackWorkflowS2RunLifecycleProof from "./store-run-lifecycle.ts"
import tanstackWorkflowS2RuntimeApprovalProof from "./store-runtime-approval.ts"
import tanstackWorkflowS2RuntimeProof from "./store-runtime.ts"
import tanstackWorkflowS2RuntimeScheduleSweepProof from "./store-runtime-schedule-sweep.ts"
import tanstackWorkflowS2RuntimeTimerSweepProof from "./store-runtime-timer-sweep.ts"
import tanstackWorkflowS2TimersSignalsProof from "./store-timers-signals.ts"

const proofs = [
  effectS2CapabilityAProof,
  ...effectS2SubstrateProofs,
  fluentFiregridS2AwakeableProof,
  fluentFiregridS2ObjectIndexWaitProof,
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
  fluentFiregridS2WorkflowScheduleProof,
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
