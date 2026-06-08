import { defineSimulation } from "../../types.ts"
import { forkChildSessionSubstrateDriver } from "./driver.ts"

// tf-n3qc proof-sequence #1: Durable Streams fork child-session inheritance.
//
// SDD_FLUENT_RUNTIME_WORKBENCH "Durable Streams Substrate Commitments" claims
// agent spawn/spawn_all maps onto a stream fork: the child inherits the parent's
// event history up to the fork offset, then diverges, with no bespoke
// child-session row family. This is a SUBSTRATE workbench (launchHost: false):
// the finding is about the Durable Streams wire (PUT + Stream-Forked-From /
// Stream-Fork-Offset), not the Firegrid client/host seam.
//
// No `coverage` spec by design: launchHost:false means no Firegrid host, so the
// trace carries no host-substrate spans (every span is firegrid.side=driver) and
// a forge-proof gate has nothing to bind to. The deliverable is the prose finding
// per the methodology's substrate carve-out; the runner reports "no computed verdict".
export default defineSimulation({
  id: "fork-child-session-substrate",
  description:
    "Substrate workbench: fork a child stream from a parent via Stream-Forked-From "
    + "/ Stream-Fork-Offset, observe the child inherits parent history to the fork "
    + "offset then diverges, and the parent is unaffected by child appends.",
  launchHost: false,
  driver: forkChildSessionSubstrateDriver,
})
