import { defineSimulation } from "../../types.ts"
import { idempotentProducerSubstrateDriver } from "./driver.ts"

// tf-n3qc proof-sequence #3: idempotent producer restart reads back before
// redoing side effects.
//
// SDD_FLUENT_RUNTIME_WORKBENCH "Durable Streams Substrate Commitments" /
// "Session And Turn Stream Model": idempotent producer headers (Producer-Id,
// Producer-Epoch, Producer-Seq) provide retry safety and zombie fencing, so a
// re-driver that replays a side-effecting append does not double-write. This
// substrate workbench (launchHost: false) observes: (a) a re-sent (id,epoch,seq)
// dedups to one row, (b) a lower epoch is fenced, (c) a sequence gap is rejected.
//
// No `coverage` spec by design: launchHost:false means no Firegrid host, so the
// trace carries no host-substrate spans (every span is firegrid.side=driver) and
// a forge-proof gate has nothing to bind to. The deliverable is the prose finding
// per the methodology's substrate carve-out; the runner reports "no computed verdict".
export default defineSimulation({
  id: "idempotent-producer-substrate",
  description:
    "Substrate workbench: re-appending the same Producer-Id/Epoch/Seq dedups to a "
    + "single row (retry safety), a lower Producer-Epoch is fenced (zombie "
    + "fencing), and a Producer-Seq gap is rejected (ordering).",
  launchHost: false,
  driver: idempotentProducerSubstrateDriver,
})
