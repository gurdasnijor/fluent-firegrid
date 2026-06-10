import { defineSimulation } from "../../types.ts"
import { streamClosureSubstrateDriver } from "./driver.ts"

// tf-n3qc proof-sequence #2: turn completion = Stream-Closed, not
// Stream-Up-To-Date.
//
// SDD_FLUENT_RUNTIME_WORKBENCH "Session And Turn Stream Model" makes terminality
// a hard constraint: `Stream-Up-To-Date` is only a catch-up point and cannot
// prove a turn is done; a finite turn stream is terminal only when it returns
// `Stream-Closed: true`, written via atomic append-and-close. This substrate
// workbench (launchHost: false) observes that distinction on the wire and that a
// closed stream rejects further appends.
//
// No `coverage` spec by design: launchHost:false means no Firegrid host, so the
// trace carries no host-substrate spans (every span is firegrid.side=driver) and
// a forge-proof gate has nothing to bind to. The deliverable is the prose finding
// per the methodology's substrate carve-out; the runner reports "no computed verdict".
export default defineSimulation({
  id: "stream-closure-substrate",
  description:
    "Substrate workbench: a catching-up reader sees Stream-Up-To-Date while the "
    + "stream is still open (not terminal); the terminal result is written with "
    + "atomic append-and-close (Stream-Closed: true) and a closed stream rejects "
    + "further appends.",
  launchHost: false,
  driver: streamClosureSubstrateDriver,
})
