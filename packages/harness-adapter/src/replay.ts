/**
 * The pure lowering core — Effect-free, deterministic, the `harness.fixture-replay`
 * target. Folding a `HarnessLowering` over a recorded event sequence reproduces an
 * identical L1 record sequence; all nondeterminism lives in the harness process,
 * behind the I/O shell (`drive`).
 */

import type { L1StreamRecord } from "@firegrid/l1-vocabulary"
import type { HarnessLowering } from "./contract.ts"

/**
 * Fold a lowering over an ordered event sequence, returning the full L1 record
 * sequence. Pure: no I/O, no clock, no entropy — same events in, same records out.
 */
export const replay = <Event, State>(
  lowering: HarnessLowering<Event, State>,
  events: ReadonlyArray<Event>
): ReadonlyArray<L1StreamRecord> =>
  events.reduce<{ readonly state: State; readonly out: ReadonlyArray<L1StreamRecord> }>(
    (acc, event) => {
      const step = lowering.lower(acc.state, event)
      return { state: step.state, out: [...acc.out, ...step.records] }
    },
    { state: lowering.initial, out: [] }
  ).out
