/**
 * The reference harness lowering: an ACP-native harness whose protocol events
 * *are* L1 records — the near-trivial lowering the G2 rationale predicts ("future
 * ACP harnesses lower near-trivially"). It is the reference implementation of the
 * contract used to exercise the fixture-replay harness against D1's seed corpus.
 *
 * This is deliberately NOT the Claude adapter: WP D3 supplies Claude's non-trivial
 * lowering (parent_tool_use_id scoping, usage/cost facts) against this same
 * contract. The reference is `observe-only` (it mediates no durable tools), so its
 * layer never depends on `ToolGate`.
 */

import type { L1StreamRecord } from "@firegrid/l1-vocabulary"
import * as Effect from "effect/Effect"

import type { HarnessCapabilities, HarnessLowering, NativeResumeArtifact } from "./contract.ts"
import type { HarnessRun, HarnessSource } from "./reconstruction.ts"

/** The reference harness id, also the `firegrid/native` `harness` tag it would use. */
export const REFERENCE_HARNESS = "reference-acp"

/**
 * Pass-through lowering: an ACP-native event already *is* an `L1StreamRecord`, so
 * lowering emits it unchanged. Stateless (`State = null`) and pure.
 */
export const referenceLowering: HarnessLowering<L1StreamRecord, null> = {
  initial: null,
  lower: (state, event) => ({ state, records: [event] })
}

export const referenceCapabilities: HarnessCapabilities = {
  harness: REFERENCE_HARNESS,
  interception: "observe-only",
  emitsUsage: true,
  emitsSubagents: true
}

/** A resume artifact for the reference harness whose cursor is the record count. */
export const referenceArtifact = (recordCount: number): NativeResumeArtifact => ({
  harness: REFERENCE_HARNESS,
  version: 1,
  payload: { cursor: recordCount }
})

/**
 * A reconstruction source over a recorded L1 transcript (e.g. a D1 fixture). The
 * transcript is self-contained — replay is authoritative — so `run` ignores its
 * input and yields the full recorded sequence; the shell applies any
 * resume-suppression by `observedThrough`. A live adapter (D3) instead drives a
 * real process here and the process's echoed prompt appears in the transcript.
 */
export const recordedTranscriptSource = (
  records: ReadonlyArray<L1StreamRecord>,
  terminal: HarnessRun<L1StreamRecord>["terminal"] = { _tag: "completed" }
): HarnessSource<L1StreamRecord, null> => ({
  lowering: referenceLowering,
  capabilities: referenceCapabilities,
  run: () =>
    Effect.succeed({
      events: records,
      artifact: referenceArtifact(records.length),
      terminal
    })
})
