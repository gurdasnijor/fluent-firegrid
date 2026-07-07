/**
 * The Claude Agent SDK adapter, built on WP D2's contract (`@firegrid/harness-adapter`):
 * the `claudeLowering` plugged into the generic reconstruction shell, plus the
 * declared capabilities and the native-resume-artifact projection (the Claude
 * session id).
 *
 * The shipped adapter drives *recorded* transcripts (the proof path) and mediates
 * no durable tools, so it declares `observe-only`. A gateable live variant — the
 * real SDK `query()` feeding `ClaudeEvent`s and Firegrid durable tools mediated
 * through `ToolGate` — is a follow-up at the agent-ui integration (WP E4); it
 * reuses this same lowering.
 */

import type {
  HarnessCapabilities,
  HarnessSource,
  L1Terminal,
  NativeResumeArtifact
} from "@firegrid/harness-adapter"
import {
  makeReconstructionAdapter,
  reconstructionAdapterLayer
} from "@firegrid/harness-adapter"
import * as Effect from "effect/Effect"

import type { ClaudeEvent } from "./events.ts"
import { claudeLowering, type ClaudeLoweringState, terminalOfResult } from "./lowering.ts"

/** Stable harness id — also the `firegrid/native` / usage `harness` tag. */
export const CLAUDE_HARNESS = "claude-agent-sdk"

export const claudeCapabilities: HarnessCapabilities = {
  harness: CLAUDE_HARNESS,
  interception: "observe-only",
  emitsUsage: true,
  emitsSubagents: true
}

const latestSessionId = (events: ReadonlyArray<ClaudeEvent>): string | undefined => {
  let sessionId: string | undefined = undefined
  for (const event of events) {
    if (typeof event.session_id === "string") sessionId = event.session_id
  }
  return sessionId
}

const terminalOf = (events: ReadonlyArray<ClaudeEvent>): L1Terminal => {
  for (const event of events) {
    if (event.type === "result") return terminalOfResult(event)
  }
  return { _tag: "completed" }
}

/** The native resume artifact is the Claude session id (opaque to the kernel). */
export const claudeResumeArtifact = (events: ReadonlyArray<ClaudeEvent>): NativeResumeArtifact => ({
  harness: CLAUDE_HARNESS,
  version: 1,
  payload: { sessionId: latestSessionId(events) }
})

/**
 * A reconstruction source over a recorded Claude transcript. `run` yields the
 * full recorded event sequence (the shell applies resume-suppression); a live
 * adapter would instead drive the SDK `query()` here.
 */
export const recordedClaudeSource = (
  events: ReadonlyArray<ClaudeEvent>
): HarnessSource<ClaudeEvent, ClaudeLoweringState> => ({
  lowering: claudeLowering,
  capabilities: claudeCapabilities,
  run: () =>
    Effect.succeed({
      events,
      artifact: claudeResumeArtifact(events),
      terminal: terminalOf(events)
    })
})

/** Build a `HarnessAdapter` service value over a recorded Claude transcript. */
export const makeClaudeAdapter = (events: ReadonlyArray<ClaudeEvent>) =>
  makeReconstructionAdapter(recordedClaudeSource(events))

/** Provide the Claude adapter as a `HarnessAdapter` layer. */
export const claudeAdapterLayer = (events: ReadonlyArray<ClaudeEvent>) =>
  reconstructionAdapterLayer(recordedClaudeSource(events))
