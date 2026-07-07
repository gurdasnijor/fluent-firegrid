/**
 * The generic reconstruction shell: the reusable I/O half of `drive` that any
 * `HarnessLowering` plugs into. It lowers a harness's events to L1 records, emits
 * the suffix under the kernel's fence via `L1Sink`, applies resume-suppression by
 * the exclusive-upper-bound convention, and returns the outcome. A live adapter
 * (D3) supplies a `HarnessSource` that drives a real process; the reference
 * source replays a recorded transcript.
 *
 * The sans-IO split lives here: the pure `lower`/`replay` core is deterministic
 * and testable; this shell is the only place I/O and the fenced sink appear.
 */

import type { L1StreamRecord } from "@firegrid/l1-vocabulary"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import type * as Scope from "effect/Scope"

import {
  type DriveInput,
  DriveError,
  type HarnessCapabilities,
  HarnessAdapter,
  type HarnessLowering,
  L1Sink,
  type L1Terminal,
  type NativeResumeArtifact
} from "./contract.ts"
import { replay } from "./replay.ts"

/** One harness turn's raw product: the event transcript, terminal, and artifact. */
export interface HarnessRun<Event> {
  readonly events: ReadonlyArray<Event>
  readonly artifact: NativeResumeArtifact
  readonly terminal: L1Terminal
}

/**
 * A harness-specific event source. For a live harness `run` drives the process
 * (needing a `Scope` for the process resource); for the reference/replay case it
 * yields a recorded transcript.
 */
export interface HarnessSource<Event, State> {
  readonly lowering: HarnessLowering<Event, State>
  readonly capabilities: HarnessCapabilities
  readonly run: (input: DriveInput) => Effect.Effect<HarnessRun<Event>, DriveError, Scope.Scope>
}

const readMessageId = (record: L1StreamRecord): string | undefined => {
  const value = (record as { readonly messageId?: unknown }).messageId
  return typeof value === "string" ? value : undefined
}

/**
 * The ratified prompt invariant: a non-empty run of `user_message_chunk`s sharing
 * one `messageId`. Returns a `DriveError` describing the first violation, or
 * `undefined` when valid.
 */
export const validatePrompt = (
  harness: string,
  prompt: DriveInput["prompt"]
): DriveError | undefined => {
  const nonUser = prompt.find((record) => record.sessionUpdate !== "user_message_chunk")
  if (nonUser !== undefined) {
    return new DriveError({
      harness,
      message: `prompt must be all user_message_chunks; saw ${nonUser.sessionUpdate}`
    })
  }
  const ids = new Set(
    prompt.map(readMessageId).filter((id): id is string => id !== undefined)
  )
  if (ids.size > 1) {
    return new DriveError({
      harness,
      message: `prompt chunks must share one messageId; saw ${[...ids].join(", ")}`
    })
  }
  return undefined
}

/**
 * Build a `HarnessAdapter` service value from a `HarnessSource`. `drive` validates
 * the prompt, runs the source, lowers its events deterministically, emits the
 * suffix from `observedThrough` onward under the fence, and returns the outcome.
 */
export const makeReconstructionAdapter = <Event, State>(
  source: HarnessSource<Event, State>
): HarnessAdapter["Service"] => {
  const harness = source.capabilities.harness
  return {
    capabilities: source.capabilities,
    drive: Effect.fn("HarnessAdapter.drive")(function*(input: DriveInput) {
      const invalid = validatePrompt(harness, input.prompt)
      if (invalid !== undefined) {
        return yield* invalid
      }
      const run = yield* source.run(input)
      const records = replay(source.lowering, run.events)
      // Exclusive upper bound: everything below `observedThrough` is already
      // durable, so emit only the suffix at Version >= observedThrough.
      const from = input.resume === undefined ? 0 : Math.max(0, input.resume.observedThrough)
      const suffix = from <= 0 ? records : records.slice(from)
      const sink = yield* L1Sink
      yield* Effect.forEach(
        suffix,
        (record) =>
          sink.emit(record).pipe(
            Effect.mapError((error) =>
              new DriveError({
                harness,
                message: `L1 emit failed (${error.kind}): ${error.message}`,
                cause: error
              })
            )
          ),
        { discard: true }
      )
      return { artifact: run.artifact, terminal: run.terminal }
    })
  }
}

/** Provide a reconstruction adapter as a `HarnessAdapter` layer. */
export const reconstructionAdapterLayer = <Event, State>(
  source: HarnessSource<Event, State>
): Layer.Layer<HarnessAdapter> => Layer.succeed(HarnessAdapter, makeReconstructionAdapter(source))
