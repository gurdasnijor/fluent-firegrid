import type { Envelope, TestStepResultStatus } from "@cucumber/messages"

/**
 * Serializable types crossing the durable call boundary. The support module
 * (step-body closures) is NOT here — it is captured in the coordinator/worker
 * closures, never serialized. Everything in this file round-trips as plain JSON,
 * so the durable handlers use opaque JSON I/O (the default `Schema.Unknown`).
 */

/** A parsed feature file's raw content, ready for `generateMessages`. */
export interface SourceInput {
  readonly uri: string
  readonly data: string
  /** A `@cucumber/messages` `SourceMediaType` value. */
  readonly mediaType: string
}

/** Run-level options. CCK mode is `scenarioConcurrency: 1` (the default). */
export interface RunOptions {
  readonly scenarioConcurrency?: number
}

export interface RunInput {
  readonly sources: ReadonlyArray<SourceInput>
  readonly options: RunOptions
}

export interface RunResult {
  readonly envelopes: ReadonlyArray<Envelope>
  readonly statuses: ReadonlyArray<TestStepResultStatus>
  readonly success: boolean
}

export interface ScenarioAttemptInput {
  readonly sources: ReadonlyArray<SourceInput>
  readonly options: RunOptions
  readonly testCaseId: string
  readonly testCaseStartedId: string
  readonly attempt: number
}

export interface ScenarioAttemptResult {
  readonly envelopes: ReadonlyArray<Envelope>
  readonly statuses: ReadonlyArray<TestStepResultStatus>
}
