import type { PickleTable, TestStepResultStatus } from "@cucumber/messages"

/**
 * Serializable types shared across the cucumber runner.
 *
 * The runtime is a command/event system: the **data plane** is the cucumber
 * `Envelope` stream (facts), and the **control plane** is the wire requests —
 * `begin` / `invoke` / `end` — as durable commands to the per-scenario object.
 * Step-definition identity (`StepDefId`) is owned by the step host, exactly as
 * the cucumber-wire protocol's `step_matches` returns ids and `invoke` takes one.
 */

/** A parsed feature file's raw content, ready for `generateMessages`. */
export interface SourceInput {
  readonly uri: string
  readonly data: string
  /** A `@cucumber/messages` `SourceMediaType` value. */
  readonly mediaType: string
}

export interface RunOptions {
  readonly scenarioConcurrency?: number
}

export interface RunInput {
  /** Stable id for this run; keys the durable event stream + dedups re-runs. */
  readonly runId: string
  readonly sources: ReadonlyArray<SourceInput>
  readonly options: RunOptions
}

/** Host-owned step-definition identity (the wire protocol's step id). */
export type StepDefId = string

/** The wire `invoke` command: which step to run + the pickle step's text/argument. */
export interface InvokeRequest {
  readonly stepDefId: StepDefId
  readonly text: string
  readonly docString?: string
  readonly dataTable?: PickleTable
}

/** One step in the prepared plan: a runnable invoke, or a static undefined/ambiguous outcome. */
export type PreparedStep =
  | { readonly _tag: "invoke"; readonly testStepId: string; readonly request: InvokeRequest }
  | { readonly _tag: "undefined"; readonly testStepId: string }
  | { readonly _tag: "ambiguous"; readonly testStepId: string; readonly message: string }

/** One scenario the core will drive, with all ids resolved up front. */
export interface PreparedScenario {
  readonly testCaseId: string
  readonly testCaseStartedId: string
  readonly scenarioId: string
  readonly tags: ReadonlyArray<string>
  readonly steps: ReadonlyArray<PreparedStep>
}

/** A captured attachment, part of a step's outcome, mapped to an envelope by the core. */
export interface CapturedAttachment {
  readonly body: string
  readonly mediaType: string
  readonly contentEncoding: string
  readonly fileName?: string
}

/** The outcome of one `invoke` (the wire `success`/`fail`/`pending`). */
export interface StepOutcome {
  readonly status: TestStepResultStatus
  readonly attachments: ReadonlyArray<CapturedAttachment>
  readonly error?: { readonly type: string; readonly message: string; readonly stackTrace?: string }
}

/** The run's envelopes are published to the durable event stream, not returned. */
export interface RunResult {
  readonly success: boolean
}

export interface BeginScenarioInput {
  readonly scenarioId: string
  readonly tags: ReadonlyArray<string>
}
