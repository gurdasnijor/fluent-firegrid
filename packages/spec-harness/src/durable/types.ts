import type { Pickle, PickleDocString, PickleTable, TestStepResultStatus } from "@cucumber/messages"

/**
 * Serializable types that cross the durable boundary between the `runner`
 * (service, message authority) and the `world` (object, step-definition host).
 *
 * The "wire" here is durable RPC modelled on the Cucumber wire protocol
 * (`begin_scenario` / `step_matches` / `invoke` / `end_scenario`). Step-body
 * closures never cross it — they live in the `world` host's support library,
 * addressed by a registered bundle name. Everything here is plain JSON.
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
  /** Stable id for this run; keys the durable envelope stream + dedups re-runs. */
  readonly runId: string
  /** Name of the support bundle registered with `defineSupport`. */
  readonly supportName: string
  readonly sources: ReadonlyArray<SourceInput>
  readonly options: RunOptions
}

/** What the `world` host needs to execute one matched pickle step (the wire `invoke`). */
export interface StepInvocation {
  /** The pickle step text — the host re-matches it against its support library. */
  readonly text: string
  /** Argument values from the Cucumber-expression match (built-ins are JSON-safe). */
  readonly argValues: ReadonlyArray<unknown>
  readonly docString?: PickleDocString["content"]
  readonly dataTable?: PickleTable
}

export type StepKind =
  | { readonly _tag: "prepared"; readonly invocation: StepInvocation }
  | { readonly _tag: "undefined" }
  | { readonly _tag: "ambiguous"; readonly message: string }

/** One step the runner will drive: its envelope id plus how to execute it. */
export interface PlannedStep {
  readonly testStepId: string
  readonly always: boolean
  readonly kind: StepKind
}

/** One scenario the runner will drive, with all ids resolved up front. */
export interface PlannedScenario {
  readonly testCaseId: string
  readonly testCaseStartedId: string
  readonly scenarioId: string
  readonly tags: ReadonlyArray<string>
  readonly steps: ReadonlyArray<PlannedStep>
}

/** A captured attachment, returned from `world.invoke` and mapped to an envelope by the runner. */
export interface CapturedAttachment {
  readonly body: string
  readonly mediaType: string
  readonly contentEncoding: string
  readonly fileName?: string
}

/** The outcome of one `world.invoke` (the wire `success`/`fail`/`pending`). */
export interface StepOutcome {
  readonly status: TestStepResultStatus
  readonly attachments: ReadonlyArray<CapturedAttachment>
  readonly error?: { readonly type: string; readonly message: string; readonly stackTrace?: string }
}

/** The run's envelopes are published to the durable envelope stream, not returned. */
export interface RunResult {
  readonly success: boolean
}

export interface BeginScenarioInput {
  readonly supportName: string
  readonly scenarioId: string
  readonly tags: ReadonlyArray<string>
}

export type { Pickle }
