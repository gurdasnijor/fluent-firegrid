import type {
  Attachment,
  Duration,
  Envelope,
  Exception,
  SourceReference,
  StepDefinitionPatternType,
  StepMatchArgumentsList,
  TestStep,
  TestStepResult,
  TestStepResultStatus,
  Timestamp,
} from "@cucumber/messages"
import { type AttachmentContentEncoding, TestStepResultStatus as Status } from "@cucumber/messages"

/**
 * Cucumber Messages envelope constructors and result mapping for the durable
 * runner. These are pure data builders: the durable coordinator/worker emit
 * their output as ordered `Envelope[]`, so nothing here touches the clock,
 * randomness, or external effects. Timestamps and durations are emitted as
 * zero — the canonical CCK comparison normalizes them away, and a compliant
 * message stream is more valuable than wall-clock fidelity in this first cut.
 */

const ZERO_TIMESTAMP: Timestamp = { seconds: 0, nanos: 0 }
const ZERO_DURATION: Duration = { seconds: 0, nanos: 0 }

const PROTOCOL_VERSION = "31.1.0"

export const metaEnvelope = (): Envelope => ({
  meta: {
    protocolVersion: PROTOCOL_VERSION,
    implementation: { name: "cucumber-effect", version: "0.0.0" },
    cpu: { name: "unknown" },
    os: { name: "unknown" },
    runtime: { name: "node" },
  },
})

export const testRunStarted = (id: string): Envelope => ({
  testRunStarted: { id, timestamp: ZERO_TIMESTAMP },
})

// ── discovery / support code envelopes ─────────────────────────────────────

/** Support code is authored in-process; there is no source file to point at. */
const SUPPORT_SOURCE_REFERENCE: SourceReference = { uri: "cucumber-effect/support", location: { line: 0 } }

export const stepDefinitionEnvelope = (input: {
  readonly id: string
  readonly source: string
  readonly type: StepDefinitionPatternType
}): Envelope => ({
  stepDefinition: {
    id: input.id,
    pattern: { source: input.source, type: input.type },
    sourceReference: SUPPORT_SOURCE_REFERENCE,
  },
})

export const hookEnvelope = (input: {
  readonly id: string
  readonly name?: string
  readonly tagExpression?: string
}): Envelope => ({
  hook: {
    id: input.id,
    sourceReference: SUPPORT_SOURCE_REFERENCE,
    ...(input.name === undefined ? {} : { name: input.name }),
    ...(input.tagExpression === undefined ? {} : { tagExpression: input.tagExpression }),
  },
})

export const parameterTypeEnvelope = (input: {
  readonly id: string
  readonly name: string
  readonly regularExpressions: ReadonlyArray<string>
  readonly preferForRegexpMatch: boolean
  readonly useForSnippets: boolean
}): Envelope => ({
  parameterType: {
    id: input.id,
    name: input.name,
    regularExpressions: [...input.regularExpressions],
    preferForRegularExpressionMatch: input.preferForRegexpMatch,
    useForSnippets: input.useForSnippets,
    sourceReference: SUPPORT_SOURCE_REFERENCE,
  },
})

export const testCaseEnvelope = (input: {
  readonly id: string
  readonly pickleId: string
  readonly testRunStartedId: string
  readonly testSteps: ReadonlyArray<TestStep>
}): Envelope => ({
  testCase: {
    id: input.id,
    pickleId: input.pickleId,
    testRunStartedId: input.testRunStartedId,
    testSteps: [...input.testSteps],
  },
})

/** A test step that runs a matched pickle step (omit empty match lists for an undefined step). */
export const pickleTestStep = (input: {
  readonly id: string
  readonly pickleStepId: string
  readonly stepDefinitionIds: ReadonlyArray<string>
  readonly stepMatchArgumentsLists: ReadonlyArray<StepMatchArgumentsList>
}): TestStep => ({
  id: input.id,
  pickleStepId: input.pickleStepId,
  stepDefinitionIds: [...input.stepDefinitionIds],
  stepMatchArgumentsLists: input.stepMatchArgumentsLists.map((list) => ({
    stepMatchArguments: [...list.stepMatchArguments],
  })),
})

export const testRunFinished = (input: {
  readonly testRunStartedId: string
  readonly success: boolean
}): Envelope => ({
  testRunFinished: {
    testRunStartedId: input.testRunStartedId,
    timestamp: ZERO_TIMESTAMP,
    success: input.success,
  },
})

export const testCaseStarted = (input: {
  readonly id: string
  readonly testCaseId: string
  readonly attempt: number
}): Envelope => ({
  testCaseStarted: {
    id: input.id,
    testCaseId: input.testCaseId,
    attempt: input.attempt,
    timestamp: ZERO_TIMESTAMP,
  },
})

export const testCaseFinished = (input: {
  readonly testCaseStartedId: string
  readonly willBeRetried: boolean
}): Envelope => ({
  testCaseFinished: {
    testCaseStartedId: input.testCaseStartedId,
    timestamp: ZERO_TIMESTAMP,
    willBeRetried: input.willBeRetried,
  },
})

export const testStepStarted = (input: {
  readonly testCaseStartedId: string
  readonly testStepId: string
}): Envelope => ({
  testStepStarted: {
    testCaseStartedId: input.testCaseStartedId,
    testStepId: input.testStepId,
    timestamp: ZERO_TIMESTAMP,
  },
})

export const testStepFinished = (input: {
  readonly testCaseStartedId: string
  readonly testStepId: string
  readonly testStepResult: TestStepResult
}): Envelope => ({
  testStepFinished: {
    testCaseStartedId: input.testCaseStartedId,
    testStepId: input.testStepId,
    testStepResult: input.testStepResult,
    timestamp: ZERO_TIMESTAMP,
  },
})

export const attachmentEnvelope = (input: {
  readonly testCaseStartedId: string
  readonly testStepId: string
  readonly body: string
  readonly mediaType: string
  readonly contentEncoding: AttachmentContentEncoding
  readonly fileName?: string
}): Envelope => {
  const attachment: Attachment = {
    testCaseStartedId: input.testCaseStartedId,
    testStepId: input.testStepId,
    body: input.body,
    mediaType: input.mediaType,
    contentEncoding: input.contentEncoding,
    timestamp: ZERO_TIMESTAMP,
    ...(input.fileName === undefined ? {} : { fileName: input.fileName }),
  }
  return { attachment }
}

// ── result mapping ───────────────────────────────────────────────────────

const result = (status: TestStepResultStatus, extra?: Partial<TestStepResult>): TestStepResult => ({
  status,
  duration: ZERO_DURATION,
  ...extra,
})

export const passedResult = (): TestStepResult => result(Status.PASSED)
export const skippedResult = (): TestStepResult => result(Status.SKIPPED)
export const pendingResult = (): TestStepResult => result(Status.PENDING)
export const undefinedResult = (): TestStepResult => result(Status.UNDEFINED)

export const ambiguousResult = (message: string): TestStepResult =>
  result(Status.AMBIGUOUS, {
    message,
    exception: { type: "AmbiguousError", message },
  })

export const failedResult = (error: { readonly type: string; readonly message: string; readonly stackTrace?: string }): TestStepResult => {
  const exception: Exception = {
    type: error.type,
    message: error.message,
    ...(error.stackTrace === undefined ? {} : { stackTrace: error.stackTrace }),
  }
  return result(Status.FAILED, { message: error.stackTrace ?? error.message, exception })
}

/**
 * The run succeeds only when every observed step status is one a strict
 * Cucumber run tolerates. UNDEFINED/AMBIGUOUS/PENDING/FAILED all fail the run.
 */
export const testRunSuccess = (statuses: ReadonlyArray<TestStepResultStatus>): boolean =>
  statuses.every((status) => status === Status.PASSED || status === Status.SKIPPED)
