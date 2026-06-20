import { generateMessages } from "@cucumber/gherkin"
import type { Envelope, GherkinDocument, Pickle, PickleStep, SourceMediaType, TestStep } from "@cucumber/messages"
import { IdGenerator } from "@cucumber/messages"
import { hookEnvelope, parameterTypeEnvelope, pickleTestStep, stepDefinitionEnvelope, testCaseEnvelope } from "./messages.ts"
import type { HostMatch, StepHost } from "./step-host.ts"
import type { InvokeRequest, PreparedScenario, PreparedStep, SourceInput } from "./types.ts"

/**
 * Deterministic assembly: parse features, emit the support-code discovery
 * envelopes the host describes, ask the host to match each pickle step, and mint
 * the test-case/test-step ids — cucumber-js's `assembleTestCases`. The runner
 * holds no matching logic; it delegates to the `StepHost`. Pure given
 * `(sources, host)`; the resulting `PreparedScenario[]` is fully serializable.
 */

export interface AssembledRun {
  readonly discoveryEnvelopes: ReadonlyArray<Envelope>
  readonly supportEnvelopes: ReadonlyArray<Envelope>
  readonly testCaseEnvelopes: ReadonlyArray<Envelope>
  readonly testRunStartedId: string
  readonly scenarios: ReadonlyArray<PreparedScenario>
}

interface ParsedSource {
  readonly envelopes: ReadonlyArray<Envelope>
  readonly gherkinDocument: GherkinDocument | undefined
  readonly pickles: ReadonlyArray<Pickle>
}

const parseSource = (source: SourceInput, newId: IdGenerator.NewId): ParsedSource => {
  const envelopes = generateMessages(source.data, source.uri, source.mediaType as SourceMediaType, {
    newId,
    includeSource: true,
    includeGherkinDocument: true,
    includePickles: true,
  })
  return {
    envelopes,
    gherkinDocument: envelopes.find((envelope) => envelope.gherkinDocument !== undefined)?.gherkinDocument,
    pickles: envelopes.flatMap((envelope) => (envelope.pickle === undefined ? [] : [envelope.pickle])),
  }
}

const invokeRequest = (stepDefId: string, pickleStep: PickleStep): InvokeRequest => ({
  stepDefId,
  text: pickleStep.text,
  ...(pickleStep.argument?.docString === undefined ? {} : { docString: pickleStep.argument.docString.content }),
  ...(pickleStep.argument?.dataTable === undefined ? {} : { dataTable: pickleStep.argument.dataTable }),
})

const testStepFrom = (testStepId: string, pickleStep: PickleStep, match: HostMatch): TestStep => {
  switch (match._tag) {
    case "undefined":
      return pickleTestStep({ id: testStepId, pickleStepId: pickleStep.id, stepDefinitionIds: [], stepMatchArgumentsLists: [] })
    case "ambiguous":
      return pickleTestStep({ id: testStepId, pickleStepId: pickleStep.id, stepDefinitionIds: match.stepDefIds, stepMatchArgumentsLists: [] })
    case "defined":
      return pickleTestStep({
        id: testStepId,
        pickleStepId: pickleStep.id,
        stepDefinitionIds: [match.stepDefId],
        stepMatchArgumentsLists: [{ stepMatchArguments: match.arguments }],
      })
  }
}

const preparedStepFrom = (testStepId: string, pickleStep: PickleStep, match: HostMatch): PreparedStep => {
  switch (match._tag) {
    case "undefined":
      return { _tag: "undefined", testStepId }
    case "ambiguous":
      return { _tag: "ambiguous", testStepId, message: `Multiple step definitions match: ${match.stepDefIds.join(", ")}` }
    case "defined":
      return { _tag: "invoke", testStepId, request: invokeRequest(match.stepDefId, pickleStep) }
  }
}

export const assemble = (sources: ReadonlyArray<SourceInput>, host: StepHost): AssembledRun => {
  const newId = IdGenerator.incrementing()

  const parsed = sources.map((source) => parseSource(source, newId))
  const discoveryEnvelopes = parsed.flatMap((source) => [...source.envelopes])

  const descriptor = host.describe()
  const supportEnvelopes: ReadonlyArray<Envelope> = [
    ...descriptor.stepDefinitions.map(stepDefinitionEnvelope),
    ...descriptor.beforeHooks.map(hookEnvelope),
    ...descriptor.afterHooks.map(hookEnvelope),
    ...descriptor.parameterTypes.map(parameterTypeEnvelope),
  ]

  const testRunStartedId = newId()

  // Match each pickle step once; derive both the test-step envelope and the prepared step.
  const assembledCases = parsed.flatMap((source) =>
    source.pickles.map((pickle) => {
      const steps = pickle.steps.map((pickleStep) => {
        const match = host.match(pickleStep.text)
        return { pickleStep, testStepId: newId(), match }
      })
      const testCaseId = newId()
      return { pickle, testCaseId, steps }
    }),
  )

  const testCaseEnvelopes = assembledCases.map(({ pickle, steps, testCaseId }) =>
    testCaseEnvelope({
      id: testCaseId,
      pickleId: pickle.id,
      testRunStartedId,
      testSteps: steps.map(({ match, pickleStep, testStepId }) => testStepFrom(testStepId, pickleStep, match)),
    }),
  )

  const scenarios = assembledCases.map(({ pickle, steps, testCaseId }): PreparedScenario => ({
    testCaseId,
    testCaseStartedId: newId(),
    scenarioId: pickle.id,
    tags: pickle.tags.map((tag) => tag.name),
    steps: steps.map(({ match, pickleStep, testStepId }) => preparedStepFrom(testStepId, pickleStep, match)),
  }))

  return { discoveryEnvelopes, supportEnvelopes, testCaseEnvelopes, testRunStartedId, scenarios }
}
