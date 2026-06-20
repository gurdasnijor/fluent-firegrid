import { generateMessages } from "@cucumber/gherkin"
import type { Envelope, GherkinDocument, Pickle, PickleStep, SourceMediaType, TestStep } from "@cucumber/messages"
import { IdGenerator } from "@cucumber/messages"
import { matchStep } from "./matcher.ts"
import { hookEnvelope, parameterTypeEnvelope, pickleTestStep, stepDefinitionEnvelope, testCaseEnvelope } from "./messages.ts"
import type { SupportBundle } from "./support.ts"
import type { PlannedScenario, PlannedStep, SourceInput, StepKind } from "./types.ts"

/**
 * Deterministic, runner-side assembly: parse features, emit support-code
 * envelopes, match each pickle step against the support bundle, and mint the
 * test-case/test-step ids — cucumber-js's `assembleTestCases`, reimplemented on
 * `@cucumber/cucumber-expressions` instead of `@cucumber/core`'s test plan.
 *
 * Pure given `(sources, support)` and owns **all** Cucumber ids/envelopes; the
 * resulting `PlannedScenario[]` is fully serializable (steps carry the matched
 * definition index, not a closure), so the runner drives the `world` host over
 * the durable boundary without ever holding step code.
 */

export interface AssembledRun {
  readonly discoveryEnvelopes: ReadonlyArray<Envelope>
  readonly supportEnvelopes: ReadonlyArray<Envelope>
  readonly testCaseEnvelopes: ReadonlyArray<Envelope>
  readonly testRunStartedId: string
  readonly scenarios: ReadonlyArray<PlannedScenario>
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

const stepKind = (bundle: SupportBundle, pickleStep: PickleStep): StepKind => {
  const match = matchStep(bundle, pickleStep.text)
  switch (match._tag) {
    case "undefined":
      return { _tag: "undefined" }
    case "ambiguous":
      return { _tag: "ambiguous", message: `Multiple step definitions match: ${match.expressions.join(", ")}` }
    case "defined":
      return {
        _tag: "prepared",
        invocation: {
          stepIndex: match.index,
          text: pickleStep.text,
          ...(pickleStep.argument?.docString === undefined ? {} : { docString: pickleStep.argument.docString.content }),
          ...(pickleStep.argument?.dataTable === undefined ? {} : { dataTable: pickleStep.argument.dataTable }),
        },
      }
  }
}

/** Build the test step (envelope shape) for one pickle step from its match. */
const testStepFor = (bundle: SupportBundle, pickleStep: PickleStep, stepDefIds: ReadonlyArray<string>, testStepId: string): TestStep => {
  const match = matchStep(bundle, pickleStep.text)
  switch (match._tag) {
    case "undefined":
      return pickleTestStep({ id: testStepId, pickleStepId: pickleStep.id, stepDefinitionIds: [], stepMatchArgumentsLists: [] })
    case "ambiguous":
      return pickleTestStep({
        id: testStepId,
        pickleStepId: pickleStep.id,
        stepDefinitionIds: match.indices.map((index) => stepDefIds[index]!),
        stepMatchArgumentsLists: [],
      })
    case "defined":
      return pickleTestStep({
        id: testStepId,
        pickleStepId: pickleStep.id,
        stepDefinitionIds: [stepDefIds[match.index]!],
        stepMatchArgumentsLists: [{ stepMatchArguments: match.arguments }],
      })
  }
}

export const assembleRun = (input: {
  readonly sources: ReadonlyArray<SourceInput>
  readonly support: SupportBundle
}): AssembledRun => {
  const newId = IdGenerator.incrementing()
  const { support } = input

  const parsed = input.sources.map((source) => parseSource(source, newId))
  const discoveryEnvelopes = parsed.flatMap((source) => [...source.envelopes])

  // Support-code ids/envelopes, in registration order (step definitions first,
  // matching cucumber's discovery order before `testRunStarted`).
  const stepDefIds = support.steps.map(() => newId())
  const supportEnvelopes: ReadonlyArray<Envelope> = [
    ...support.steps.map((step, index) =>
      stepDefinitionEnvelope({ id: stepDefIds[index]!, source: step.expression.source, type: step.patternType }),
    ),
    ...support.beforeHooks.map((hook) => hookEnvelope({ id: newId(), ...(hook.name === undefined ? {} : { name: hook.name }), ...(hook.tags === undefined ? {} : { tagExpression: hook.tags }) })),
    ...support.afterHooks.map((hook) => hookEnvelope({ id: newId(), ...(hook.name === undefined ? {} : { name: hook.name }), ...(hook.tags === undefined ? {} : { tagExpression: hook.tags }) })),
    ...support.parameterTypes.map((parameterType) => parameterTypeEnvelope({ id: newId(), ...parameterType })),
  ]

  const testRunStartedId = newId()

  const assembledCases = parsed.flatMap((source) =>
    source.pickles.map((pickle) => {
      const testSteps = pickle.steps.map((pickleStep) => testStepFor(support, pickleStep, stepDefIds, newId()))
      const testCaseId = newId()
      return { pickle, testCaseId, testSteps }
    }),
  )

  const testCaseEnvelopes = assembledCases.map(({ pickle, testCaseId, testSteps }) =>
    testCaseEnvelope({ id: testCaseId, pickleId: pickle.id, testRunStartedId, testSteps }),
  )

  const scenarios = assembledCases.map(({ pickle, testCaseId, testSteps }): PlannedScenario => {
    const steps: ReadonlyArray<PlannedStep> = pickle.steps.map((pickleStep, index) => ({
      testStepId: testSteps[index]!.id,
      always: false,
      kind: stepKind(support, pickleStep),
    }))
    return {
      testCaseId,
      testCaseStartedId: newId(),
      scenarioId: pickle.id,
      tags: pickle.tags.map((tag) => tag.name),
      steps,
    }
  })

  return { discoveryEnvelopes, supportEnvelopes, testCaseEnvelopes, testRunStartedId, scenarios }
}
