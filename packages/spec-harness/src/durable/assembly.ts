import { makeTestPlan } from "@cucumber/core"
import type { AssembledTestStep } from "@cucumber/core"
import { generateMessages } from "@cucumber/gherkin"
import type { Envelope, GherkinDocument, Pickle, SourceMediaType } from "@cucumber/messages"
import { IdGenerator } from "@cucumber/messages"
import { buildSupportLibrary } from "./support.ts"
import type { PlannedScenario, PlannedStep, SourceInput, StepKind } from "./types.ts"

/**
 * Deterministic, runner-side assembly: parse features, build the support
 * library, mint ids, and `makeTestPlan`. This is pure given `(sources,
 * supportName)` and owns **all** Cucumber ids/envelopes; the resulting
 * `PlannedScenario[]` is fully serializable, so the runner can drive the `world`
 * host over the durable boundary without ever holding a step closure.
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

const stepKind = (step: AssembledTestStep, pickle: Pickle): StepKind => {
  const prepared = step.prepare()
  if (prepared.type === "undefined") return { _tag: "undefined" }
  if (prepared.type === "ambiguous") {
    return {
      _tag: "ambiguous",
      message: `Multiple step definitions match: ${prepared.matches.map((m) => String(m.expression.raw)).join(", ")}`,
    }
  }
  // pickleStepId is absent for hook steps; those carry no re-matchable text yet
  // (hooks are out of scope for the current samples — TODO when hooks land).
  const pickleStepId = step.toMessage().pickleStepId
  const text = pickle.steps.find((s) => s.id === pickleStepId)?.text ?? ""
  return {
    _tag: "prepared",
    invocation: {
      text,
      argValues: prepared.args.map((arg) => arg.getValue(undefined)),
      ...(prepared.docString === undefined ? {} : { docString: prepared.docString.content }),
      ...(prepared.dataTable === undefined ? {} : { dataTable: prepared.dataTable }),
    },
  }
}

export const assembleRun = (input: {
  readonly sources: ReadonlyArray<SourceInput>
  readonly supportName: string
}): AssembledRun => {
  const newId = IdGenerator.incrementing()

  const parsed = input.sources.map((source) => parseSource(source, newId))
  const discoveryEnvelopes = parsed.flatMap((source) => [...source.envelopes])

  const support = buildSupportLibrary(input.supportName, newId)
  const supportEnvelopes = support.toEnvelopes()

  const testRunStartedId = newId()

  const plans = parsed.flatMap((source) =>
    source.gherkinDocument === undefined ? [] : [{
      pickles: source.pickles,
      plan: makeTestPlan(
        { testRunStartedId, gherkinDocument: source.gherkinDocument, pickles: source.pickles, supportCodeLibrary: support },
        { newId },
      ),
    }],
  )

  const testCaseEnvelopes = plans.flatMap(({ plan }) => [...plan.toEnvelopes()])

  const scenarios = plans.flatMap(({ plan, pickles }) =>
    plan.testCases.flatMap((testCase): ReadonlyArray<PlannedScenario> => {
      const pickle = pickles.find((p) => p.id === testCase.pickleId)
      if (pickle === undefined) return []
      const steps: ReadonlyArray<PlannedStep> = testCase.testSteps.map((step) => ({
        testStepId: step.id,
        always: step.always,
        kind: stepKind(step, pickle),
      }))
      return [{
        testCaseId: testCase.id,
        testCaseStartedId: newId(),
        scenarioId: pickle.id,
        tags: pickle.tags.map((tag) => tag.name),
        steps,
      }]
    }),
  )

  return { discoveryEnvelopes, supportEnvelopes, testCaseEnvelopes, testRunStartedId, scenarios }
}
