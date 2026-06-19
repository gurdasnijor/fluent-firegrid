import type { AssembledTestCase } from "@cucumber/core"
import { makeTestPlan } from "@cucumber/core"
import { generateMessages } from "@cucumber/gherkin"
import type { Envelope, GherkinDocument, Pickle, SourceMediaType } from "@cucumber/messages"
import { IdGenerator } from "@cucumber/messages"
import { buildSupportLibrary, type SupportModule } from "./support.ts"
import type { SourceInput } from "./types.ts"

/**
 * Deterministic Cucumber run assembly: parse features, build the support
 * library, mint the run id, and make the test plan. Given the same sources and
 * support module, this is a pure function of its inputs and produces identical
 * ids on every call (it threads one `IdGenerator.incrementing()` through the
 * whole sequence). That determinism is what lets the coordinator and the worker
 * assemble independently and still agree on every test-case / test-step id — no
 * shared mutable cache, no serialized closures.
 */

export interface AssembledRun {
  /** source / gherkinDocument / pickle envelopes, in parse order. */
  readonly discoveryEnvelopes: ReadonlyArray<Envelope>
  /** stepDefinition / hook / parameterType envelopes. */
  readonly supportEnvelopes: ReadonlyArray<Envelope>
  /** testCase envelopes, one per assembled test case. */
  readonly testCaseEnvelopes: ReadonlyArray<Envelope>
  /** the assembled test cases, with live `prepare()` closures. */
  readonly testCases: ReadonlyArray<AssembledTestCase>
  readonly testRunStartedId: string
  readonly byId: ReadonlyMap<string, AssembledTestCase>
  /**
   * The run's id generator, positioned after the test plan. The coordinator
   * continues it to mint `testCaseStarted` ids in a single deterministic
   * sequence (re-derived identically on replay).
   */
  readonly newId: IdGenerator.NewId
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
  const gherkinDocument = envelopes.find((envelope) => envelope.gherkinDocument !== undefined)?.gherkinDocument
  const pickles = envelopes.flatMap((envelope) => (envelope.pickle === undefined ? [] : [envelope.pickle]))
  return { envelopes, gherkinDocument, pickles }
}

export const assembleRun = (input: {
  readonly sources: ReadonlyArray<SourceInput>
  readonly support: SupportModule
}): AssembledRun => {
  const newId = IdGenerator.incrementing()

  const parsed = input.sources.map((source) => parseSource(source, newId))
  const discoveryEnvelopes = parsed.flatMap((source) => [...source.envelopes])

  const support = buildSupportLibrary(input.support, newId)
  const supportEnvelopes = support.toEnvelopes()

  const testRunStartedId = newId()

  const plans = parsed.flatMap((source) =>
    source.gherkinDocument === undefined
      ? []
      : [
        makeTestPlan(
          {
            testRunStartedId,
            gherkinDocument: source.gherkinDocument,
            pickles: source.pickles,
            supportCodeLibrary: support,
          },
          { newId },
        ),
      ],
  )

  const testCaseEnvelopes = plans.flatMap((plan) => [...plan.toEnvelopes()])
  const testCases = plans.flatMap((plan) => [...plan.testCases])
  const byId = new Map(testCases.map((testCase) => [testCase.id, testCase] as const))

  return {
    discoveryEnvelopes,
    supportEnvelopes,
    testCaseEnvelopes,
    testCases,
    testRunStartedId,
    byId,
    newId,
  }
}
