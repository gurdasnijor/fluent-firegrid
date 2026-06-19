import type { Envelope } from "@cucumber/messages"

/**
 * Cucumber Compatibility Kit comparison helpers (pure). The comparison mirrors
 * cucumber-js's own `cck_spec`: reorder run-hook envelopes to just after
 * `testRunStarted`, strip the keys that are environment- or id-specific, then
 * deep-compare the produced envelopes against the sample's expected `.ndjson`.
 *
 * Fixture loading (feature files, expected ndjson) lives with the tests, since
 * it reaches for the filesystem directly.
 */

// The keys cucumber-js excludes from the CCK comparison (ids, timestamps,
// sources, error text, snippets, meta) — they are normalized away.
const IGNORABLE_KEYS: ReadonlySet<string> = new Set([
  "meta",
  "uri",
  "line",
  "astNodeId",
  "astNodeIds",
  "hookId",
  "id",
  "pickleId",
  "pickleStepId",
  "stepDefinitionIds",
  "testRunStartedId",
  "testRunHookStartedId",
  "testCaseId",
  "testCaseStartedId",
  "testStepId",
  "nanos",
  "seconds",
  "message",
  "stackTrace",
  "language",
  "code",
])

/** Recursively drop the keys the CCK comparison ignores. */
export const stripIgnorable = (value: unknown): unknown => {
  if (Array.isArray(value)) return value.map(stripIgnorable)
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([key]) => !IGNORABLE_KEYS.has(key))
        .map(([key, child]) => [key, stripIgnorable(child)]),
    )
  }
  return value
}

/** Move run-hook envelopes that precede the first scenario to just after `testRunStarted`. */
export const reorderEnvelopes = (envelopes: ReadonlyArray<Envelope>): ReadonlyArray<Envelope> => {
  let testRunStartedEnvelope: Envelope | undefined
  let sawTestCaseStarted = false
  const result: Array<Envelope> = []
  const moveAfterTestRunStarted: Array<Envelope> = []

  envelopes.forEach((envelope) => {
    if (envelope.testRunStarted !== undefined) testRunStartedEnvelope = envelope
    if (envelope.testCaseStarted !== undefined) sawTestCaseStarted = true
    if ((envelope.testRunHookStarted !== undefined || envelope.testRunHookFinished !== undefined) && !sawTestCaseStarted) {
      moveAfterTestRunStarted.push(envelope)
    } else {
      result.push(envelope)
    }
  })

  if (testRunStartedEnvelope !== undefined && moveAfterTestRunStarted.length > 0) {
    result.splice(result.indexOf(testRunStartedEnvelope) + 1, 0, ...moveAfterTestRunStarted)
  }
  return result
}

/**
 * Normalize a produced envelope stream for comparison against expected CCK
 * output. Each envelope is JSON round-tripped first (dropping `undefined`
 * fields, exactly as cucumber-js's own gate does) before reordering + stripping.
 */
export const normalizeEnvelopes = (envelopes: ReadonlyArray<Envelope>): ReadonlyArray<unknown> =>
  reorderEnvelopes(envelopes.map((envelope) => JSON.parse(JSON.stringify(envelope)) as Envelope)).map(stripIgnorable)
