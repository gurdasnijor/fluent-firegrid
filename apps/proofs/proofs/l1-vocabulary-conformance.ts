import {
  assistantText,
  classifyUpdate,
  decodeStream,
  decodeStreamRecord,
  FIREGRID_EXTENSION_SESSION_UPDATES,
  foldTurn,
  formatIssues,
  L1_BASE_SESSION_UPDATES,
  L1_SCHEMA_VERSION,
  l1FixtureByName,
  l1Fixtures,
  retainBaseUpdates,
  type L1StreamRecord
} from "@firegrid/l1-vocabulary"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"

import { proof } from "../src/Proof.ts"

class L1ConformanceError extends Data.TaggedError("L1ConformanceError")<{
  readonly issues: ReadonlyArray<string>
}> {}

const stableJson = (value: unknown): string => JSON.stringify(value)

const toolCallText = (records: ReadonlyArray<L1StreamRecord>, toolCallId: string): string => {
  const call = foldTurn(records).toolCalls.find((c) => c.toolCallId === toolCallId)
  if (call === undefined) return ""
  return call.content
    .map((item) => {
      const inner = item.content
      return inner !== undefined && typeof inner.text === "string" ? inner.text : ""
    })
    .join(" ")
}

/**
 * Run every conformance law over the fixture corpus and return the accumulated
 * failures. An empty array means the vocabulary schema, decoder, and base fold
 * satisfy the G2 contract for the whole corpus.
 */
const collectConformanceIssues = (): ReadonlyArray<string> => {
  const issues: Array<string> = []
  const fail = (message: string): void => {
    issues.push(message)
  }

  if (l1Fixtures.length === 0) fail("fixture corpus is empty")

  for (const fixture of l1Fixtures) {
    const at = `fixture ${fixture.name}`

    // Schema is versioned and every fixture declares the current version.
    if (fixture.schemaVersion !== L1_SCHEMA_VERSION) {
      fail(`${at}: schemaVersion ${fixture.schemaVersion} !== ${L1_SCHEMA_VERSION}`)
    }

    // Every record decodes against the schema.
    const decoded = decodeStream(fixture.records)
    if (!decoded.ok) {
      fail(`${at}: does not decode — ${formatIssues(decoded.issues)}`)
      continue
    }

    // Decoding is JSON round-trip stable (lossless: validation, not transformation).
    const reparsed = JSON.parse(JSON.stringify(fixture.records)) as ReadonlyArray<unknown>
    const redecoded = decodeStream(reparsed)
    if (!redecoded.ok || stableJson(redecoded.value) !== stableJson(decoded.value)) {
      fail(`${at}: JSON round-trip changed the decoded records`)
    }

    // Ignorable-by-default: the base fold is invariant to stripping every
    // extension and foreign record. This is the executable G2 rule.
    const foldedFull = foldTurn(fixture.records)
    const foldedBaseOnly = foldTurn(retainBaseUpdates(fixture.records))
    if (stableJson(foldedFull) !== stableJson(foldedBaseOnly)) {
      fail(`${at}: base fold changed when extensions were removed (extension is load-bearing)`)
    }
  }

  // Corpus coverage: every base variant and every declared extension appears.
  const seen = new Set<string>()
  for (const fixture of l1Fixtures) {
    for (const record of fixture.records) seen.add(record.sessionUpdate)
  }
  for (const variant of L1_BASE_SESSION_UPDATES) {
    if (!seen.has(variant)) fail(`corpus never exercises base variant ${variant}`)
  }
  for (const variant of FIREGRID_EXTENSION_SESSION_UPDATES) {
    if (!seen.has(variant)) fail(`corpus never exercises extension ${variant}`)
  }

  // Decoder actually validates: a structurally invalid known variant is rejected.
  const badToolCall = decodeStreamRecord({ sessionUpdate: "tool_call", title: "no id" })
  if (badToolCall.ok) fail("decoder accepted a tool_call missing toolCallId")
  const missingDiscriminant = decodeStreamRecord({ content: { type: "text", text: "x" } })
  if (missingDiscriminant.ok) fail("decoder accepted a record without a sessionUpdate discriminant")

  // Subagent scoping: subagent output folds under the parent tool call and never
  // into top-level turn text; the firegrid/subagent record is ignorable.
  const subagent = l1FixtureByName("subagent-scoping")
  if (subagent === undefined) {
    fail("missing fixture subagent-scoping")
  } else {
    const top = assistantText(foldTurn(subagent.records))
    if (top.includes("Subagent:")) {
      fail("subagent output leaked into top-level assistant text")
    }
    if (top !== "Delegating research to a subagent. The research is complete.") {
      fail(`unexpected top-level assistant text: ${JSON.stringify(top)}`)
    }
    const parentText = toolCallText(subagent.records, "call_task")
    if (!parentText.includes("Subagent: found 3 relevant files.") || !parentText.includes("Subagent: done.")) {
      fail("subagent output was not attributed to its parent tool call")
    }
  }

  // Classification of the forward-compat cases.
  const native = l1FixtureByName("native-passthrough")
  if (native !== undefined) {
    const foreign = native.records.find((r) => r.sessionUpdate === "available_commands_update")
    if (foreign === undefined || classifyUpdate(foreign) !== "foreign") {
      fail("a non-base ACP variant was not classified as foreign")
    }
    const nativeRecord = native.records.find((r) => r.sessionUpdate === "firegrid/native")
    if (nativeRecord === undefined || classifyUpdate(nativeRecord) !== "firegrid") {
      fail("firegrid/native was not classified as a firegrid extension")
    }
  }

  return issues
}

/**
 * `l1-vocabulary.schema-conformance` — proves the L1 observation vocabulary (I2)
 * satisfies the G2 contract across the seed fixture corpus: every fixture
 * decodes, is JSON round-trip stable, declares the current schema version, and
 * folds invariantly to stripping its `firegrid/` and foreign records
 * (ignorable-by-default); the base fold keeps subagent output under its parent
 * tool call; the decoder rejects malformed known variants; and the corpus covers
 * every base variant and every extension.
 */
export const l1VocabularyConformanceProof = proof("l1-vocabulary.schema-conformance")
  .describedAs(
    "Proves the L1 observation vocabulary schema, decoder, and base fold satisfy the G2 ACP-superset contract over the D1 fixture corpus."
  )
  .spec(({ property }) =>
    property("l1-vocabulary.schema-conformance")
      .workload(() =>
        Effect.gen(function*() {
          const issues = collectConformanceIssues()
          if (issues.length > 0) {
            return yield* new L1ConformanceError({ issues })
          }
          return { ok: true, fixtures: l1Fixtures.length, schemaVersion: L1_SCHEMA_VERSION } as const
        })
      )
      .verify(({ expect }) => [
        expect.workloadResult({ ok: true, fixtures: l1Fixtures.length, schemaVersion: L1_SCHEMA_VERSION })
      ])
  )
