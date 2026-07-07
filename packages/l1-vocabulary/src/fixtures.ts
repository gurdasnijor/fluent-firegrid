/**
 * The initial L1 vocabulary fixture corpus (WP D1), seeded for D2's
 * fixture-replay harness. Each fixture is a named turn expressed as an ordered
 * L1 record sequence — the canonical vocabulary corpus. D2 pairs these with
 * recorded harness transcripts and asserts an adapter reconstructs the same L1
 * facts and the same {@link foldTurn} output.
 *
 * Fixtures are shipped as neutral JSON under `fixtures/` (diffable, language-
 * agnostic, replay-friendly) and re-exported here as a typed corpus. New
 * scenarios are added by dropping a JSON file and appending it to `l1Fixtures`.
 */

import basicMessageTurn from "../fixtures/basic-message-turn.json" with { type: "json" }
import nativePassthrough from "../fixtures/native-passthrough.json" with { type: "json" }
import planUpdate from "../fixtures/plan-update.json" with { type: "json" }
import subagentScoping from "../fixtures/subagent-scoping.json" with { type: "json" }
import toolCallLifecycle from "../fixtures/tool-call-lifecycle.json" with { type: "json" }
import usageExtension from "../fixtures/usage-extension.json" with { type: "json" }
import type { L1StreamRecord } from "./vocabulary.ts"

export interface L1FixtureDocument {
  /** The schema version the records were authored against. */
  readonly schemaVersion: number
  /** Stable fixture identifier. */
  readonly name: string
  /** One-line description of the scenario. */
  readonly description: string
  /** The ordered L1 record sequence for the turn. */
  readonly records: ReadonlyArray<L1StreamRecord>
  /** Optional authoring notes explaining the expected fold behavior. */
  readonly notes?: string
}

const asFixture = (value: unknown): L1FixtureDocument => value as L1FixtureDocument

/** The initial fixture corpus, in a stable order. */
export const l1Fixtures: ReadonlyArray<L1FixtureDocument> = [
  asFixture(basicMessageTurn),
  asFixture(toolCallLifecycle),
  asFixture(planUpdate),
  asFixture(usageExtension),
  asFixture(subagentScoping),
  asFixture(nativePassthrough)
]

/** Look up a fixture by name. */
export const l1FixtureByName = (name: string): L1FixtureDocument | undefined =>
  l1Fixtures.find((fixture) => fixture.name === name)
