/**
 * Recorded Claude Agent SDK transcript fixtures for the D3 proofs — the
 * "recorded harness transcripts as fixtures" the lane's replay harness consumes.
 * Each is an ordered `ClaudeEvent` sequence; the proofs lower it and assert on the
 * reconstructed L1 records and their `foldTurn` state.
 */

import claudeBasicTurn from "../fixtures/claude-basic-turn.json" with { type: "json" }
import claudeSubagent from "../fixtures/claude-subagent.json" with { type: "json" }
import claudeToolCall from "../fixtures/claude-tool-call.json" with { type: "json" }
import type { ClaudeEvent } from "./events.ts"

export interface ClaudeFixture {
  readonly name: string
  readonly description: string
  readonly events: ReadonlyArray<ClaudeEvent>
  readonly notes?: string
}

const asFixture = (value: unknown): ClaudeFixture => value as ClaudeFixture

export const claudeFixtures: ReadonlyArray<ClaudeFixture> = [
  asFixture(claudeBasicTurn),
  asFixture(claudeToolCall),
  asFixture(claudeSubagent)
]

export const claudeFixtureByName = (name: string): ClaudeFixture | undefined =>
  claudeFixtures.find((fixture) => fixture.name === name)
