import {
  assistantText,
  classifyUpdate,
  decodeStream,
  foldTurn,
  type L1StreamRecord,
  retainBaseUpdates
} from "@firegrid/l1-vocabulary"
import {
  claudeAdapterLayer,
  claudeFixtureByName,
  claudeFixtures,
  claudeLowering
} from "@firegrid/claude-adapter"
import { HarnessAdapter, L1Sink, replay } from "@firegrid/harness-adapter"
import * as Data from "effect/Data"
import * as Effect from "effect/Effect"
import * as Layer from "effect/Layer"
import * as Ref from "effect/Ref"

import { proof } from "../src/Proof.ts"

class ClaudeProofError extends Data.TaggedError("ClaudeProofError")<{
  readonly issues: ReadonlyArray<string>
}> {}

const json = (value: unknown): string => JSON.stringify(value)
const sameJson = (a: unknown, b: unknown): boolean => json(a) === json(b)

const prompt = [
  { sessionUpdate: "user_message_chunk", messageId: "prompt", content: { type: "text", text: "go" } }
] as [L1StreamRecord]

const toolCallText = (records: ReadonlyArray<L1StreamRecord>, toolCallId: string): string => {
  const call = foldTurn(records).toolCalls.find((c) => c.toolCallId === toolCallId)
  if (call === undefined) return ""
  return call.content
    .map((item) => {
      const inner = item.content
      return inner !== undefined && typeof inner.text === "string" ? inner.text : ""
    })
    .join(" | ")
}

/** Drive a recorded Claude transcript through the D2 shell, capturing emitted L1 records. */
const driveCapture = Effect.fn("driveCapture")(function*(events: Parameters<typeof claudeAdapterLayer>[0]) {
  const captured = yield* Ref.make<ReadonlyArray<L1StreamRecord>>([])
  const sinkLayer = Layer.succeed(L1Sink, {
    emit: (record: L1StreamRecord) => Ref.update(captured, (records) => [...records, record])
  })
  const outcome = yield* Effect.gen(function*() {
    const adapter = yield* HarnessAdapter
    return yield* adapter.drive({ prompt })
  }).pipe(Effect.scoped, Effect.provide(claudeAdapterLayer(events)), Effect.provide(sinkLayer))
  return { captured: yield* Ref.get(captured), outcome }
})

/**
 * `harness.subagent-scoping` — subagent output (an event with a non-null
 * `parent_tool_use_id`) folds under its parent Task tool call and never into
 * top-level turn text; the `firegrid/subagent` attribution is ignorable (the base
 * fold is invariant to stripping it).
 */
export const claudeSubagentScopingProof = proof("harness.subagent-scoping")
  .describedAs(
    "Proves the Claude adapter attributes subagent output (parent_tool_use_id) to its parent tool call, never top-level turn text; firegrid/subagent is ignorable."
  )
  .spec(({ property }) =>
    property("harness.subagent-scoping")
      .workload(() =>
        Effect.gen(function*() {
          const issues: Array<string> = []
          const fail = (message: string): void => {
            issues.push(message)
          }

          const fixture = claudeFixtureByName("claude-subagent")
          if (fixture === undefined) {
            return yield* new ClaudeProofError({ issues: ["missing fixture claude-subagent"] })
          }
          const records = replay(claudeLowering, fixture.events)
          const top = assistantText(foldTurn(records))
          if (top.includes("Subagent:")) fail("subagent output leaked into top-level assistant text")
          if (top !== "Delegating research. The research is done.") {
            fail(`unexpected top-level assistant text: ${JSON.stringify(top)}`)
          }

          const parentText = toolCallText(records, "toolu_task")
          for (const fragment of ["Subagent: found 3 files.", "Subagent: file contents", "Research complete: 3 files."]) {
            if (!parentText.includes(fragment)) fail(`parent tool call missing subagent fragment: ${fragment}`)
          }
          const task = foldTurn(records).toolCalls.find((c) => c.toolCallId === "toolu_task")
          if (task?.status !== "completed") fail("parent Task tool call did not reach completed")

          const subagentExtensions = records.filter((r) => r.sessionUpdate === "firegrid/subagent")
          const subagentExtension = subagentExtensions[0]
          if (subagentExtensions.length !== 1 || subagentExtension === undefined) {
            fail(`expected exactly one firegrid/subagent, saw ${subagentExtensions.length}`)
          } else if (classifyUpdate(subagentExtension) !== "firegrid") {
            fail("firegrid/subagent not classified as a firegrid extension")
          }

          // Ignorable-by-default: the base fold is invariant to stripping firegrid/subagent.
          if (!sameJson(foldTurn(records), foldTurn(retainBaseUpdates(records)))) {
            fail("base fold changed when firegrid/subagent was stripped (extension is load-bearing)")
          }

          // The drive shell reconstructs the same records.
          const { captured } = yield* driveCapture(fixture.events)
          if (!sameJson(captured, records)) fail("drive shell diverged from the pure lowering")

          if (issues.length > 0) return yield* new ClaudeProofError({ issues })
          return { ok: true } as const
        })
      )
      .verify(({ expect }) => [expect.workloadResult({ ok: true })])
  )

/**
 * `harness.claude.fixture-replay` — the Claude lowering and the D2 `drive` shell
 * reconstruct valid, deterministic L1 records from recorded Claude transcripts; a
 * mutated transcript is detected as divergent.
 */
export const claudeFixtureReplayProof = proof("harness.claude.fixture-replay")
  .describedAs(
    "Proves the Claude adapter deterministically reconstructs valid L1 records from recorded transcripts, consistently between the pure lowering and the drive shell."
  )
  .spec(({ property }) =>
    property("harness.claude.fixture-replay")
      .workload(() =>
        Effect.gen(function*() {
          const issues: Array<string> = []
          const fail = (message: string): void => {
            issues.push(message)
          }
          if (claudeFixtures.length === 0) fail("Claude fixture corpus is empty")

          for (const fixture of claudeFixtures) {
            const records = replay(claudeLowering, fixture.events)
            const decoded = decodeStream(records)
            if (!decoded.ok) fail(`${fixture.name}: lowered records do not decode as L1`)
            if (!sameJson(replay(claudeLowering, fixture.events), records)) {
              fail(`${fixture.name}: lowering is nondeterministic`)
            }
            const { captured, outcome } = yield* driveCapture(fixture.events)
            if (!sameJson(captured, records)) fail(`${fixture.name}: drive shell diverged from lowering`)
            if (outcome.terminal._tag !== "completed") fail(`${fixture.name}: unexpected terminal ${outcome.terminal._tag}`)
            if (outcome.artifact.harness !== "claude-agent-sdk") {
              fail(`${fixture.name}: unexpected resume artifact harness ${outcome.artifact.harness}`)
            }
          }

          // Divergence must be detected: mutate a transcript and require inequality.
          const sample = claudeFixtureByName("claude-basic-turn")
          if (sample !== undefined) {
            const mutated = sample.events.map((event, index) =>
              index === sample.events.length - 1
                ? { ...event, usage: { input_tokens: 999 } }
                : event
            )
            if (sameJson(replay(claudeLowering, mutated), replay(claudeLowering, sample.events))) {
              fail("a mutated transcript was not detected as divergent")
            }
          }

          if (issues.length > 0) return yield* new ClaudeProofError({ issues })
          return { ok: true } as const
        })
      )
      .verify(({ expect }) => [expect.workloadResult({ ok: true })])
  )

/**
 * `harness.claude.usage-facts` — the Claude adapter surfaces token usage and cost
 * from the SDK result message as a `firegrid/usage` extension, ignorable-by-default.
 */
export const claudeUsageFactsProof = proof("harness.claude.usage-facts")
  .describedAs(
    "Proves the Claude adapter surfaces token/cost facts as an ignorable firegrid/usage L1 extension."
  )
  .spec(({ property }) =>
    property("harness.claude.usage-facts")
      .workload(() =>
        Effect.gen(function*() {
          const issues: Array<string> = []
          const fail = (message: string): void => {
            issues.push(message)
          }

          const basic = claudeFixtureByName("claude-basic-turn")
          if (basic === undefined) {
            return yield* new ClaudeProofError({ issues: ["missing fixture claude-basic-turn"] })
          }
          const records = replay(claudeLowering, basic.events)
          const usage = records.find((r) => r.sessionUpdate === "firegrid/usage") as
            | { readonly inputTokens?: number; readonly outputTokens?: number; readonly totalTokens?: number; readonly costUsd?: number; readonly model?: string }
            | undefined
          if (usage === undefined) {
            fail("no firegrid/usage record emitted")
          } else {
            if (usage.inputTokens !== 100 || usage.outputTokens !== 25) fail("usage token counts wrong")
            if (usage.totalTokens !== 125) fail("usage totalTokens wrong")
            if (usage.costUsd !== 0.0021) fail("usage costUsd wrong")
            if (usage.model !== "claude-opus-4-8") fail("usage model wrong")
          }
          const usageRecord = records.find((r) => r.sessionUpdate === "firegrid/usage")
          if (usageRecord !== undefined && classifyUpdate(usageRecord) !== "firegrid") {
            fail("firegrid/usage not classified as a firegrid extension")
          }

          // Ignorable-by-default: stripping usage does not change the base fold.
          if (!sameJson(foldTurn(records), foldTurn(retainBaseUpdates(records)))) {
            fail("base fold changed when firegrid/usage was stripped")
          }
          if (assistantText(foldTurn(records)) !== "Here is the summary.") {
            fail("usage lowering perturbed the folded assistant text")
          }

          if (issues.length > 0) return yield* new ClaudeProofError({ issues })
          return { ok: true } as const
        })
      )
      .verify(({ expect }) => [expect.workloadResult({ ok: true })])
  )
