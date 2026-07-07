/**
 * The pure Claude → L1 lowering: a deterministic fold from `ClaudeEvent`s to L1
 * records (`@firegrid/l1-vocabulary`, interface I2). No I/O, no clock, no
 * entropy — replaying a transcript reproduces identical L1 facts (the
 * `harness.fixture-replay` target). Effect-free (two-zone rule).
 *
 * The two agent-ui defects WP D3 exists to fix are handled here:
 *  - **subagent scoping** — an assistant/user event with a non-null
 *    `parent_tool_use_id` is a subagent's output; its text/tools/results lower to
 *    `tool_call_update` content on the parent Task tool call (never a top-level
 *    `agent_message_chunk`), plus an ignorable `firegrid/subagent` attribution
 *    record emitted once per parent. So the base fold attributes subagent work to
 *    its parent without reading the extension.
 *  - **usage / cost facts** — the SDK `result` message's token usage and
 *    `total_cost_usd` lower to a `firegrid/usage` extension.
 */

import type {
  FiregridSubagent,
  FiregridUsage,
  L1StreamRecord,
  L1ToolCallContent,
  L1ToolKind
} from "@firegrid/l1-vocabulary"
import type { HarnessLowering } from "@firegrid/harness-adapter"

import type {
  ClaudeAssistant,
  ClaudeEvent,
  ClaudeResult,
  ClaudeToolResultBlock,
  ClaudeUser
} from "./events.ts"

export interface ClaudeLoweringState {
  readonly model: string | undefined
  readonly sessionId: string | undefined
  /** Parents for which a `firegrid/subagent` attribution has already been emitted. */
  readonly seenParents: ReadonlyArray<string>
}

const initial: ClaudeLoweringState = { model: undefined, sessionId: undefined, seenParents: [] }

const TOOL_KINDS: Record<string, L1ToolKind> = {
  Read: "read",
  Write: "edit",
  Edit: "edit",
  MultiEdit: "edit",
  NotebookEdit: "edit",
  Bash: "execute",
  Glob: "search",
  Grep: "search",
  WebFetch: "fetch",
  WebSearch: "search",
  Task: "other"
}

const toolKind = (name: string): L1ToolKind => TOOL_KINDS[name] ?? "other"

const textContent = (text: string): L1ToolCallContent => ({
  type: "content",
  content: { type: "text", text }
})

const toolResultText = (block: ClaudeToolResultBlock): string => {
  if (typeof block.content === "string") return block.content
  return block.content
    .map((item) => (item.type === "text" && typeof (item as { text?: unknown }).text === "string" ? (item as { text: string }).text : ""))
    .join("")
}

/** Emit a `firegrid/subagent` attribution for a newly-seen parent, updating state. */
const openSubagent = (
  state: ClaudeLoweringState,
  parent: string,
  model: string | undefined,
  records: Array<L1StreamRecord>
): ClaudeLoweringState => {
  if (state.seenParents.includes(parent)) return state
  const record: FiregridSubagent = {
    sessionUpdate: "firegrid/subagent",
    parentToolCallId: parent,
    subagentId: parent,
    ...(model !== undefined ? { model } : {})
  }
  records.push(record)
  return { ...state, seenParents: [...state.seenParents, parent] }
}

const lowerAssistant = (
  state: ClaudeLoweringState,
  event: ClaudeAssistant
): { readonly state: ClaudeLoweringState; readonly records: ReadonlyArray<L1StreamRecord> } => {
  const records: Array<L1StreamRecord> = []
  const model = event.message.model ?? state.model
  const parent = event.parent_tool_use_id
  let next: ClaudeLoweringState = { ...state, model }
  if (parent !== null) next = openSubagent(next, parent, model, records)

  for (const block of event.message.content) {
    if (block.type === "text") {
      if (parent === null) {
        records.push({ sessionUpdate: "agent_message_chunk", messageId: event.message.id, content: { type: "text", text: block.text } })
      } else {
        records.push({ sessionUpdate: "tool_call_update", toolCallId: parent, content: [textContent(block.text)] })
      }
    } else if (block.type === "thinking") {
      if (parent === null) {
        records.push({ sessionUpdate: "agent_thought_chunk", messageId: event.message.id, content: { type: "text", text: block.thinking } })
      } else {
        records.push({ sessionUpdate: "tool_call_update", toolCallId: parent, content: [textContent(block.thinking)] })
      }
    } else if (block.type === "tool_use") {
      if (parent === null) {
        records.push({
          sessionUpdate: "tool_call",
          toolCallId: block.id,
          title: block.name,
          kind: toolKind(block.name),
          status: "in_progress",
          rawInput: block.input
        })
      } else {
        // A subagent's own tool call — nested under the parent Task, not top-level.
        records.push({ sessionUpdate: "tool_call_update", toolCallId: parent, content: [textContent(`↳ ${block.name}`)] })
      }
    }
  }
  return { state: next, records }
}

const lowerUser = (
  state: ClaudeLoweringState,
  event: ClaudeUser
): { readonly state: ClaudeLoweringState; readonly records: ReadonlyArray<L1StreamRecord> } => {
  const records: Array<L1StreamRecord> = []
  const parent = event.parent_tool_use_id
  let next: ClaudeLoweringState = state
  if (parent !== null) next = openSubagent(next, parent, state.model, records)

  for (const block of event.message.content) {
    if (block.type !== "tool_result") continue
    if (parent === null) {
      records.push({
        sessionUpdate: "tool_call_update",
        toolCallId: block.tool_use_id,
        status: block.is_error === true ? "failed" : "completed",
        content: [textContent(toolResultText(block))]
      })
    } else {
      // A subagent's tool result folds under the parent Task tool call.
      records.push({ sessionUpdate: "tool_call_update", toolCallId: parent, content: [textContent(toolResultText(block))] })
    }
  }
  return { state: next, records }
}

const lowerResult = (
  state: ClaudeLoweringState,
  event: ClaudeResult
): ReadonlyArray<L1StreamRecord> => {
  const usage = event.usage
  const input = usage?.input_tokens
  const output = usage?.output_tokens
  const cacheCreate = usage?.cache_creation_input_tokens
  const cacheRead = usage?.cache_read_input_tokens
  const total = input !== undefined || output !== undefined
    ? (input ?? 0) + (output ?? 0)
    : undefined
  const record: FiregridUsage = {
    sessionUpdate: "firegrid/usage",
    ...(input !== undefined ? { inputTokens: input } : {}),
    ...(output !== undefined ? { outputTokens: output } : {}),
    ...(cacheCreate !== undefined ? { cacheCreationInputTokens: cacheCreate } : {}),
    ...(cacheRead !== undefined ? { cacheReadInputTokens: cacheRead } : {}),
    ...(total !== undefined ? { totalTokens: total } : {}),
    ...(event.total_cost_usd !== undefined ? { costUsd: event.total_cost_usd } : {}),
    ...(state.model !== undefined ? { model: state.model } : {})
  }
  return [record]
}

/** The Claude Agent SDK lowering (per WP D2's `HarnessLowering` contract). */
export const claudeLowering: HarnessLowering<ClaudeEvent, ClaudeLoweringState> = {
  initial,
  lower: (state, event) => {
    switch (event.type) {
      case "system":
        return {
          state: { ...state, model: event.model ?? state.model, sessionId: event.session_id },
          records: [{
            sessionUpdate: "firegrid/native",
            harness: "claude-agent-sdk",
            nativeType: "system_init",
            payload: {
              sessionId: event.session_id,
              ...(event.model !== undefined ? { model: event.model } : {}),
              ...(event.cwd !== undefined ? { cwd: event.cwd } : {}),
              ...(event.permissionMode !== undefined ? { permissionMode: event.permissionMode } : {})
            }
          }]
        }
      case "assistant":
        return lowerAssistant(state, event)
      case "user":
        return lowerUser(state, event)
      case "result":
        return { state, records: lowerResult(state, event) }
    }
  }
}

/** The terminal an SDK result maps to. */
export const terminalOfResult = (event: ClaudeResult): { readonly _tag: "completed" } | { readonly _tag: "failed"; readonly reason: string } =>
  event.subtype === "success"
    ? { _tag: "completed" }
    : { _tag: "failed", reason: event.subtype }
