/**
 * A faithful, Effect-free model of the Claude Agent SDK's `SDKMessage` stream —
 * the transcript the Claude adapter lowers into L1 records. It mirrors the SDK's
 * documented message envelope (`system` / `assistant` / `user` / `result`,
 * `parent_tool_use_id`, `session_id`, result `usage` + `total_cost_usd`) and the
 * Messages-API content blocks (`text`, `thinking`, `tool_use`, `tool_result`).
 *
 * This package models the shape rather than depending on `@anthropic-ai/claude-agent-sdk`
 * so the lowering stays pure and deterministic (the `harness.fixture-replay`
 * target) and the proofs run in CI with no API key. A live adapter maps a real
 * `SDKMessage` to a `ClaudeEvent` at the integration boundary — a near-identity
 * projection — and drives the same pure lowering.
 */

/** Messages-API content blocks that appear inside assistant/user SDK messages. */
export interface ClaudeTextBlock {
  readonly type: "text"
  readonly text: string
}
export interface ClaudeThinkingBlock {
  readonly type: "thinking"
  readonly thinking: string
}
export interface ClaudeToolUseBlock {
  readonly type: "tool_use"
  readonly id: string
  readonly name: string
  readonly input: unknown
}
export interface ClaudeToolResultBlock {
  readonly type: "tool_result"
  readonly tool_use_id: string
  readonly content: string | ReadonlyArray<ClaudeTextBlock | { readonly type: string; readonly [k: string]: unknown }>
  readonly is_error?: boolean
}
export type ClaudeContentBlock =
  | ClaudeTextBlock
  | ClaudeThinkingBlock
  | ClaudeToolUseBlock
  | ClaudeToolResultBlock

/** Token usage as reported on the SDK result message (Messages-API usage fields). */
export interface ClaudeUsage {
  readonly input_tokens?: number
  readonly output_tokens?: number
  readonly cache_creation_input_tokens?: number
  readonly cache_read_input_tokens?: number
}

/** `{ type: "system", subtype: "init", ... }` — session bootstrap. */
export interface ClaudeSystemInit {
  readonly type: "system"
  readonly subtype: "init"
  readonly session_id: string
  readonly model?: string
  readonly cwd?: string
  readonly tools?: ReadonlyArray<string>
  readonly permissionMode?: string
}

/** `{ type: "assistant", message, parent_tool_use_id, session_id }`. */
export interface ClaudeAssistant {
  readonly type: "assistant"
  readonly message: {
    readonly id: string
    readonly role: "assistant"
    readonly model?: string
    readonly content: ReadonlyArray<ClaudeContentBlock>
  }
  /** Non-null when this assistant turn is a subagent's output (Task tool child). */
  readonly parent_tool_use_id: string | null
  readonly session_id: string
}

/** `{ type: "user", message, parent_tool_use_id, session_id }` — carries tool results. */
export interface ClaudeUser {
  readonly type: "user"
  readonly message: {
    readonly role: "user"
    readonly content: ReadonlyArray<ClaudeContentBlock>
  }
  readonly parent_tool_use_id: string | null
  readonly session_id: string
}

/** `{ type: "result", ... }` — terminal, carrying usage + cost. */
export interface ClaudeResult {
  readonly type: "result"
  readonly subtype: "success" | "error_max_turns" | "error_during_execution"
  readonly session_id: string
  readonly total_cost_usd?: number
  readonly usage?: ClaudeUsage
  readonly num_turns?: number
  readonly duration_ms?: number
  readonly is_error?: boolean
}

export type ClaudeEvent = ClaudeSystemInit | ClaudeAssistant | ClaudeUser | ClaudeResult
