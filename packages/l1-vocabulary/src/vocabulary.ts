/**
 * L1 observation vocabulary — the harness-agnostic fact vocabulary a managed
 * session appends to its turn stream, folded identically by every UI regardless
 * of which harness produced it (cross-lane interface I2 in the managed-sessions
 * execution ledger).
 *
 * Decision G2 (2026-07-06, managed-sessions SDD §MS-C6): the vocabulary is an
 * **ACP `session/update` superset**. The base variants mirror ACP
 * `session/update` exactly (message chunks, thought chunks, `tool_call` +
 * `tool_call_update`, `plan`); Firegrid extensions are namespaced under
 * `firegrid/` and additive. Every extension is *ignorable-by-default*: a
 * consumer that does not understand it MUST skip it, and the base fold's
 * correctness never depends on it. The schema is versioned; unrecognized
 * `sessionUpdate` values are preserved verbatim and ignored by the base fold so
 * new variants never break an old fold.
 *
 * These are Effect-free, adapter-facing data types. Per the repository two-zone
 * rule the harness-adapter edge is TypeScript and its public data types carry no
 * Effect dependency; Effect appears only in the proof harness that exercises
 * them.
 *
 * The turn-stream envelope (record address, sequence, terminal marker) is owned
 * by interface I1 (WP B1's `DurableLog`/Turn binding), not by this module. This
 * vocabulary describes the *payload* carried inside each turn-stream record.
 */

/**
 * Schema version for the L1 observation vocabulary. Bumped only when a base
 * variant is added, removed, or given breaking semantics — never for a new
 * additive `firegrid/` extension (extensions are ignorable-by-default, so they
 * are backward-compatible without a version bump).
 */
export const L1_SCHEMA_VERSION = 1

export type L1SchemaVersion = typeof L1_SCHEMA_VERSION

/** Namespace prefix that marks a `sessionUpdate` value as a Firegrid extension. */
export const FIREGRID_EXTENSION_PREFIX = "firegrid/"

// ---------------------------------------------------------------------------
// Content blocks (ACP / MCP-aligned; text is interpreted, everything else is
// preserved opaquely so the vocabulary never loses harness evidence).
// ---------------------------------------------------------------------------

/**
 * A single content block. `type: "text"` carries readable `text`; other block
 * types (`image`, `audio`, `resource_link`, `resource`, or future kinds) are
 * preserved verbatim. Only text blocks contribute to folded message/thought
 * text.
 */
export interface L1ContentBlock {
  readonly type: string
  readonly text?: string
  readonly [key: string]: unknown
}

export const isTextContent = (
  block: L1ContentBlock
): block is L1ContentBlock & { readonly type: "text"; readonly text: string } =>
  block.type === "text" && typeof block.text === "string"

// ---------------------------------------------------------------------------
// Base vocabulary — faithful to ACP `session/update`.
// ---------------------------------------------------------------------------

/** Shared body of the streaming chunk variants. */
export interface L1ChunkBody {
  /** The streamed content block. */
  readonly content: L1ContentBlock
  /**
   * Optional message identity. All chunks sharing a `messageId` belong to one
   * message; a change indicates a new message has started (ACP semantics).
   */
  readonly messageId?: string
  readonly _meta?: Readonly<Record<string, unknown>>
}

export interface L1UserMessageChunk extends L1ChunkBody {
  readonly sessionUpdate: "user_message_chunk"
}

export interface L1AgentMessageChunk extends L1ChunkBody {
  readonly sessionUpdate: "agent_message_chunk"
}

export interface L1AgentThoughtChunk extends L1ChunkBody {
  readonly sessionUpdate: "agent_thought_chunk"
}

export type L1ToolCallStatus = "pending" | "in_progress" | "completed" | "failed"

export type L1ToolKind =
  | "read"
  | "edit"
  | "delete"
  | "move"
  | "search"
  | "execute"
  | "think"
  | "fetch"
  | "switch_mode"
  | "other"

/**
 * Content produced by a tool call. `type: "content"` wraps a {@link
 * L1ContentBlock}; `diff` / `terminal` / future kinds are preserved opaquely.
 * Subagent output is carried here (on the parent tool call), which is how the
 * base fold attributes subagent work to its parent without depending on the
 * `firegrid/subagent` extension.
 */
export interface L1ToolCallContent {
  readonly type: string
  readonly content?: L1ContentBlock
  readonly [key: string]: unknown
}

export interface L1ToolCall {
  readonly sessionUpdate: "tool_call"
  readonly toolCallId: string
  readonly title: string
  readonly kind?: L1ToolKind
  readonly status?: L1ToolCallStatus
  readonly content?: ReadonlyArray<L1ToolCallContent>
  readonly locations?: ReadonlyArray<Readonly<Record<string, unknown>>>
  readonly rawInput?: unknown
  readonly rawOutput?: unknown
  readonly _meta?: Readonly<Record<string, unknown>>
}

export interface L1ToolCallUpdate {
  readonly sessionUpdate: "tool_call_update"
  readonly toolCallId: string
  readonly title?: string
  readonly kind?: L1ToolKind
  readonly status?: L1ToolCallStatus
  readonly content?: ReadonlyArray<L1ToolCallContent>
  readonly locations?: ReadonlyArray<Readonly<Record<string, unknown>>>
  readonly rawInput?: unknown
  readonly rawOutput?: unknown
  readonly _meta?: Readonly<Record<string, unknown>>
}

export type L1PlanEntryPriority = "high" | "medium" | "low"
export type L1PlanEntryStatus = "pending" | "in_progress" | "completed"

export interface L1PlanEntry {
  readonly content: string
  readonly priority: L1PlanEntryPriority
  readonly status: L1PlanEntryStatus
  readonly _meta?: Readonly<Record<string, unknown>>
}

export interface L1Plan {
  readonly sessionUpdate: "plan"
  readonly entries: ReadonlyArray<L1PlanEntry>
  readonly _meta?: Readonly<Record<string, unknown>>
}

/** The base vocabulary — every variant is a faithful ACP `session/update`. */
export type L1BaseUpdate =
  | L1UserMessageChunk
  | L1AgentMessageChunk
  | L1AgentThoughtChunk
  | L1ToolCall
  | L1ToolCallUpdate
  | L1Plan

export const L1_BASE_SESSION_UPDATES = [
  "user_message_chunk",
  "agent_message_chunk",
  "agent_thought_chunk",
  "tool_call",
  "tool_call_update",
  "plan"
] as const

export type L1BaseSessionUpdate = (typeof L1_BASE_SESSION_UPDATES)[number]

// ---------------------------------------------------------------------------
// Firegrid extensions — namespaced, additive, ignorable-by-default.
// ---------------------------------------------------------------------------

/**
 * `firegrid/usage` — token and cost accounting for a turn. ACP recently added a
 * native `usage_update`, but the G2 decision deliberately keeps usage as a
 * Firegrid extension: token/cost accounting is a Firegrid concern that must be
 * ignorable-by-default for the base fold, not load-bearing for it. All fields
 * are optional because harnesses report different subsets.
 */
export interface FiregridUsage {
  readonly sessionUpdate: "firegrid/usage"
  readonly inputTokens?: number
  readonly outputTokens?: number
  readonly cacheCreationInputTokens?: number
  readonly cacheReadInputTokens?: number
  readonly totalTokens?: number
  readonly costUsd?: number
  readonly model?: string
  readonly _meta?: Readonly<Record<string, unknown>>
}

/**
 * `firegrid/subagent` — parent-scoped attribution for subagent activity. This is
 * *enrichment only*: the subagent's output already folds under its parent tool
 * call (its chunks arrive as `tool_call_update` content on `parentToolCallId`),
 * so a base fold that ignores this record still attributes subagent work to the
 * parent and never interleaves it into top-level turn text. A subagent-aware UI
 * reads this record for richer rendering.
 */
export interface FiregridSubagent {
  readonly sessionUpdate: "firegrid/subagent"
  readonly parentToolCallId: string
  readonly subagentId?: string
  readonly label?: string
  readonly model?: string
  readonly _meta?: Readonly<Record<string, unknown>>
}

/**
 * `firegrid/native` — harness-specific passthrough, tagged with the emitting
 * `harness` id. Carries data with no ACP equivalent; always ignorable-by-default.
 */
export interface FiregridNative {
  readonly sessionUpdate: "firegrid/native"
  readonly harness: string
  readonly nativeType?: string
  readonly payload: unknown
  readonly _meta?: Readonly<Record<string, unknown>>
}

export type FiregridExtensionUpdate = FiregridUsage | FiregridSubagent | FiregridNative

export const FIREGRID_EXTENSION_SESSION_UPDATES = [
  "firegrid/usage",
  "firegrid/subagent",
  "firegrid/native"
] as const

export type FiregridExtensionSessionUpdate = (typeof FIREGRID_EXTENSION_SESSION_UPDATES)[number]

// ---------------------------------------------------------------------------
// Foreign / forward-compatible records and the stream record union.
// ---------------------------------------------------------------------------

/**
 * A record whose `sessionUpdate` this schema version does not recognize — a
 * future ACP variant, a future/unknown `firegrid/` extension, or another
 * harness's namespace. Preserved verbatim and ignored by the base fold, which
 * is what makes the schema forward-compatible.
 */
export interface L1ForeignUpdate {
  readonly sessionUpdate: string
  readonly [key: string]: unknown
}

/** A known, typed update: a base variant or a recognized Firegrid extension. */
export type L1Update = L1BaseUpdate | FiregridExtensionUpdate

/** Any record that can appear in a turn stream, including forward-compat ones. */
export type L1StreamRecord = L1Update | L1ForeignUpdate

export type L1UpdateClass = "base" | "firegrid" | "foreign"

const baseSet: ReadonlySet<string> = new Set(L1_BASE_SESSION_UPDATES)
const firegridSet: ReadonlySet<string> = new Set(FIREGRID_EXTENSION_SESSION_UPDATES)

/** Classify a record without asserting its internal structure is valid. */
export const classifyUpdate = (record: L1StreamRecord): L1UpdateClass => {
  if (baseSet.has(record.sessionUpdate)) return "base"
  if (firegridSet.has(record.sessionUpdate)) return "firegrid"
  return "foreign"
}

export const isBaseUpdate = (record: L1StreamRecord): record is L1BaseUpdate =>
  baseSet.has(record.sessionUpdate)

export const isFiregridExtension = (
  record: L1StreamRecord
): record is FiregridExtensionUpdate => firegridSet.has(record.sessionUpdate)

export const isForeignUpdate = (record: L1StreamRecord): record is L1ForeignUpdate =>
  !baseSet.has(record.sessionUpdate) && !firegridSet.has(record.sessionUpdate)

/**
 * Whether the base fold ignores this record. True for every extension and every
 * foreign record — the operational statement of "ignorable-by-default".
 */
export const isIgnorableByBaseFold = (record: L1StreamRecord): boolean =>
  !isBaseUpdate(record)

/** Keep only the base records — the input a "strip all extensions" test folds. */
export const retainBaseUpdates = (
  records: ReadonlyArray<L1StreamRecord>
): ReadonlyArray<L1BaseUpdate> => records.filter(isBaseUpdate)
