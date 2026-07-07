/**
 * The base fold: the canonical projection of an L1 turn-stream into folded
 * message state. This is the fold every UI shares regardless of harness, and the
 * reference D2's fixture-replay harness compares adapter output against.
 *
 * The base fold consumes only {@link L1BaseUpdate} records. Every Firegrid
 * extension and every foreign record is skipped — this function is the
 * executable statement of the ignorable-by-default rule: its output is invariant
 * to the presence or absence of any non-base record.
 *
 * Folding rules (deterministic, no clock/entropy):
 *  - Message chunks fold by identity: consecutive `agent`/`user` chunks of the
 *    same role concatenate into one message; a role change, or a change of
 *    `messageId` (when both are present), starts a new message. Interleaved
 *    thoughts, tool calls, and plans do not split a message.
 *  - Thought chunks fold by the same identity rule into a separate thought list,
 *    never into message text.
 *  - `tool_call` opens an entry keyed by `toolCallId` (in first-seen order);
 *    `tool_call_update` merges into it (status/title/kind overwrite when present,
 *    content appends). An update for an unseen id opens a lenient entry.
 *  - `plan` replaces the current plan.
 */

import {
  isTextContent,
  type L1BaseUpdate,
  type L1ContentBlock,
  type L1PlanEntry,
  type L1StreamRecord,
  type L1ToolCallContent,
  type L1ToolCallStatus,
  type L1ToolKind
} from "./vocabulary.ts"
import { isBaseUpdate } from "./vocabulary.ts"

export interface FoldedTextBlock {
  readonly text: string
  readonly content: ReadonlyArray<L1ContentBlock>
}

export interface FoldedMessage extends FoldedTextBlock {
  readonly role: "user" | "assistant"
}

export interface FoldedToolCall {
  readonly toolCallId: string
  readonly title: string | undefined
  readonly kind: L1ToolKind | undefined
  readonly status: L1ToolCallStatus | undefined
  readonly content: ReadonlyArray<L1ToolCallContent>
}

export interface FoldedTurn {
  readonly messages: ReadonlyArray<FoldedMessage>
  readonly thoughts: ReadonlyArray<FoldedTextBlock>
  readonly toolCalls: ReadonlyArray<FoldedToolCall>
  readonly plan: ReadonlyArray<L1PlanEntry> | undefined
}

interface MessageDraft {
  role: "user" | "assistant"
  messageId: string | undefined
  text: string
  content: Array<L1ContentBlock>
}

interface ThoughtDraft {
  messageId: string | undefined
  text: string
  content: Array<L1ContentBlock>
}

interface ToolCallDraft {
  toolCallId: string
  title: string | undefined
  kind: L1ToolKind | undefined
  status: L1ToolCallStatus | undefined
  content: Array<L1ToolCallContent>
}

const continuesBlock = (
  previous: { readonly messageId: string | undefined } | undefined,
  incoming: string | undefined
): boolean => {
  if (previous === undefined) return false
  if (previous.messageId !== undefined && incoming !== undefined) {
    return previous.messageId === incoming
  }
  return true
}

const appendChunk = (
  draft: { text: string; content: Array<L1ContentBlock>; messageId: string | undefined },
  block: L1ContentBlock,
  messageId: string | undefined
): void => {
  draft.content.push(block)
  if (isTextContent(block)) draft.text += block.text
  if (draft.messageId === undefined && messageId !== undefined) draft.messageId = messageId
}

/**
 * Fold a turn-stream into folded message state, ignoring every non-base record.
 * Accepts any {@link L1StreamRecord} so it can be applied to a real stream that
 * carries extensions and forward-compat records.
 */
export const foldTurn = (records: ReadonlyArray<L1StreamRecord>): FoldedTurn => {
  const messages: Array<MessageDraft> = []
  const thoughts: Array<ThoughtDraft> = []
  const toolCalls: Array<ToolCallDraft> = []
  const toolCallsById = new Map<string, ToolCallDraft>()
  let plan: ReadonlyArray<L1PlanEntry> | undefined = undefined

  for (const record of records) {
    if (!isBaseUpdate(record)) continue
    foldBaseRecord(record, { messages, thoughts, toolCalls, toolCallsById }, (next) => {
      plan = next
    })
  }

  return {
    messages: messages.map((m) => ({ role: m.role, text: m.text, content: m.content })),
    thoughts: thoughts.map((t) => ({ text: t.text, content: t.content })),
    toolCalls: toolCalls.map((t) => ({
      toolCallId: t.toolCallId,
      title: t.title,
      kind: t.kind,
      status: t.status,
      content: t.content
    })),
    plan
  }
}

interface FoldState {
  readonly messages: Array<MessageDraft>
  readonly thoughts: Array<ThoughtDraft>
  readonly toolCalls: Array<ToolCallDraft>
  readonly toolCallsById: Map<string, ToolCallDraft>
}

const foldBaseRecord = (
  record: L1BaseUpdate,
  state: FoldState,
  setPlan: (entries: ReadonlyArray<L1PlanEntry>) => void
): void => {
  switch (record.sessionUpdate) {
    case "user_message_chunk":
    case "agent_message_chunk": {
      const role = record.sessionUpdate === "user_message_chunk" ? "user" : "assistant"
      const last = state.messages[state.messages.length - 1]
      if (last !== undefined && last.role === role && continuesBlock(last, record.messageId)) {
        appendChunk(last, record.content, record.messageId)
      } else {
        const draft: MessageDraft = { role, messageId: record.messageId, text: "", content: [] }
        appendChunk(draft, record.content, record.messageId)
        state.messages.push(draft)
      }
      return
    }
    case "agent_thought_chunk": {
      const last = state.thoughts[state.thoughts.length - 1]
      if (last !== undefined && continuesBlock(last, record.messageId)) {
        appendChunk(last, record.content, record.messageId)
      } else {
        const draft: ThoughtDraft = { messageId: record.messageId, text: "", content: [] }
        appendChunk(draft, record.content, record.messageId)
        state.thoughts.push(draft)
      }
      return
    }
    case "tool_call": {
      const existing = state.toolCallsById.get(record.toolCallId)
      const draft: ToolCallDraft = existing ?? {
        toolCallId: record.toolCallId,
        title: undefined,
        kind: undefined,
        status: undefined,
        content: []
      }
      draft.title = record.title
      if (record.kind !== undefined) draft.kind = record.kind
      if (record.status !== undefined) draft.status = record.status
      if (record.content !== undefined) draft.content.push(...record.content)
      if (existing === undefined) {
        state.toolCallsById.set(record.toolCallId, draft)
        state.toolCalls.push(draft)
      }
      return
    }
    case "tool_call_update": {
      const existing = state.toolCallsById.get(record.toolCallId)
      const draft: ToolCallDraft = existing ?? {
        toolCallId: record.toolCallId,
        title: undefined,
        kind: undefined,
        status: undefined,
        content: []
      }
      if (record.title !== undefined) draft.title = record.title
      if (record.kind !== undefined) draft.kind = record.kind
      if (record.status !== undefined) draft.status = record.status
      if (record.content !== undefined) draft.content.push(...record.content)
      if (existing === undefined) {
        state.toolCallsById.set(record.toolCallId, draft)
        state.toolCalls.push(draft)
      }
      return
    }
    case "plan": {
      setPlan(record.entries)
      return
    }
  }
}

/** Concatenated top-level assistant text (the base-fold "what the user reads"). */
export const assistantText = (turn: FoldedTurn): string =>
  turn.messages
    .filter((m) => m.role === "assistant")
    .map((m) => m.text)
    .join("")
