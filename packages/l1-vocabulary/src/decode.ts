/**
 * Effect-free structural validation for the L1 observation vocabulary.
 *
 * Decoding is *validation, not transformation*: a decoded record is the input
 * preserved verbatim (all fields, including `_meta` and harness extras), only
 * having been checked to satisfy its variant's shape. This keeps L1 facts
 * lossless — the harness's evidence is never normalized away.
 *
 * Known base variants and recognized `firegrid/` extensions are validated
 * strictly. Any other `sessionUpdate` — a future ACP variant, an unknown
 * `firegrid/` extension, or a foreign namespace — is accepted as a
 * {@link L1ForeignUpdate} and preserved, per the ignorable-by-default rule.
 */

import {
  FIREGRID_EXTENSION_PREFIX,
  type FiregridExtensionUpdate,
  type L1PlanEntryPriority,
  type L1PlanEntryStatus,
  type L1StreamRecord,
  type L1ToolCallStatus,
  type L1ToolKind
} from "./vocabulary.ts"

export interface L1DecodeIssue {
  readonly path: string
  readonly message: string
}

export type L1Decoded<T> =
  | { readonly ok: true; readonly value: T }
  | { readonly ok: false; readonly issues: ReadonlyArray<L1DecodeIssue> }

const ok = <T>(value: T): L1Decoded<T> => ({ ok: true, value })
const err = (issues: ReadonlyArray<L1DecodeIssue>): L1Decoded<never> => ({ ok: false, issues })
const issue = (path: string, message: string): L1DecodeIssue => ({ path, message })

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)

const TOOL_STATUSES: ReadonlySet<string> = new Set<L1ToolCallStatus>([
  "pending",
  "in_progress",
  "completed",
  "failed"
])
const TOOL_KINDS: ReadonlySet<string> = new Set<L1ToolKind>([
  "read",
  "edit",
  "delete",
  "move",
  "search",
  "execute",
  "think",
  "fetch",
  "switch_mode",
  "other"
])
const PLAN_PRIORITIES: ReadonlySet<string> = new Set<L1PlanEntryPriority>(["high", "medium", "low"])
const PLAN_STATUSES: ReadonlySet<string> = new Set<L1PlanEntryStatus>([
  "pending",
  "in_progress",
  "completed"
])

const requireContentBlock = (
  value: unknown,
  path: string,
  issues: Array<L1DecodeIssue>
): void => {
  if (!isRecord(value)) {
    issues.push(issue(path, "expected a content block object"))
    return
  }
  if (typeof value["type"] !== "string") {
    issues.push(issue(`${path}.type`, "content block requires a string `type`"))
  }
}

const requireOptionalString = (
  record: Record<string, unknown>,
  key: string,
  path: string,
  issues: Array<L1DecodeIssue>
): void => {
  if (key in record && typeof record[key] !== "string") {
    issues.push(issue(`${path}.${key}`, `\`${key}\` must be a string when present`))
  }
}

const requireOptionalEnum = (
  record: Record<string, unknown>,
  key: string,
  members: ReadonlySet<string>,
  path: string,
  issues: Array<L1DecodeIssue>
): void => {
  if (key in record) {
    const raw = record[key]
    if (typeof raw !== "string" || !members.has(raw)) {
      issues.push(issue(`${path}.${key}`, `\`${key}\` must be one of ${[...members].join(", ")}`))
    }
  }
}

const validateChunk = (record: Record<string, unknown>, issues: Array<L1DecodeIssue>): void => {
  requireContentBlock(record["content"], "content", issues)
  requireOptionalString(record, "messageId", "", issues)
}

const validateToolCall = (
  record: Record<string, unknown>,
  requireTitle: boolean,
  issues: Array<L1DecodeIssue>
): void => {
  if (typeof record["toolCallId"] !== "string") {
    issues.push(issue("toolCallId", "`toolCallId` is required and must be a string"))
  }
  if (requireTitle) {
    if (typeof record["title"] !== "string") {
      issues.push(issue("title", "`title` is required on `tool_call` and must be a string"))
    }
  } else {
    requireOptionalString(record, "title", "", issues)
  }
  requireOptionalEnum(record, "status", TOOL_STATUSES, "", issues)
  requireOptionalEnum(record, "kind", TOOL_KINDS, "", issues)
  if ("content" in record) {
    const content = record["content"]
    if (!Array.isArray(content)) {
      issues.push(issue("content", "`content` must be an array of tool-call content items"))
    } else {
      content.forEach((item, index) => {
        if (!isRecord(item) || typeof item["type"] !== "string") {
          issues.push(issue(`content[${index}].type`, "tool-call content item requires a string `type`"))
        }
      })
    }
  }
}

const validatePlan = (record: Record<string, unknown>, issues: Array<L1DecodeIssue>): void => {
  const entries = record["entries"]
  if (!Array.isArray(entries)) {
    issues.push(issue("entries", "`plan` requires an `entries` array"))
    return
  }
  entries.forEach((entry: unknown, index) => {
    const path = `entries[${index}]`
    if (!isRecord(entry)) {
      issues.push(issue(path, "plan entry must be an object"))
      return
    }
    if (typeof entry["content"] !== "string") {
      issues.push(issue(`${path}.content`, "plan entry requires a string `content`"))
    }
    requireOptionalEnum(entry, "priority", PLAN_PRIORITIES, path, issues)
    requireOptionalEnum(entry, "status", PLAN_STATUSES, path, issues)
    if (!("priority" in entry)) issues.push(issue(`${path}.priority`, "plan entry requires `priority`"))
    if (!("status" in entry)) issues.push(issue(`${path}.status`, "plan entry requires `status`"))
  })
}

const validateFiregridExtension = (
  record: Record<string, unknown>,
  sessionUpdate: string,
  issues: Array<L1DecodeIssue>
): void => {
  switch (sessionUpdate) {
    case "firegrid/subagent": {
      if (typeof record["parentToolCallId"] !== "string") {
        issues.push(issue("parentToolCallId", "`firegrid/subagent` requires a string `parentToolCallId`"))
      }
      return
    }
    case "firegrid/native": {
      if (typeof record["harness"] !== "string") {
        issues.push(issue("harness", "`firegrid/native` requires a string `harness` id"))
      }
      if (!("payload" in record)) {
        issues.push(issue("payload", "`firegrid/native` requires a `payload`"))
      }
      return
    }
    case "firegrid/usage":
    default:
      // `firegrid/usage` fields are all optional; unknown `firegrid/*` suffixes
      // are accepted as ignorable extensions rather than rejected.
      return
  }
}

/**
 * Decode a single unknown value into a typed {@link L1StreamRecord}. Base and
 * recognized `firegrid/` variants are validated strictly; any other
 * `sessionUpdate` is preserved as a foreign, ignorable record.
 */
export const decodeStreamRecord = (value: unknown): L1Decoded<L1StreamRecord> => {
  if (!isRecord(value)) {
    return err([issue("", "expected an object with a `sessionUpdate` discriminant")])
  }
  const sessionUpdate = value["sessionUpdate"]
  if (typeof sessionUpdate !== "string") {
    return err([issue("sessionUpdate", "`sessionUpdate` is required and must be a string")])
  }

  const issues: Array<L1DecodeIssue> = []
  switch (sessionUpdate) {
    case "user_message_chunk":
    case "agent_message_chunk":
    case "agent_thought_chunk":
      validateChunk(value, issues)
      break
    case "tool_call":
      validateToolCall(value, true, issues)
      break
    case "tool_call_update":
      validateToolCall(value, false, issues)
      break
    case "plan":
      validatePlan(value, issues)
      break
    default:
      if (sessionUpdate.startsWith(FIREGRID_EXTENSION_PREFIX)) {
        validateFiregridExtension(value, sessionUpdate, issues)
      }
      // else: foreign namespace — accepted verbatim, no structural claims.
      break
  }

  if (issues.length > 0) return err(issues)
  return ok(value as L1StreamRecord)
}

/** Decode a raw update and narrow to a recognized Firegrid extension, if it is one. */
export const decodeFiregridExtension = (
  value: unknown
): L1Decoded<FiregridExtensionUpdate> => {
  const decoded = decodeStreamRecord(value)
  if (!decoded.ok) return decoded
  const sessionUpdate = decoded.value.sessionUpdate
  if (
    sessionUpdate === "firegrid/usage" ||
    sessionUpdate === "firegrid/subagent" ||
    sessionUpdate === "firegrid/native"
  ) {
    return ok(decoded.value as FiregridExtensionUpdate)
  }
  return err([issue("sessionUpdate", `\`${sessionUpdate}\` is not a recognized firegrid extension`)])
}

/**
 * Decode an ordered sequence of records. Collects every issue across the stream
 * (prefixed with the record index) so a malformed corpus reports all problems at
 * once rather than only the first.
 */
export const decodeStream = (
  values: ReadonlyArray<unknown>
): L1Decoded<ReadonlyArray<L1StreamRecord>> => {
  const records: Array<L1StreamRecord> = []
  const issues: Array<L1DecodeIssue> = []
  values.forEach((value, index) => {
    const decoded = decodeStreamRecord(value)
    if (decoded.ok) {
      records.push(decoded.value)
    } else {
      for (const problem of decoded.issues) {
        issues.push(issue(`[${index}]${problem.path === "" ? "" : `.${problem.path}`}`, problem.message))
      }
    }
  })
  if (issues.length > 0) return err(issues)
  return ok(records)
}

/** Format decode issues into a single human-readable line for error messages. */
export const formatIssues = (issues: ReadonlyArray<L1DecodeIssue>): string =>
  issues.map((i) => `${i.path === "" ? "<root>" : i.path}: ${i.message}`).join("; ")
