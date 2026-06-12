import { Effect } from "effect"
import { decodeStreamPath } from "@firegrid/fluent-stream-log"
import type { AppendStreamOutcome, StreamProblem } from "./model.ts"
import { contentTypeEssence } from "./content.ts"

export const STREAM_NEXT_OFFSET = "stream-next-offset"
export const STREAM_CLOSED = "stream-closed"
export const STREAM_UP_TO_DATE = "stream-up-to-date"
export const STREAM_SEQ = "stream-seq"
export const STREAM_CURSOR = "stream-cursor"
export const STREAM_TTL = "stream-ttl"
export const STREAM_EXPIRES_AT = "stream-expires-at"
export const STREAM_FORKED_FROM = "stream-forked-from"
export const STREAM_FORK_OFFSET = "stream-fork-offset"
export const STREAM_FORK_SUB_OFFSET = "stream-fork-sub-offset"
export const STREAM_SSE_DATA_ENCODING = "stream-sse-data-encoding"
export const PRODUCER_ID = "producer-id"
export const PRODUCER_EPOCH = "producer-epoch"
export const PRODUCER_SEQ = "producer-seq"
export const PRODUCER_EXPECTED_SEQ = "producer-expected-seq"
export const PRODUCER_RECEIVED_SEQ = "producer-received-seq"

export const defaultHeaders: Record<string, string> = {
  "x-content-type-options": "nosniff",
  "cross-origin-resource-policy": "cross-origin",
}

export const badRequest = (message: string): StreamProblem => ({
  _tag: "BadRequest",
  code: "BAD_REQUEST",
  message,
})

export const notFound = (message: string): StreamProblem => ({
  _tag: "NotFound",
  code: "NOT_FOUND",
  message,
})

export const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

export const pathFromUrl = (url: URL) =>
  decodeStreamPath(decodeURIComponent(url.pathname.replace(/^\/+/, "")))

export const problemStatus = (problem: StreamProblem): number => {
  switch (problem._tag) {
    case "BadRequest":
      return 400
    case "Conflict":
      return 409
    case "NotFound":
      return 404
    case "Gone":
      return 410
    case "PayloadTooLarge":
      return 413
  }
}

export const problemBody = (problem: StreamProblem): string =>
  JSON.stringify({ code: problem.code, message: problem.message })

export const isProblem = (outcome: { readonly _tag: string }): outcome is StreamProblem =>
  outcome._tag === "BadRequest" ||
  outcome._tag === "Conflict" ||
  outcome._tag === "NotFound" ||
  outcome._tag === "Gone" ||
  outcome._tag === "PayloadTooLarge"

const appendStatus = (outcome: AppendStreamOutcome): number => {
  switch (outcome._tag) {
    case "Appended":
    case "Noop":
      return 204
    case "Duplicate":
      return 204
    case "WriteToClosed":
    case "ContentMismatch":
    case "OffsetConflict":
    case "SequenceGap":
      return 409
    case "AlreadyClosed":
      return 204
    case "Fenced":
      return 403
    case "BadRequest":
    case "Conflict":
    case "NotFound":
    case "Gone":
    case "PayloadTooLarge":
      return problemStatus(outcome)
  }
}

export const appendStatusFor = (
  outcome: AppendStreamOutcome,
  hasProducer: boolean,
): number =>
  outcome._tag === "Appended" && hasProducer ? 200 : appendStatus(outcome)

export const canonicalContentType = (contentType: string): string =>
  contentTypeEssence(contentType)

export const etagFor = (path: string, nextOffset: string, closed: boolean): string =>
  `"${Buffer.from(`${path}:${nextOffset}:${closed ? "1" : "0"}`).toString("base64url")}"`

export const maybeJson = (contentType: string, bytes: Uint8Array): unknown => {
  const essence = contentTypeEssence(contentType)
  if (essence !== "application/json") {
    return bytes
  }
  return JSON.parse(textDecoder.decode(bytes)) as unknown
}

export const responseBytes = (response: Response): Effect.Effect<Uint8Array, Error> =>
  Effect.tryPromise({
    try: async () => new Uint8Array(await response.arrayBuffer()),
    catch: (cause) => cause instanceof Error ? cause : new Error(String(cause)),
  })
