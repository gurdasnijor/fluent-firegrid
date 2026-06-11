import { Effect } from "effect"
import { decodeStreamPath } from "@firegrid/fluent-stream-log"
import type { AppendStreamOutcome, StreamProblem } from "./model.ts"

export const STREAM_NEXT_OFFSET = "stream-next-offset"
export const STREAM_CLOSED = "stream-closed"
export const STREAM_UP_TO_DATE = "stream-up-to-date"
export const PRODUCER_ID = "producer-id"
export const PRODUCER_EPOCH = "producer-epoch"
export const PRODUCER_SEQ = "producer-seq"
export const PRODUCER_EXPECTED_SEQ = "producer-expected-seq"
export const PRODUCER_RECEIVED_SEQ = "producer-received-seq"

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
  }
}

export const problemBody = (problem: StreamProblem): string =>
  JSON.stringify({ code: problem.code, message: problem.message })

export const isProblem = (outcome: { readonly _tag: string }): outcome is StreamProblem =>
  outcome._tag === "BadRequest" ||
  outcome._tag === "Conflict" ||
  outcome._tag === "NotFound" ||
  outcome._tag === "Gone"

export const appendStatus = (outcome: AppendStreamOutcome): number => {
  switch (outcome._tag) {
    case "Appended":
    case "Noop":
      return 204
    case "Duplicate":
      return 200
    case "AlreadyClosed":
    case "WriteToClosed":
    case "ContentMismatch":
    case "OffsetConflict":
    case "SequenceGap":
      return 409
    case "Fenced":
      return 403
    case "BadRequest":
    case "Conflict":
    case "NotFound":
    case "Gone":
      return problemStatus(outcome)
  }
}

export const maybeJson = (contentType: string, bytes: Uint8Array): unknown => {
  const essence = contentType.split(";")[0]?.trim().toLowerCase()
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
