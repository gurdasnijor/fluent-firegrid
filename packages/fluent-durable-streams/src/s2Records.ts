import {
  AppendRecord,
  type AppendAck as S2AppendAck,
  type ReadBatch as S2ReadBatch,
  type ReadRecord as S2ReadRecord,
  type StreamPosition as S2StreamPosition,
} from "@s2-dev/streamstore"
import { Effect, Schema } from "effect"
import type {
  AppendAck,
  ApiError,
  ReadBatch,
  ReadRecord,
  RecordKind,
  StateMessage,
  StateReadBatch,
  StateRecord,
  StreamPosition,
  TailResponse,
} from "./api.ts"
import {
  StateMessage as StateMessageSchema,
  StateRecordError,
} from "./api.ts"

const textEncoder = new TextEncoder()
const textDecoder = new TextDecoder()

export const KIND_HEADER = "ds-kind"
export const CLOSE_HEADER: readonly [string, string] = [KIND_HEADER, "close"]

export const decodeBase64 = (value: string): Uint8Array =>
  Uint8Array.from(atob(value), (char) => char.charCodeAt(0))

const encodeBase64 = (bytes: Uint8Array): string =>
  btoa(Array.from(bytes, (byte) => String.fromCharCode(byte)).join(""))

export const fromHeaderPair = ([name, value]: readonly [string, string]): readonly [Uint8Array, Uint8Array] => [
  textEncoder.encode(name),
  textEncoder.encode(value),
]

const decodeHeaders = (
  headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
): ReadonlyArray<readonly [string, string]> =>
  headers.map(([name, value]) => [textDecoder.decode(name), textDecoder.decode(value)] as const)

export const textHeaderValue = (
  headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>,
  name: string,
): string | undefined => {
  const value = headers.find(([candidate]) => textDecoder.decode(candidate) === name)?.[1]
  return value === undefined ? undefined : textDecoder.decode(value)
}

const recordKind = (headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>): RecordKind => {
  const value = textHeaderValue(headers, KIND_HEADER)
  switch (value) {
    case "state":
    case "close":
    case "meta":
      return value
    default:
      return "data"
  }
}

export const streamPosition = (position: S2StreamPosition): StreamPosition => ({
  seqNum: position.seqNum,
  timestamp: position.timestamp.toISOString(),
})

export const tailResponse = (tail: S2StreamPosition): TailResponse => ({
  tail: streamPosition(tail),
})

export const appendAck = (ack: S2AppendAck): AppendAck => ({
  start: streamPosition(ack.start),
  end: streamPosition(ack.end),
  tail: streamPosition(ack.tail),
})

export const readRecord = (record: S2ReadRecord<"bytes">): ReadRecord => ({
  seqNum: record.seqNum,
  body: encodeBase64(record.body),
  headers: decodeHeaders(record.headers),
  kind: recordKind(record.headers),
  timestamp: record.timestamp.toISOString(),
})

export const isCloseRecord = (record: S2ReadRecord<"bytes">): boolean =>
  recordKind(record.headers) === "close"

export const readBatch = (batch: S2ReadBatch<"bytes">): ReadBatch => ({
  records: batch.records.map(readRecord),
  ...(batch.tail === undefined ? {} : { tail: streamPosition(batch.tail) }),
  ...(batch.records.some(isCloseRecord) ? { closed: true } : {}),
})

export const sseEvent = (event: string, payload: unknown): Uint8Array =>
  textEncoder.encode(`event: ${event}\ndata: ${JSON.stringify(payload)}\n\n`)

export const sseBatch = (
  record: S2ReadRecord<"bytes">,
  tail: S2StreamPosition | undefined,
): Uint8Array =>
  sseEvent("batch", readBatch({
    records: [record],
    ...(tail === undefined ? {} : { tail }),
  }))

const isStateChange = (message: StateMessage): message is Extract<StateMessage, { readonly type: string }> =>
  "type" in message

const stringHeader = (name: string, value: string): readonly [string, string] => [name, value]

const optionalStateHeaders = (
  headers: Readonly<{
    readonly txid?: string | undefined
    readonly schema?: string | undefined
  }>,
): ReadonlyArray<readonly [string, string]> => [
  ...(headers.txid === undefined ? [] : [stringHeader("ds-state-txid", headers.txid)]),
  ...(headers.schema === undefined ? [] : [stringHeader("ds-state-schema", headers.schema)]),
]

const stateHeaders = (message: StateMessage): ReadonlyArray<readonly [string, string]> => {
  const base = [
    stringHeader(KIND_HEADER, "state"),
    stringHeader("ds-content-type", "application/vnd.firegrid.state+json"),
  ]
  if (isStateChange(message)) {
    return [
      ...base,
      stringHeader("ds-state-kind", "change"),
      stringHeader("ds-state-type", message.type),
      stringHeader("ds-state-key", message.key),
      stringHeader("ds-state-operation", message.headers.operation),
      ...optionalStateHeaders(message.headers),
    ]
  }
  return [
    ...base,
    stringHeader("ds-state-kind", "control"),
    stringHeader("ds-state-control", message.headers.control),
    ...optionalStateHeaders(message.headers),
  ]
}

export const stateAppendRecord = (message: StateMessage): AppendRecord =>
  AppendRecord.string({
    body: JSON.stringify(message),
    headers: stateHeaders(message),
  })

export const decodeStateRecord = (record: S2ReadRecord<"bytes">): Effect.Effect<StateRecord, ApiError> => {
  if (textHeaderValue(record.headers, KIND_HEADER) !== "state") {
    return Effect.fail(new StateRecordError({
      message: `S2 record ${record.seqNum} is not a state record`,
      code: "not-state-record",
    }))
  }
  return Schema.decodeUnknownEffect(StateMessageSchema)(JSON.parse(textDecoder.decode(record.body))).pipe(
    Effect.map((message): StateRecord => ({
      seqNum: record.seqNum,
      timestamp: record.timestamp.toISOString(),
      message,
    })),
    Effect.mapError((error) =>
      new StateRecordError({
        message: `Invalid state record ${record.seqNum}: ${String(error)}`,
        code: "invalid-state-record",
      }),
    ),
  )
}

export const stateReadBatch = (batch: S2ReadBatch<"bytes">): Effect.Effect<StateReadBatch, ApiError> =>
  Effect.forEach(
    batch.records.filter((record) => textHeaderValue(record.headers, KIND_HEADER) === "state"),
    decodeStateRecord,
  ).pipe(
    Effect.map((records) => ({
      records,
      ...(batch.tail === undefined ? {} : { tail: streamPosition(batch.tail) }),
      ...(batch.records.some(isCloseRecord) ? { closed: true } : {}),
    })),
  )
