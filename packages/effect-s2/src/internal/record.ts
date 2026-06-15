import type { BytesAppendRecord, ReadRecord, SdkAppendRecord, StringAppendRecord } from "./sdk.ts"

export interface S2Record {
  readonly seqNum: number
  readonly timestamp: number
  readonly headers: ReadonlyArray<readonly [string, string]>
  readonly body: string
}

export interface S2RecordBytes {
  readonly seqNum: number
  readonly timestamp: number
  readonly headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>
  readonly body: Uint8Array
}

export const toS2Record = (record: ReadRecord<"string">): S2Record => ({
  seqNum: record.seqNum,
  timestamp: record.timestamp.getTime(),
  headers: record.headers,
  body: record.body,
})

export const toS2RecordBytes = (record: ReadRecord<"bytes">): S2RecordBytes => ({
  seqNum: record.seqNum,
  timestamp: record.timestamp.getTime(),
  headers: record.headers,
  body: record.body,
})

export const isStringAppendRecord = (
  record: SdkAppendRecord,
): record is StringAppendRecord =>
  typeof record.body === "string"

export const isBytesAppendRecord = (
  record: SdkAppendRecord,
): record is BytesAppendRecord =>
  record.body instanceof Uint8Array
