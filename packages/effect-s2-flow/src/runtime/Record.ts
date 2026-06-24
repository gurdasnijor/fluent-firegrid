import { AppendRecord, type ReadRecord } from "effect-s2"

const ownerHeader = "effect-s2-flow-owner"
const writeHeader = "effect-s2-flow-write-id"

export type StringFlowAppendRecord = ReturnType<typeof AppendRecord.string>

export interface FlowRecord {
  readonly seqNum: number
  readonly body: string
  readonly headers: ReadonlyArray<readonly [string, string]>
}

export interface OwnedAppendAck {
  readonly startSeqNum: number
  readonly endSeqNum: number
  readonly records: ReadonlyArray<FlowRecord>
}

export const fromReadRecord = (record: ReadRecord<"string">): FlowRecord => ({
  seqNum: record.seqNum,
  body: record.body,
  headers: record.headers
})

export const stringRecord = (
  body: string,
  headers?: ReadonlyArray<readonly [string, string]>
): StringFlowAppendRecord =>
  AppendRecord.string({
    body,
    ...(headers === undefined ? {} : { headers })
  })

export const ownedRecord = (
  record: StringFlowAppendRecord,
  ownerId: string,
  writeId: number
): StringFlowAppendRecord => {
  const headers = normalizeHeaders(record.headers).filter(([key]) => key !== ownerHeader && key !== writeHeader)
  return stringRecord(record.body, [
    ...headers,
    [ownerHeader, ownerId],
    [writeHeader, String(writeId)]
  ])
}

export const appendRecordToFlowRecord = (seqNum: number, record: StringFlowAppendRecord): FlowRecord => ({
  seqNum,
  body: record.body,
  headers: normalizeHeaders(record.headers)
})

export const ownerId = (record: FlowRecord): string | undefined =>
  record.headers.find(([key]) => key === ownerHeader)?.[1]

const normalizeHeaders = (
  headers: StringFlowAppendRecord["headers"]
): ReadonlyArray<readonly [string, string]> => {
  if (headers === undefined) {
    return []
  }

  return headers.map(([key, value]) => [key, value] as const)
}
