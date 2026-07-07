// @ts-expect-error Fable emits JavaScript without declaration files.
import * as Generated from "../dist/S2/Client.js"
// @ts-expect-error Fable emits JavaScript without declaration files.
import { S2Config, S2StreamRef } from "../dist/S2/Types.js"

type GeneratedClient = unknown

const generated = Generated as Record<string, unknown>

const generatedFunction = <Args extends ReadonlyArray<unknown>, A>(
  name: string
): ((...args: Args) => A) => {
  const value = generated[name]
  if (typeof value !== "function") {
    throw new Error(`Generated Firegrid.Log export ${name} is missing`)
  }
  return value as (...args: Args) => A
}

const facadeMake = generatedFunction<[S2Config], Promise<GeneratedClient>>("Facade_make")
const facadeEnsureBasin = generatedFunction<[string, GeneratedClient], Promise<void>>("Facade_ensureBasin")
const facadeEnsureStream = generatedFunction<[S2StreamRef, GeneratedClient], Promise<void>>("Facade_ensureStream")
const facadeCheckTail = generatedFunction<[S2StreamRef, GeneratedClient], Promise<unknown>>("Facade_checkTail")
const facadeAppend = generatedFunction<[S2StreamRef, AppendInput, GeneratedClient], Promise<unknown>>("Facade_append")
const facadeRead = generatedFunction<[S2StreamRef, ReadInput, ReadFormat, GeneratedClient], Promise<unknown>>("Facade_read")
const facadeReadSession = generatedFunction<
  [S2StreamRef, ReadInput, ReadFormat, GeneratedClient],
  Promise<AsyncIterable<unknown>>
>("Facade_readSession")

export interface S2ClientConfig {
  readonly accessToken: string
  readonly endpoint?: string
  readonly requestTimeoutMillis?: number
  readonly connectionTimeoutMillis?: number
}

export interface StreamRef {
  readonly basin: string
  readonly stream: string
}

export interface StreamPosition {
  readonly seqNum: number
  readonly timestamp: Date
}

export interface AppendAck {
  readonly start: StreamPosition
  readonly end: StreamPosition
  readonly tail: StreamPosition
}

export interface StringAppendRecord {
  readonly kind: "string"
  readonly body: string
  readonly headers?: ReadonlyArray<readonly [string, string]>
  readonly timestamp?: Date
}

export interface BytesAppendRecord {
  readonly kind: "bytes"
  readonly body: Uint8Array
  readonly headers?: ReadonlyArray<readonly [Uint8Array, Uint8Array]>
  readonly timestamp?: Date
}

export interface FenceAppendRecord {
  readonly kind: "fence"
  readonly token: string
}

export interface TrimAppendRecord {
  readonly kind: "trim"
  readonly seqNum: number
}

export type AppendRecord =
  | StringAppendRecord
  | BytesAppendRecord
  | FenceAppendRecord
  | TrimAppendRecord

export const AppendRecord = {
  string: (input: Omit<StringAppendRecord, "kind">): AppendRecord => ({ ...input, kind: "string" }),
  bytes: (input: Omit<BytesAppendRecord, "kind">): AppendRecord => ({ ...input, kind: "bytes" }),
  fence: (token: string): AppendRecord => ({ kind: "fence", token }),
  trim: (seqNum: number): AppendRecord => ({ kind: "trim", seqNum })
} as const

export interface AppendInput {
  readonly records: ReadonlyArray<AppendRecord>
  readonly matchSeqNum?: number
  readonly fencingToken?: string
}

export const AppendInput = {
  create: (
    records: ReadonlyArray<AppendRecord>,
    options: Pick<AppendInput, "matchSeqNum" | "fencingToken"> = {}
  ): AppendInput => ({
    records,
    ...options
  })
} as const

export type ReadStart =
  | { readonly from: { readonly seqNum: number } }
  | { readonly from: { readonly timestamp: Date } }
  | { readonly from: { readonly tailOffset: number } }

export interface ReadStop {
  readonly limits?: {
    readonly count?: number
    readonly bytes?: number
  }
  readonly untilTimestamp?: Date
  readonly waitSecs?: number
}

type ReadFormat = "string" | "bytes"

export interface ReadInput {
  readonly start?: ReadStart
  readonly clamp?: boolean
  readonly stop?: ReadStop
  readonly ignoreCommandRecords?: boolean
  readonly format?: ReadFormat
}

export interface StringReadRecord {
  readonly kind: "string"
  readonly seqNum: number
  readonly body: string
  readonly headers: ReadonlyArray<readonly [string, string]>
  readonly timestamp: Date
}

export interface BytesReadRecord {
  readonly kind: "bytes"
  readonly seqNum: number
  readonly body: Uint8Array
  readonly headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>
  readonly timestamp: Date
}

export type ReadRecord = StringReadRecord | BytesReadRecord

export interface ReadBatch {
  readonly records: ReadonlyArray<ReadRecord>
  readonly tail?: StreamPosition
}

interface S2ErrorInput {
  readonly message: string
  readonly code?: string
  readonly status?: number
  readonly origin?: string
  readonly data?: unknown
  readonly cause?: unknown
}

export class S2Error extends Error {
  readonly code?: string
  readonly status?: number
  readonly origin?: string
  readonly data?: unknown

  constructor(input: S2ErrorInput) {
    super(input.message, { cause: input.cause })
    this.name = "S2Error"
    if (input.code !== undefined) this.code = input.code
    if (input.status !== undefined) this.status = input.status
    if (input.origin !== undefined) this.origin = input.origin
    if (input.data !== undefined) this.data = input.data
  }
}

export class SeqNumMismatchError extends S2Error {
  readonly expectedSeqNum: number

  constructor(input: S2ErrorInput & { readonly expectedSeqNum: number }) {
    super(input)
    this.name = "SeqNumMismatchError"
    this.expectedSeqNum = input.expectedSeqNum
  }
}

export class FencingTokenMismatchError extends S2Error {
  readonly expectedFencingToken: string

  constructor(input: S2ErrorInput & { readonly expectedFencingToken: string }) {
    super(input)
    this.name = "FencingTokenMismatchError"
    this.expectedFencingToken = input.expectedFencingToken
  }
}

export class RangeNotSatisfiableError extends S2Error {
  readonly tailSeqNum?: number

  constructor(input: S2ErrorInput & { readonly tailSeqNum?: number }) {
    super(input)
    this.name = "RangeNotSatisfiableError"
    if (input.tailSeqNum !== undefined) this.tailSeqNum = input.tailSeqNum
  }
}

export interface FiregridLogStream {
  readonly append: (input: AppendInput) => Promise<AppendAck>
  readonly checkTail: () => Promise<{ readonly tail: StreamPosition }>
  readonly read: (input?: ReadInput) => Promise<ReadBatch>
  readonly readSession: (input?: ReadInput) => AsyncIterable<ReadRecord>
}

export interface FiregridLogClient {
  readonly ensureBasin: (basin: string) => Promise<void>
  readonly ensureStream: (target: StreamRef) => Promise<void>
  readonly stream: (target: StreamRef) => FiregridLogStream
}

const streamRef = (target: StreamRef): S2StreamRef =>
  new S2StreamRef(target.basin, target.stream)

const withS2Error = async <A>(promise: Promise<A>): Promise<A> => {
  try {
    return await promise
  } catch (cause) {
    throw toS2Error(cause)
  }
}

const field = (value: unknown, name: string): unknown =>
  typeof value === "object" && value !== null ? (value as Record<string, unknown>)[name] : undefined

const numberField = (value: unknown, name: string): number | undefined => {
  const found = field(value, name)
  if (typeof found === "number") return found
  if (typeof found === "bigint") return Number(found)
  return undefined
}

const stringField = (value: unknown, name: string): string | undefined => {
  const found = field(value, name)
  return typeof found === "string" ? found : undefined
}

const dateField = (value: unknown, name: string): Date => {
  const found = field(value, name)
  if (found instanceof Date) return found
  if (typeof found === "string" || typeof found === "number") return new Date(found)
  return new Date(Number(found))
}

const position = (raw: unknown): StreamPosition => ({
  seqNum: numberField(raw, "seqNum") ?? numberField(raw, "SeqNum") ?? 0,
  timestamp: dateField(raw, "timestamp")
})

const tailPosition = (raw: unknown): StreamPosition =>
  position(field(raw, "tail") ?? field(raw, "Tail"))

const appendAck = (raw: unknown): AppendAck => ({
  start: position(field(raw, "start") ?? field(raw, "Start")),
  end: position(field(raw, "end") ?? field(raw, "End")),
  tail: tailPosition(raw)
})

const readFormat = (input: ReadInput | undefined): ReadFormat => input?.format ?? "string"

const readInput = (input: ReadInput | undefined): ReadInput => input ?? {}

const tupleArray = (value: unknown): ReadonlyArray<readonly [unknown, unknown]> =>
  Array.isArray(value) ? value.map((item) => item as readonly [unknown, unknown]) : []

const readRecord = (raw: unknown, format: ReadFormat): ReadRecord => {
  const seqNum = numberField(raw, "seqNum") ?? numberField(raw, "SeqNum") ?? 0
  const timestamp = dateField(raw, "timestamp")
  const headers = tupleArray(field(raw, "headers"))
  if (format === "bytes") {
    return {
      kind: "bytes",
      seqNum,
      body: field(raw, "body") as Uint8Array,
      headers: headers.map(([key, value]) => [key as Uint8Array, value as Uint8Array] as const),
      timestamp
    }
  }
  return {
    kind: "string",
    seqNum,
    body: String(field(raw, "body") ?? ""),
    headers: headers.map(([key, value]) => [String(key), String(value)] as const),
    timestamp
  }
}

const readBatch = (raw: unknown, format: ReadFormat): ReadBatch => ({
  records: Array.isArray(field(raw, "records"))
    ? (field(raw, "records") as ReadonlyArray<unknown>).map((record) => readRecord(record, format))
    : [],
  ...(field(raw, "tail") === undefined ? {} : { tail: tailPosition(raw) })
})

const toS2Error = (cause: unknown): S2Error => {
  if (cause instanceof S2Error) return cause
  const message = stringField(cause, "message") ?? String(cause)
  const code = stringField(cause, "code")
  const status = numberField(cause, "status")
  const origin = stringField(cause, "origin")
  const data = field(cause, "data")
  const input = {
    message,
    ...(code === undefined ? {} : { code }),
    ...(status === undefined ? {} : { status }),
    ...(origin === undefined ? {} : { origin }),
    ...(data === undefined ? {} : { data }),
    cause
  }
  const expectedSeqNum = numberField(cause, "expectedSeqNum")
  if (expectedSeqNum !== undefined) {
    return new SeqNumMismatchError({ ...input, expectedSeqNum })
  }
  const expectedFencingToken = stringField(cause, "expectedFencingToken")
  if (expectedFencingToken !== undefined) {
    return new FencingTokenMismatchError({ ...input, expectedFencingToken })
  }
  const tail = field(cause, "tail")
  const tailSeqNum = numberField(tail, "seqNum") ?? numberField(tail, "seq_num")
  if (status === 416 || stringField(cause, "name") === "RangeNotSatisfiableError") {
    return new RangeNotSatisfiableError({
      ...input,
      ...(tailSeqNum === undefined ? {} : { tailSeqNum })
    })
  }
  return new S2Error(input)
}

const readSessionIterable = (
  source: AsyncIterable<unknown>,
  format: ReadFormat
): AsyncIterable<ReadRecord> => ({
  async *[Symbol.asyncIterator]() {
    const iterator = source[Symbol.asyncIterator]()
    try {
      while (true) {
        const next = await iterator.next()
        if (next.done === true) return
        yield readRecord(next.value, format)
      }
    } catch (cause) {
      throw toS2Error(cause)
    } finally {
      if (typeof iterator.return === "function") {
        await iterator.return()
      }
    }
  }
})

export const makeFiregridLog = async (config: S2ClientConfig): Promise<FiregridLogClient> => {
  const generatedConfig = new S2Config(
    config.accessToken,
    config.endpoint,
    config.requestTimeoutMillis,
    config.connectionTimeoutMillis
  )
  const client = await withS2Error(facadeMake(generatedConfig))
  return {
    ensureBasin: (basin) => withS2Error(facadeEnsureBasin(basin, client)),
    ensureStream: (target) => withS2Error(facadeEnsureStream(streamRef(target), client)),
    stream: (target) => {
      const ref = streamRef(target)
      return {
        append: (input) => withS2Error(facadeAppend(ref, input, client).then(appendAck)),
        checkTail: () => withS2Error(facadeCheckTail(ref, client).then((raw) => ({ tail: tailPosition(raw) }))),
        read: (input) => {
          const format = readFormat(input)
          return withS2Error(facadeRead(ref, readInput(input), format, client).then((raw) => readBatch(raw, format)))
        },
        readSession: (input) => {
          const format = readFormat(input)
          return readSessionIterable(
            {
              async *[Symbol.asyncIterator]() {
                const session = await withS2Error(facadeReadSession(ref, readInput(input), format, client))
                yield* session
              }
            },
            format
          )
        }
      }
    }
  }
}
