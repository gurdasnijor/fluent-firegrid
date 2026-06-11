import { Effect, pipe } from "effect"
import { BeginningOffset, NowOffset, type Offset, type StreamPath } from "./domainTypes.ts"
import type { DurableStreamLog } from "./durableStreamLog.ts"
import type { AppendStream, ReadPosition, StreamMetadata } from "./streamTypes.ts"

export const beginning = (path: StreamPath): ReadPosition => ({
  path,
  offset: BeginningOffset,
})

export const now = (path: StreamPath): ReadPosition => ({
  path,
  offset: NowOffset,
})

export const fromOffset = (path: StreamPath, offset: Offset): ReadPosition => ({
  path,
  offset,
})

export const appendBytes = (
  log: DurableStreamLog,
  request: AppendStream,
  bytes: Uint8Array,
) => log.append({ ...request, messages: bytes.length === 0 ? [] : [bytes] })

export const appendEmpty = (log: DurableStreamLog, request: AppendStream) =>
  log.append({ ...request, messages: [] })

export const readCollect = (log: DurableStreamLog, position: ReadPosition) =>
  pipe(
    position,
    log.read,
    Effect.map((window) => window.records),
  )

export const readBytes = (log: DurableStreamLog, position: ReadPosition) =>
  pipe(
    position,
    log.read,
    Effect.map((window) =>
      window.records.reduce((acc, record) => {
        const out = new Uint8Array(acc.length + record.bytes.length)
        out.set(acc, 0)
        out.set(record.bytes, acc.length)
        return out
      }, new Uint8Array()),
    ),
  )

export const currentTail = (log: DurableStreamLog, path: StreamPath) =>
  pipe(
    path,
    log.head,
    Effect.map((metadata: StreamMetadata) => metadata.tailOffset),
  )
