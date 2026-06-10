import { Effect, Stream, pipe } from "effect"
import { BeginningOffset, NowOffset, type Offset, type StreamPath } from "./domainTypes.ts"
import type { DurableStreamLog } from "./services.ts"
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
) => Stream.make(bytes).pipe(Stream.run(log.append(request)))

export const appendEmpty = (log: DurableStreamLog, request: AppendStream) =>
  Stream.empty.pipe(Stream.run(log.append(request)))

export const readCollect = (log: DurableStreamLog, position: ReadPosition) =>
  pipe(position, log.read, Effect.flatMap(Stream.runCollect))

export const readBytes = (log: DurableStreamLog, position: ReadPosition) =>
  pipe(
    position,
    log.read,
    Effect.flatMap((records) =>
      records.pipe(
        Stream.runFold(new Uint8Array(), (acc, record) => {
          const out = new Uint8Array(acc.length + record.bytes.length)
          out.set(acc, 0)
          out.set(record.bytes, acc.length)
          return out
        }),
      ),
    ),
  )

export const currentTail = (log: DurableStreamLog, path: StreamPath) =>
  pipe(
    path,
    log.head,
    Effect.map((metadata: StreamMetadata) => metadata.tailOffset),
  )
