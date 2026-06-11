/* eslint-disable effect/no-runPromise */
import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http"
import { Effect, Stream } from "effect"
import type { DurableStreamsServer } from "./model.ts"
import {
  appendStatusFor,
  canonicalContentType,
  defaultHeaders,
  etagFor,
  isProblem,
  pathFromUrl,
  problemBody,
  problemStatus,
  PRODUCER_EPOCH,
  PRODUCER_EXPECTED_SEQ,
  PRODUCER_ID,
  PRODUCER_RECEIVED_SEQ,
  PRODUCER_SEQ,
  STREAM_CLOSED,
  STREAM_CURSOR,
  STREAM_FORKED_FROM,
  STREAM_FORK_OFFSET,
  STREAM_FORK_SUB_OFFSET,
  STREAM_NEXT_OFFSET,
  STREAM_SEQ,
  STREAM_SSE_DATA_ENCODING,
  STREAM_EXPIRES_AT,
  STREAM_TTL,
  STREAM_UP_TO_DATE,
  textEncoder,
} from "./httpShared.ts"
import { contentTypeEssence, isUtf8ReadableContentType } from "./content.ts"
import type { ReadStreamOutcome, StreamEvent, StreamProblem } from "./model.ts"

export interface StartedHttpServer {
  readonly url: string
  readonly close: () => Promise<void>
}

const requestBody = (request: IncomingMessage): Promise<Uint8Array> =>
  new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    request.on("data", (chunk: Buffer) => chunks.push(chunk))
    request.on("error", reject)
    request.on("end", () => resolve(new Uint8Array(Buffer.concat(chunks))))
  })

const send = (
  response: ServerResponse,
  status: number,
  headers: Record<string, string> = {},
  body?: string | Uint8Array,
) => {
  response.writeHead(status, { ...defaultHeaders, ...headers })
  response.end(body)
}

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)
const textDecoder = new TextDecoder()

const headerString = (
  value: string | string[] | undefined,
  fallback: string,
): string => Array.isArray(value) ? value[0] ?? fallback : value ?? fallback

const headerOptional = (value: string | string[] | undefined): string | undefined =>
  Array.isArray(value) ? value[0] : value

const badRequest = (message: string): StreamProblem => ({
  _tag: "BadRequest",
  code: "BAD_REQUEST",
  message,
})

const notFound = (message: string): StreamProblem => ({
  _tag: "NotFound",
  code: "NOT_FOUND",
  message,
})

const parseIntegerHeader = (
  headers: IncomingMessage["headers"],
  name: string,
): number | StreamProblem | undefined => {
  const value = headerOptional(headers[name])
  if (value === undefined) {
    return undefined
  }
  if (!/^(?:0|[1-9][0-9]*)$/.test(value)) {
    return badRequest(`invalid ${name}`)
  }
  return Number(value)
}

const parseProducer = (
  request: IncomingMessage,
): { readonly producerId: string; readonly epoch: number; readonly seq: number } | StreamProblem | undefined => {
  const producerId = headerOptional(request.headers[PRODUCER_ID])
  const epoch = parseIntegerHeader(request.headers, PRODUCER_EPOCH)
  const seq = parseIntegerHeader(request.headers, PRODUCER_SEQ)
  const present = [
    producerId !== undefined,
    epoch !== undefined,
    seq !== undefined,
  ].filter(Boolean).length
  if (present === 0) {
    return undefined
  }
  if (present !== 3 || producerId === undefined || epoch === undefined || seq === undefined) {
    return badRequest("producer headers must be supplied together")
  }
  if (producerId.length === 0) {
    return badRequest("producer-id must not be empty")
  }
  if (typeof epoch !== "number") {
    return epoch
  }
  if (typeof seq !== "number") {
    return seq
  }
  return { producerId, epoch, seq }
}

const parseLimit = (url: URL): number | StreamProblem | undefined => {
  const value = url.searchParams.get("chunk-size")
  if (value === null) {
    return undefined
  }
  if (!/^[1-9][0-9]*$/.test(value)) {
    return badRequest("chunk-size must be a positive integer")
  }
  return Number(value)
}

const problemResponse = (response: ServerResponse, problem: StreamProblem, headers: Record<string, string> = {}) =>
  send(response, problemStatus(problem), { ...headers, "content-type": "application/json" }, problemBody(problem))

const locationFor = (request: IncomingMessage, path: string): string => {
  const host = headerString(request.headers.host, "127.0.0.1")
  return `http://${host}/${encodeURI(path).replace(/^\/+/, "")}`
}

const streamClosedHeader = (closed: boolean): Record<string, string> =>
  closed ? { [STREAM_CLOSED]: "true" } : {}

interface Lifetime {
  readonly ttl?: string
  readonly expiresAt?: string
  readonly deadline: number
}

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

const cursorFor = (url: URL): string => {
  const requested = url.searchParams.get("cursor")
  const base = requested !== null && /^[0-9]+$/.test(requested) ? Number(requested) : Date.now()
  return String(base + 1)
}

const sseDataLines = (data: string): string =>
  data.split(/\r\n|\r|\n/).map((line) => `data:${line}`).join("\n")

const sseEvent = (event: string, data: string): string =>
  `event: ${event}\n${sseDataLines(data)}\n\n`

const recordSsePayload = (contentType: string, bytes: Uint8Array): string => {
  if (contentTypeEssence(contentType) === "application/json") {
    return `[${textDecoder.decode(bytes)}]`
  }
  return isUtf8ReadableContentType(contentType)
    ? textDecoder.decode(bytes)
    : Buffer.from(bytes).toString("base64")
}

type ReadSuccess = Extract<ReadStreamOutcome, { readonly _tag: "Read" }>

const readBody = (contentType: string, outcome: ReadSuccess): Uint8Array =>
  contentTypeEssence(contentType) === "application/json"
    ? textEncoder.encode(`[${outcome.records.map((record) => textDecoder.decode(record.bytes)).join(",")}]`)
    : Buffer.concat(outcome.records.map((record) => Buffer.from(record.bytes)))

const readHeaders = (
  contentType: string,
  outcome: ReadSuccess,
  extra: Record<string, string> = {},
): Record<string, string> => ({
  "content-type": contentType,
  [STREAM_NEXT_OFFSET]: outcome.nextOffset,
  ...(outcome.upToDate && { [STREAM_UP_TO_DATE]: "true" }),
  ...streamClosedHeader(outcome.closed),
  ...extra,
})

const ensureReadOutcome = (
  response: ServerResponse,
  outcome: ReadStreamOutcome,
): ReadSuccess | undefined => {
  if (isProblem(outcome)) {
    problemResponse(response, outcome)
    return undefined
  }
  if (outcome._tag !== "Read") {
    send(response, 500, { "content-type": "text/plain" }, "unexpected read outcome")
    return undefined
  }
  return outcome
}

const initialWireOffset = "00000000000000000000"

const normalizeWireOffset = (offset: string): string =>
  /^0+_0+$/.test(offset) ? initialWireOffset : offset

const offsetOrdinal = (offset: string): number => {
  const match = /^([0-9]+)(?:_([0-9]+))?$/.exec(offset)
  if (match === null) {
    return Number.NaN
  }
  return Number(match[1]) + Number(match[2] ?? "0")
}

export const startHttpServer = async (
  durableStreams: DurableStreamsServer,
  options?: { readonly port?: number },
): Promise<StartedHttpServer> => {
  const lifetimes = new Map<string, Lifetime>()
  const forkSpecs = new Map<string, string>()

  const lifetimeHeaders = (path: string): Record<string, string> => {
    const lifetime = lifetimes.get(path)
    if (lifetime === undefined) {
      return {}
    }
    return {
      ...(lifetime.ttl !== undefined && { [STREAM_TTL]: lifetime.ttl }),
      ...(lifetime.expiresAt !== undefined && { [STREAM_EXPIRES_AT]: lifetime.expiresAt }),
    }
  }

  const parseLifetime = (request: IncomingMessage): Lifetime | StreamProblem | undefined => {
    const ttl = headerOptional(request.headers[STREAM_TTL])
    const expiresAt = headerOptional(request.headers[STREAM_EXPIRES_AT])
    if (ttl !== undefined && expiresAt !== undefined) {
      return badRequest("stream-ttl and stream-expires-at are mutually exclusive")
    }
    if (ttl !== undefined) {
      if (!/^[1-9][0-9]*$/.test(ttl)) {
        return badRequest("stream-ttl must be a positive integer without leading zeroes")
      }
      return { ttl, deadline: Date.now() + Number(ttl) * 1000 }
    }
    if (expiresAt !== undefined) {
      const deadline = Date.parse(expiresAt)
      if (!Number.isFinite(deadline)) {
        return badRequest("stream-expires-at must be a valid timestamp")
      }
      return { expiresAt, deadline }
    }
    return undefined
  }

  const isExpired = (path: string): boolean => {
    const lifetime = lifetimes.get(path)
    return lifetime !== undefined && Date.now() >= lifetime.deadline
  }

  const touchLifetime = (path: string): void => {
    const lifetime = lifetimes.get(path)
    if (lifetime?.ttl !== undefined) {
      lifetimes.set(path, { ...lifetime, deadline: Date.now() + Number(lifetime.ttl) * 1000 })
    }
  }

  const handle = async (request: IncomingMessage, response: ServerResponse) => {
    try {
      const url = new URL(request.url ?? "/", "http://127.0.0.1")
      const path = await run(pathFromUrl(url))
      if (isExpired(path)) {
        if (request.method === "PUT" && request.headers[STREAM_FORKED_FROM] === undefined) {
          await run(durableStreams.delete(path))
          lifetimes.delete(path)
        } else {
          problemResponse(response, notFound("stream expired"))
          return
        }
      }
      const rawContentType = headerString(request.headers["content-type"], "application/octet-stream")
      const contentType = canonicalContentType(rawContentType)

      switch (request.method) {
        case "PUT": {
          const body = await requestBody(request)
          const requestedLifetime = parseLifetime(request)
          if (requestedLifetime !== undefined && "_tag" in requestedLifetime) {
            problemResponse(response, requestedLifetime)
            return
          }
          const existingLifetime = lifetimes.get(path)
          if (
            existingLifetime !== undefined &&
            requestedLifetime !== undefined &&
            (existingLifetime.ttl !== requestedLifetime.ttl || existingLifetime.expiresAt !== requestedLifetime.expiresAt)
          ) {
            problemResponse(response, { _tag: "Conflict", code: "CONFLICT", message: "stream exists with different lifetime" })
            return
          }
          const forkedFrom = headerOptional(request.headers[STREAM_FORKED_FROM])
          const forkSubOffset = headerOptional(request.headers[STREAM_FORK_SUB_OFFSET])
          if (forkedFrom === undefined && forkSubOffset !== undefined) {
            problemResponse(response, badRequest("stream-fork-sub-offset requires stream-forked-from"))
            return
          }
          if (forkedFrom === undefined) {
            const existingHead = await run(durableStreams.head(path))
            if (isProblem(existingHead) && existingHead._tag === "Gone") {
              problemResponse(response, { _tag: "Conflict", code: "CONFLICT", message: "stream was soft-deleted" })
              return
            }
          }
          if (forkedFrom !== undefined) {
            const forkSource = await run(pathFromUrl(new URL(forkedFrom, "http://127.0.0.1")))
            const forkOffset = headerOptional(request.headers[STREAM_FORK_OFFSET])
            if (forkSubOffset !== undefined && !/^(?:0|[1-9][0-9]*)$/.test(forkSubOffset)) {
              problemResponse(response, badRequest("invalid stream-fork-sub-offset"))
              return
            }
            if (forkSubOffset !== undefined && forkSubOffset !== "0" && forkOffset === undefined) {
              problemResponse(response, badRequest("stream-fork-sub-offset requires stream-fork-offset"))
              return
            }
            const sourceHead = await run(durableStreams.head(forkSource))
            if (isProblem(sourceHead)) {
              problemResponse(response, sourceHead._tag === "Gone"
                ? { _tag: "Conflict", code: "CONFLICT", message: "cannot fork a soft-deleted stream" }
                : sourceHead)
              return
            }
            let normalizedForkOffset = forkOffset === undefined
              ? undefined
              : normalizeWireOffset(forkOffset)
            if (
              normalizedForkOffset !== undefined &&
              offsetOrdinal(normalizedForkOffset) > offsetOrdinal(sourceHead.metadata.tailOffset)
            ) {
              problemResponse(response, badRequest("stream-fork-offset is beyond source tail"))
              return
            }
            const sourceRead = await run(durableStreams.read({ path: forkSource, offset: "-1" as never }))
            if (isProblem(sourceRead)) {
              problemResponse(response, sourceRead)
              return
            }
            if (sourceRead._tag !== "Read") {
              send(response, 500, { "content-type": "text/plain" }, "unexpected read outcome")
              return
            }
            const subOffset = forkSubOffset === undefined ? undefined : Number(forkSubOffset)
            const prefixBodies: Uint8Array[] = []
            if (subOffset !== undefined && sourceRead.records.length === 0) {
              problemResponse(response, badRequest("stream-fork-sub-offset requires source data"))
              return
            }
            if (subOffset !== undefined && subOffset > 0) {
              const anchor = normalizedForkOffset ?? sourceHead.metadata.tailOffset
              if (contentTypeEssence(sourceHead.metadata.contentType) === "application/json") {
                const start = sourceRead.records.findIndex((record) => record.fromOffset === anchor)
                if (start < 0 || start + subOffset > sourceRead.records.length) {
                  problemResponse(response, badRequest("stream-fork-sub-offset exceeds JSON message count"))
                  return
                }
                normalizedForkOffset = sourceRead.records[start + subOffset - 1]!.nextOffset
              } else {
                const record = sourceRead.records.find((candidate) => candidate.fromOffset === anchor)
                if (record === undefined || subOffset > record.bytes.length) {
                  problemResponse(response, badRequest("stream-fork-sub-offset exceeds message length"))
                  return
                }
                if (subOffset === record.bytes.length) {
                  normalizedForkOffset = record.nextOffset
                } else {
                  normalizedForkOffset = anchor
                  prefixBodies.push(record.bytes.slice(0, subOffset))
                }
              }
            }
            const normalizedForkSubOffset = forkSubOffset === undefined || forkSubOffset === "0" ? "none" : forkSubOffset
            const forkSpec = `${forkSource}|${normalizedForkOffset ?? "head"}|${normalizedForkSubOffset}|${request.headers["content-type"] === undefined ? "inherit" : contentType}`
            const existingForkSpec = forkSpecs.get(path)
            if (existingForkSpec !== undefined && existingForkSpec !== forkSpec) {
              problemResponse(response, { _tag: "Conflict", code: "CONFLICT", message: "stream exists with different fork configuration" })
              return
            }
            const outcome = await run(durableStreams.fork({
              path,
              source: forkSource,
              ...(normalizedForkOffset !== undefined && { atOffset: normalizedForkOffset as never }),
              ...(request.headers["content-type"] !== undefined && { contentType }),
            }))
            if (isProblem(outcome)) {
              problemResponse(response, outcome)
              return
            }
            if (outcome._tag === "AlreadyExists" && request.headers["content-type"] !== undefined && outcome.metadata.contentType !== contentType) {
              problemResponse(response, { _tag: "Conflict", code: "CONFLICT", message: "stream exists with different content-type" })
              return
            }
            if (outcome._tag === "Created") {
              forkSpecs.set(path, forkSpec)
            }
            const inheritedLifetime = requestedLifetime === undefined
              ? lifetimes.get(forkSource)
              : requestedLifetime
            if (inheritedLifetime !== undefined) {
              lifetimes.set(path, inheritedLifetime.ttl !== undefined
                ? { ttl: inheritedLifetime.ttl, deadline: Date.now() + Number(inheritedLifetime.ttl) * 1000 }
                : inheritedLifetime)
            }
            for (const prefix of prefixBodies) {
              const appended = await run(durableStreams.append({
                path,
                contentType: outcome.metadata.contentType,
                body: prefix,
              }))
              if (isProblem(appended)) {
                problemResponse(response, appended)
                return
              }
            }
            if (body.length > 0) {
              const appended = await run(durableStreams.append({
                path,
                contentType: outcome.metadata.contentType,
                body,
              }))
              if (isProblem(appended)) {
                problemResponse(response, appended)
                return
              }
            }
            const headAfterFork = await run(durableStreams.head(path))
            if (isProblem(headAfterFork)) {
              problemResponse(response, headAfterFork)
              return
            }
            const metadata = headAfterFork.metadata
            send(response, outcome._tag === "Created" ? 201 : 200, {
              "content-type": metadata.contentType,
              [STREAM_NEXT_OFFSET]: metadata.tailOffset,
              ...(outcome._tag === "Created" && { location: locationFor(request, path) }),
              ...streamClosedHeader(metadata.closed),
              ...lifetimeHeaders(path),
            })
            return
          }
          const outcome = await run(durableStreams.create({
            path,
            contentType,
            body,
            closed: request.headers[STREAM_CLOSED] === "true",
          }))
          if (isProblem(outcome)) {
            problemResponse(response, outcome)
            return
          }
          if (requestedLifetime !== undefined) {
            lifetimes.set(path, requestedLifetime)
          }
          send(response, outcome._tag === "Created" ? 201 : 200, {
            "content-type": outcome.metadata.contentType,
            [STREAM_NEXT_OFFSET]: outcome.metadata.tailOffset,
            ...(outcome._tag === "Created" && { location: locationFor(request, path) }),
            ...streamClosedHeader(outcome.metadata.closed),
            ...lifetimeHeaders(path),
          })
          return
        }

        case "POST": {
          const body = await requestBody(request)
          if (request.headers["content-type"] === undefined && body.length > 0) {
            problemResponse(response, badRequest("content-type is required"))
            return
          }
          const producer = parseProducer(request)
          if (producer !== undefined && "_tag" in producer) {
            problemResponse(response, producer)
            return
          }
          const streamSeq = headerOptional(request.headers[STREAM_SEQ])
          const outcome = await run(durableStreams.append({
            path,
            contentType,
            body,
            close: request.headers[STREAM_CLOSED] === "true",
            ...(streamSeq !== undefined && { seq: streamSeq }),
            ...(producer !== undefined && { producer }),
          }))
          const status = outcome._tag === "SequenceGap" && outcome.expectedSeq === 0
            ? 400
            : outcome._tag === "Appended" && producer !== undefined && body.length === 0 && request.headers[STREAM_CLOSED] === "true"
            ? 204
            : appendStatusFor(outcome, producer !== undefined)
          const headers: Record<string, string> = {}
          switch (outcome._tag) {
            case "Appended":
            case "Noop":
            case "Duplicate":
              if (outcome._tag === "Appended") {
                touchLifetime(path)
              }
              headers[STREAM_NEXT_OFFSET] = outcome.metadata.tailOffset
              Object.assign(headers, lifetimeHeaders(path))
              Object.assign(headers, streamClosedHeader(outcome.metadata.closed))
              if (producer !== undefined) {
                headers[PRODUCER_EPOCH] = String(producer.epoch)
                headers[PRODUCER_SEQ] = String(outcome._tag === "Duplicate"
                  ? outcome.highestSeq ?? producer.seq
                  : producer.seq)
              }
              break
            case "AlreadyClosed":
            case "WriteToClosed":
              headers[STREAM_NEXT_OFFSET] = outcome.finalOffset
              Object.assign(headers, lifetimeHeaders(path))
              headers[STREAM_CLOSED] = "true"
              break
            case "Fenced":
              headers[PRODUCER_EPOCH] = String(outcome.currentEpoch)
              break
            case "SequenceGap":
              headers[PRODUCER_EXPECTED_SEQ] = String(outcome.expectedSeq)
              headers[PRODUCER_RECEIVED_SEQ] = String(outcome.receivedSeq)
              break
          }
          send(
            response,
            status,
            isProblem(outcome) ? { ...headers, "content-type": "application/json" } : headers,
            isProblem(outcome) ? problemBody(outcome) : undefined,
          )
          return
        }

        case "HEAD": {
          const outcome = await run(durableStreams.head(path))
          if (isProblem(outcome)) {
            send(response, problemStatus(outcome))
            return
          }
          send(response, 200, {
            "cache-control": "no-store",
            "content-type": outcome.metadata.contentType,
            [STREAM_NEXT_OFFSET]: outcome.metadata.tailOffset,
            ...streamClosedHeader(outcome.metadata.closed),
            ...lifetimeHeaders(path),
          })
          return
        }

        case "GET": {
          const offset = url.searchParams.get("offset") ?? undefined
          const live = url.searchParams.get("live") ?? undefined
          if ((live === "sse" || live === "long-poll") && offset === undefined) {
            problemResponse(response, badRequest("offset is required for live reads"))
            return
          }
          const limit = parseLimit(url)
          if (limit !== undefined && typeof limit !== "number") {
            problemResponse(response, limit)
            return
          }
          const head = await run(durableStreams.head(path))
          if (isProblem(head)) {
            problemResponse(response, head)
            return
          }
          const contentType = head.metadata.contentType

          if (live === "long-poll") {
            const cursor = cursorFor(url)
            const deadline = Date.now() + 500
            const readOffset = offset === "now" ? head.metadata.tailOffset : offset
            let outcome = await run(durableStreams.read({
              path,
              offset: readOffset as never,
              ...(limit !== undefined && { limit }),
            }))
            while (
              outcome._tag === "Read" &&
              outcome.records.length === 0 &&
              !outcome.closed &&
              Date.now() < deadline
            ) {
              await delay(25)
              outcome = await run(durableStreams.read({
                path,
                offset: readOffset as never,
                ...(limit !== undefined && { limit }),
              }))
            }
            const read = ensureReadOutcome(response, outcome)
            if (read === undefined) {
              return
            }
            const body = readBody(contentType, read)
            touchLifetime(path)
            send(response, body.length === 0 ? 204 : 200, {
              "cache-control": "no-cache, no-store",
              [STREAM_CURSOR]: cursor,
              ...readHeaders(contentType, read),
              ...lifetimeHeaders(path),
            }, body.length === 0 ? undefined : body)
            return
          }

          if (live === "sse") {
            const cursor = cursorFor(url)
            response.writeHead(200, {
              ...defaultHeaders,
              "content-type": "text/event-stream",
              "cache-control": "no-cache, no-store",
              connection: "keep-alive",
              ...(isUtf8ReadableContentType(contentType) ? {} : { [STREAM_SSE_DATA_ENCODING]: "base64" }),
            })
            await run(
              Effect.scoped(
                Effect.flatMap(
                  durableStreams.follow({ path, offset: offset as never }) as Effect.Effect<
                    Stream.Stream<StreamEvent, StreamProblem>,
                    StreamProblem
                  >,
                  (stream) =>
                    Stream.runForEach(stream, (event) =>
                      Effect.sync(() => {
                        switch (event._tag) {
                          case "Records":
                            for (const record of event.records) {
                              response.write(sseEvent("data", recordSsePayload(contentType, record.bytes)))
                            }
                            break
                          case "CaughtUp":
                            response.write(sseEvent("control", JSON.stringify({
                              streamNextOffset: event.offset,
                              upToDate: true,
                              streamCursor: cursor,
                            })))
                            break
                          case "Closed":
                            response.write(sseEvent("control", JSON.stringify({
                              streamNextOffset: event.finalOffset,
                              upToDate: true,
                              streamClosed: true,
                            })))
                            break
                        }
                      }),
                    ),
                ),
              ),
            ).catch((error: unknown) => {
              response.write(sseEvent("control", JSON.stringify({ error: String(error) })))
            })
            response.end()
            return
          }

          const outcome = await run(durableStreams.read({
            path,
            ...(offset !== undefined && { offset: offset as never }),
            ...(limit !== undefined && { limit }),
          }))
          const read = ensureReadOutcome(response, outcome)
          if (read === undefined) {
            return
          }
          const body = readBody(contentType, read)
          const etag = etagFor(path, read.nextOffset, read.closed)
          if (request.headers["if-none-match"] === etag) {
            send(response, 304, { etag })
            return
          }
          touchLifetime(path)
          send(response, 200, {
            "cache-control": "no-store",
            etag,
            ...readHeaders(contentType, read),
            ...lifetimeHeaders(path),
          }, body)
          return
        }

        case "DELETE": {
          const headBeforeDelete = await run(durableStreams.head(path))
          if (isProblem(headBeforeDelete) && headBeforeDelete._tag === "Gone") {
            problemResponse(response, headBeforeDelete)
            return
          }
          const outcome = await run(durableStreams.delete(path))
          send(response, outcome._tag === "Deleted" ? 204 : problemStatus(outcome), {})
          return
        }

        default:
          send(response, 405)
      }
    } catch (error) {
      if (response.headersSent) {
        response.end()
        return
      }
      send(response, 500, { "content-type": "text/plain" }, error instanceof Error ? error.message : String(error))
    }
  }

  const server: Server = createServer((request, response) => {
    void handle(request, response)
  })

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject)
    server.listen(options?.port ?? 0, "127.0.0.1", () => resolve())
  })

  const address = server.address()
  if (address === null || typeof address === "string") {
    throw new Error("failed to start HTTP server")
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((error) => error === undefined ? resolve() : reject(error))
      }),
  }
}
