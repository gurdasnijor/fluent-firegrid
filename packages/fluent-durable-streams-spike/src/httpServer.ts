/* eslint-disable effect/no-runPromise, local/no-date-now, local/no-production-js-timers */
import { NodeHttpServer } from "@effect/platform-node"
import { createServer, type Server } from "node:http"
import { Effect, Fiber, Layer, Stream } from "effect"
import { HttpRouter, HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { contentTypeEssence, isUtf8ReadableContentType } from "./content.ts"
import { DurableStreamsHttpApi } from "./httpApi.ts"
import {
  appendStatusFor,
  badRequest,
  canonicalContentType,
  defaultHeaders,
  etagFor,
  isProblem,
  notFound,
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
  STREAM_UP_TO_DATE,
  textEncoder,
} from "./httpShared.ts"
import { makeHttpServerState, type HttpServerState } from "./httpServerState.ts"
import type { DurableStreamsServer, ReadStreamOutcome, StreamProblem } from "./model.ts"

export interface StartedHttpServer {
  readonly url: string
  readonly close: () => Promise<void>
}

const run = <A, E>(effect: Effect.Effect<A, E>) => Effect.runPromise(effect)
const textDecoder = new TextDecoder()

const headerString = (
  value: string | readonly string[] | undefined,
  fallback: string,
): string => typeof value === "string" ? value : value?.[0] ?? fallback

const headerOptional = (value: string | readonly string[] | undefined): string | undefined =>
  typeof value === "string" ? value : value?.[0]

const trueHeader = (value: string | readonly string[] | undefined): boolean =>
  headerOptional(value)?.toLowerCase() === "true"

const requestBody = (request: { readonly arrayBuffer: Effect.Effect<ArrayBuffer, unknown> }) =>
  Effect.map(request.arrayBuffer, (buffer) => new Uint8Array(buffer))

const response = (
  status: number,
  headers: Record<string, string> = {},
  body?: string | Uint8Array,
): HttpServerResponse.HttpServerResponse => {
  const mergedHeaders = { ...defaultHeaders, ...headers }
  if (body === undefined) {
    return HttpServerResponse.empty({ status, headers: mergedHeaders })
  }
  return typeof body === "string"
    ? HttpServerResponse.text(body, { status, headers: mergedHeaders })
    : HttpServerResponse.uint8Array(body, { status, headers: mergedHeaders })
}

const problemResponse = (problem: StreamProblem, headers: Record<string, string> = {}) =>
  response(problemStatus(problem), { ...headers, "content-type": "application/json" }, problemBody(problem))

const streamClosedHeader = (closed: boolean): Record<string, string> =>
  closed ? { [STREAM_CLOSED]: "true" } : {}

const parseIntegerHeader = (
  headers: Record<string, string | readonly string[] | undefined>,
  name: string,
): number | StreamProblem | undefined => {
  const value = headerOptional(headers[name])
  if (value === undefined) {
    return undefined
  }
  if (!/^(?:0|[1-9][0-9]*)$/.test(value)) {
    return badRequest(`invalid ${name}`)
  }
  const parsed = Number(value)
  if (parsed > Number.MAX_SAFE_INTEGER) {
    return badRequest(`invalid ${name}`)
  }
  return parsed
}

const parseProducer = (
  headers: Record<string, string | readonly string[] | undefined>,
): { readonly producerId: string; readonly epoch: number; readonly seq: number } | StreamProblem | undefined => {
  const producerId = headerOptional(headers[PRODUCER_ID])
  const epoch = parseIntegerHeader(headers, PRODUCER_EPOCH)
  const seq = parseIntegerHeader(headers, PRODUCER_SEQ)
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

const locationFor = (headers: Record<string, string | readonly string[] | undefined>, path: string): string => {
  const host = headerString(headers.host, "127.0.0.1")
  return `http://${host}/${encodeURI(path).replace(/^\/+/, "")}`
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
  ...streamClosedHeader(outcome.closed && outcome.upToDate),
  ...extra,
})

const ensureReadOutcome = (outcome: ReadStreamOutcome): ReadSuccess | HttpServerResponse.HttpServerResponse => {
  if (isProblem(outcome)) {
    return problemResponse(outcome)
  }
  if (outcome._tag !== "Read") {
    return response(500, { "content-type": "text/plain" }, "unexpected read outcome")
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

const handleRequest = (
  durableStreams: DurableStreamsServer,
  state: HttpServerState,
  request: {
    readonly method: string
    readonly url: string
    readonly headers: Record<string, string | readonly string[] | undefined>
    readonly arrayBuffer: Effect.Effect<ArrayBuffer, unknown>
  },
): Effect.Effect<HttpServerResponse.HttpServerResponse> =>
  Effect.gen(function*() {
    const url = new URL(request.url, "http://127.0.0.1")
    const path = yield* pathFromUrl(url)
    if (state.isExpired(path)) {
      if (request.method === "PUT" && request.headers[STREAM_FORKED_FROM] === undefined) {
        yield* durableStreams.delete(path)
        state.deleteLifetime(path)
      } else {
        return problemResponse(notFound("stream expired"))
      }
    }
    const rawContentType = headerString(request.headers["content-type"], "application/octet-stream")
    const contentType = canonicalContentType(rawContentType)

    switch (request.method) {
      case "PUT": {
        const body = yield* requestBody(request)
        const requestedLifetime = state.parseLifetime(request.headers)
        if (requestedLifetime !== undefined && "_tag" in requestedLifetime) {
          return problemResponse(requestedLifetime)
        }
        const existingLifetime = state.getLifetime(path)
        if (
          existingLifetime !== undefined &&
          requestedLifetime !== undefined &&
          (existingLifetime.ttl !== requestedLifetime.ttl || existingLifetime.expiresAt !== requestedLifetime.expiresAt)
        ) {
          return problemResponse({ _tag: "Conflict", code: "CONFLICT", message: "stream exists with different lifetime" })
        }
        const forkedFrom = headerOptional(request.headers[STREAM_FORKED_FROM])
        const forkSubOffset = headerOptional(request.headers[STREAM_FORK_SUB_OFFSET])
        if (forkedFrom === undefined && forkSubOffset !== undefined) {
          return problemResponse(badRequest("stream-fork-sub-offset requires stream-forked-from"))
        }
        if (forkedFrom === undefined) {
          const existingHead = yield* durableStreams.head(path)
          if (isProblem(existingHead) && existingHead._tag === "Gone") {
            return problemResponse({ _tag: "Conflict", code: "CONFLICT", message: "stream was soft-deleted" })
          }
        }
        if (forkedFrom !== undefined) {
          const forkSource = yield* pathFromUrl(new URL(forkedFrom, "http://127.0.0.1"))
          const forkOffset = headerOptional(request.headers[STREAM_FORK_OFFSET])
          if (forkSubOffset !== undefined && !/^(?:0|[1-9][0-9]*)$/.test(forkSubOffset)) {
            return problemResponse(badRequest("invalid stream-fork-sub-offset"))
          }
          if (forkSubOffset !== undefined && forkSubOffset !== "0" && forkOffset === undefined) {
            return problemResponse(badRequest("stream-fork-sub-offset requires stream-fork-offset"))
          }
          const sourceHead = yield* durableStreams.head(forkSource)
          if (isProblem(sourceHead)) {
            return problemResponse(sourceHead._tag === "Gone"
              ? { _tag: "Conflict", code: "CONFLICT", message: "cannot fork a soft-deleted stream" }
              : sourceHead)
          }
          let normalizedForkOffset = forkOffset === undefined
            ? undefined
            : normalizeWireOffset(forkOffset)
          if (
            normalizedForkOffset !== undefined &&
            offsetOrdinal(normalizedForkOffset) > offsetOrdinal(sourceHead.metadata.tailOffset)
          ) {
            return problemResponse(badRequest("stream-fork-offset is beyond source tail"))
          }
          const sourceRead = yield* durableStreams.read({ path: forkSource, offset: "-1" })
          if (isProblem(sourceRead)) {
            return problemResponse(sourceRead)
          }
          if (sourceRead._tag !== "Read") {
            return response(500, { "content-type": "text/plain" }, "unexpected read outcome")
          }
          const subOffset = forkSubOffset === undefined ? undefined : Number(forkSubOffset)
          const prefixBodies: Uint8Array[] = []
          if (subOffset !== undefined && sourceRead.records.length === 0) {
            return problemResponse(badRequest("stream-fork-sub-offset requires source data"))
          }
          if (subOffset !== undefined && subOffset > 0) {
            const anchor = normalizedForkOffset ?? sourceHead.metadata.tailOffset
            if (contentTypeEssence(sourceHead.metadata.contentType) === "application/json") {
              const start = sourceRead.records.findIndex((record) => record.fromOffset === anchor)
              if (start < 0 || start + subOffset > sourceRead.records.length) {
                return problemResponse(badRequest("stream-fork-sub-offset exceeds JSON message count"))
              }
              normalizedForkOffset = sourceRead.records[start + subOffset - 1]!.nextOffset
            } else {
              const record = sourceRead.records.find((candidate) => candidate.fromOffset === anchor)
              if (record === undefined || subOffset > record.bytes.length) {
                return problemResponse(badRequest("stream-fork-sub-offset exceeds message length"))
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
          const existingForkSpec = state.getForkSpec(path)
          if (existingForkSpec !== undefined && existingForkSpec !== forkSpec) {
            return problemResponse({ _tag: "Conflict", code: "CONFLICT", message: "stream exists with different fork configuration" })
          }
          const outcome = yield* durableStreams.fork({
            path,
            source: forkSource,
            ...(normalizedForkOffset !== undefined && { atOffset: normalizedForkOffset as never }),
            ...(request.headers["content-type"] !== undefined && { contentType }),
          })
          if (isProblem(outcome)) {
            return problemResponse(outcome)
          }
          if (outcome._tag === "AlreadyExists" && request.headers["content-type"] !== undefined && outcome.metadata.contentType !== contentType) {
            return problemResponse({ _tag: "Conflict", code: "CONFLICT", message: "stream exists with different content-type" })
          }
          if (outcome._tag === "Created") {
            state.setForkSpec(path, forkSpec)
          }
          const inheritedLifetime = requestedLifetime === undefined
            ? state.getLifetime(forkSource)
            : requestedLifetime
          if (inheritedLifetime !== undefined) {
            state.setLifetime(path, inheritedLifetime.ttl !== undefined
              ? { ttl: inheritedLifetime.ttl, deadline: Date.now() + Number(inheritedLifetime.ttl) * 1000 }
              : inheritedLifetime)
          }
          const prefixAppends = yield* Effect.forEach(prefixBodies, (prefix) =>
            durableStreams.append({
              path,
              contentType: outcome.metadata.contentType,
              body: prefix,
            }))
          const prefixProblem = prefixAppends.find(isProblem)
          if (prefixProblem !== undefined) {
            return problemResponse(prefixProblem)
          }
          if (body.length > 0) {
            const appended = yield* durableStreams.append({
              path,
              contentType: outcome.metadata.contentType,
              body,
            })
            if (isProblem(appended)) {
              return problemResponse(appended)
            }
          }
          const headAfterFork = yield* durableStreams.head(path)
          if (isProblem(headAfterFork)) {
            return problemResponse(headAfterFork)
          }
          const metadata = headAfterFork.metadata
          return response(outcome._tag === "Created" ? 201 : 200, {
            "content-type": metadata.contentType,
            [STREAM_NEXT_OFFSET]: metadata.tailOffset,
            ...(outcome._tag === "Created" && { location: locationFor(request.headers, path) }),
            ...streamClosedHeader(metadata.closed),
            ...state.lifetimeHeaders(path),
          })
        }
        const outcome = yield* durableStreams.create({
          path,
          contentType,
          body,
          closed: trueHeader(request.headers[STREAM_CLOSED]),
        })
        if (isProblem(outcome)) {
          return problemResponse(outcome)
        }
        if (requestedLifetime !== undefined) {
          state.setLifetime(path, requestedLifetime)
        }
        return response(outcome._tag === "Created" ? 201 : 200, {
          "content-type": outcome.metadata.contentType,
          [STREAM_NEXT_OFFSET]: outcome.metadata.tailOffset,
          ...(outcome._tag === "Created" && { location: locationFor(request.headers, path) }),
          ...streamClosedHeader(outcome.metadata.closed),
          ...state.lifetimeHeaders(path),
        })
      }

      case "POST": {
        const body = yield* requestBody(request)
        if (request.headers["content-type"] === undefined && body.length > 0) {
          return problemResponse(badRequest("content-type is required"))
        }
        const producer = parseProducer(request.headers)
        if (producer !== undefined && "_tag" in producer) {
          return problemResponse(producer)
        }
        const streamSeq = headerOptional(request.headers[STREAM_SEQ])
        const outcome = yield* durableStreams.append({
          path,
          contentType,
          body,
          close: trueHeader(request.headers[STREAM_CLOSED]),
          ...(streamSeq !== undefined && { seq: streamSeq }),
          ...(producer !== undefined && { producer }),
        })
        const status = outcome._tag === "SequenceGap" && outcome.expectedSeq === 0
          ? 400
          : outcome._tag === "Appended" && producer !== undefined && body.length === 0 && trueHeader(request.headers[STREAM_CLOSED])
          ? 204
          : appendStatusFor(outcome, producer !== undefined)
        const headers: Record<string, string> = {}
        switch (outcome._tag) {
          case "Appended":
          case "Noop":
          case "Duplicate":
            if (outcome._tag === "Appended") {
              state.touchLifetime(path)
            }
            headers[STREAM_NEXT_OFFSET] = outcome.metadata.tailOffset
            Object.assign(headers, state.lifetimeHeaders(path))
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
            Object.assign(headers, state.lifetimeHeaders(path))
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
        return response(
          status,
          isProblem(outcome) ? { ...headers, "content-type": "application/json" } : headers,
          isProblem(outcome) ? problemBody(outcome) : undefined,
        )
      }

      case "HEAD": {
        const outcome = yield* durableStreams.head(path)
        if (isProblem(outcome)) {
          return response(problemStatus(outcome))
        }
        return response(200, {
          "cache-control": "no-store",
          "content-type": outcome.metadata.contentType,
          [STREAM_NEXT_OFFSET]: outcome.metadata.tailOffset,
          ...streamClosedHeader(outcome.metadata.closed),
          ...state.lifetimeHeaders(path),
        })
      }

      case "GET": {
        const offset = url.searchParams.get("offset") ?? undefined
        const live = url.searchParams.get("live") ?? undefined
        if ((live === "sse" || live === "long-poll") && offset === undefined) {
          return problemResponse(badRequest("offset is required for live reads"))
        }
        const limit = parseLimit(url)
        if (limit !== undefined && typeof limit !== "number") {
          return problemResponse(limit)
        }
        const head = yield* durableStreams.head(path)
        if (isProblem(head)) {
          return problemResponse(head)
        }
        const streamContentType = head.metadata.contentType

        if (live === "long-poll") {
          const cursor = cursorFor(url)
          const deadline = Date.now() + 500
          const readOffset = offset === "now" ? head.metadata.tailOffset : offset
          let outcome = yield* durableStreams.read({
            path,
            offset: readOffset as never,
            ...(limit !== undefined && { limit }),
          })
          while (
            outcome._tag === "Read" &&
            outcome.records.length === 0 &&
            !outcome.closed &&
            Date.now() < deadline
          ) {
            yield* Effect.promise(() => delay(25))
            outcome = yield* durableStreams.read({
              path,
              offset: readOffset as never,
              ...(limit !== undefined && { limit }),
            })
          }
          const read = ensureReadOutcome(outcome)
          if ("status" in read) {
            return read
          }
          const body = readBody(streamContentType, read)
          state.touchLifetime(path)
          return response(body.length === 0 ? 204 : 200, {
            "cache-control": "no-cache, no-store",
            [STREAM_CURSOR]: cursor,
            ...readHeaders(streamContentType, read),
            ...state.lifetimeHeaders(path),
          }, body.length === 0 ? undefined : body)
        }

        if (live === "sse") {
          const cursor = cursorFor(url)
          const eventStream = Stream.unwrap(
            Effect.map(
              durableStreams.follow({ path, offset: offset as never }),
              (events) =>
                Stream.map(events, (event): Uint8Array => {
                  switch (event._tag) {
                    case "Records":
                      return textEncoder.encode(event.records.map((record) =>
                        sseEvent("data", recordSsePayload(streamContentType, record.bytes)),
                      ).join(""))
                    case "CaughtUp":
                      return textEncoder.encode(sseEvent("control", JSON.stringify({
                        streamNextOffset: event.offset,
                        upToDate: true,
                        streamCursor: cursor,
                      })))
                    case "Closed":
                      return textEncoder.encode(sseEvent("control", JSON.stringify({
                        streamNextOffset: event.finalOffset,
                        upToDate: true,
                        streamClosed: true,
                      })))
                  }
                }),
            ),
          )
          return HttpServerResponse.stream(eventStream, {
            status: 200,
            headers: {
              ...defaultHeaders,
              "content-type": "text/event-stream",
              "cache-control": "no-cache, no-store",
              connection: "keep-alive",
              ...(isUtf8ReadableContentType(streamContentType) ? {} : { [STREAM_SSE_DATA_ENCODING]: "base64" }),
            },
          })
        }

        const outcome = yield* durableStreams.read({
          path,
          ...(offset !== undefined && { offset: offset as never }),
          ...(limit !== undefined && { limit }),
        })
        const read = ensureReadOutcome(outcome)
        if ("status" in read) {
          return read
        }
        const body = readBody(streamContentType, read)
        const etag = etagFor(path, read.nextOffset, read.closed)
        const canValidateEtag = offset !== "now"
        if (canValidateEtag && request.headers["if-none-match"] === etag) {
          return response(304, { etag })
        }
        state.touchLifetime(path)
        return response(200, {
          "cache-control": "no-store",
          ...(canValidateEtag && { etag }),
          ...readHeaders(streamContentType, read),
          ...state.lifetimeHeaders(path),
        }, body)
      }

      case "DELETE": {
        const headBeforeDelete = yield* durableStreams.head(path)
        if (isProblem(headBeforeDelete) && headBeforeDelete._tag === "Gone") {
          return problemResponse(headBeforeDelete)
        }
        const outcome = yield* durableStreams.delete(path)
        return response(outcome._tag === "Deleted" ? 204 : problemStatus(outcome))
      }

      default:
        return response(405)
    }
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.succeed(response(500, { "content-type": "text/plain" }, String(cause))),
    ),
  )

const makeApiLayer = (
  durableStreams: DurableStreamsServer,
  state: HttpServerState,
  nodeServer: Server,
  options?: { readonly port?: number },
) => {
  const handleRaw = (request: Parameters<typeof handleRequest>[2]) =>
    handleRequest(durableStreams, state, request)

  const GroupLive = HttpApiBuilder.group(
    DurableStreamsHttpApi,
    "Streams",
    (handlers) =>
      handlers
        .handleRaw("createStream", ({ request }) => handleRaw(request))
        .handleRaw("appendStream", ({ request }) => handleRaw(request))
        .handleRaw("readStream", ({ request }) => handleRaw(request))
        .handleRaw("headStream", ({ request }) => handleRaw(request))
        .handleRaw("deleteStream", ({ request }) => handleRaw(request)),
  )

  return HttpApiBuilder.layer(DurableStreamsHttpApi).pipe(
    Layer.provide(GroupLive),
    HttpRouter.serve,
    Layer.provide(NodeHttpServer.layer(() => nodeServer, {
      host: "127.0.0.1",
      port: options?.port ?? 0,
    })),
  )
}

const waitForListening = (server: Server): Promise<void> =>
  server.listening
    ? Promise.resolve()
    : new Promise((resolve, reject) => {
      const onListening = () => {
        server.off("error", onError)
        resolve()
      }
      const onError = (error: Error) => {
        server.off("listening", onListening)
        reject(error)
      }
      server.once("listening", onListening)
      server.once("error", onError)
    })

export const startHttpServer = async (
  durableStreams: DurableStreamsServer,
  options?: { readonly port?: number },
): Promise<StartedHttpServer> => {
  const nodeServer = createServer()
  const state = makeHttpServerState()
  const layer = makeApiLayer(durableStreams, state, nodeServer, options)
  const fiber = Effect.runFork(Layer.launch(layer))

  await waitForListening(nodeServer)

  const address = nodeServer.address()
  if (address === null || typeof address === "string") {
    await run(Fiber.interrupt(fiber))
    throw new Error("failed to start HTTP server")
  }

  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () => run(Fiber.interrupt(fiber)).then(() => undefined),
  }
}
