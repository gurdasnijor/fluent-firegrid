import { Effect, Stream } from "effect"
import type { Offset } from "@firegrid/fluent-stream-log"
import type {
  AppendStreamCommand,
  AppendStreamOutcome,
  CreateStreamCommand,
  CreateStreamOutcome,
  DeleteStreamOutcome,
  DurableStreamsChannel,
  HeadStreamOutcome,
  ReadStreamCommand,
  ReadStreamOutcome,
  StreamEvent,
  StreamProblem,
} from "./model.ts"
import {
  maybeJson,
  PRODUCER_EPOCH,
  PRODUCER_EXPECTED_SEQ,
  PRODUCER_ID,
  PRODUCER_RECEIVED_SEQ,
  PRODUCER_SEQ,
  responseBytes,
  STREAM_CLOSED,
  STREAM_NEXT_OFFSET,
  STREAM_UP_TO_DATE,
} from "./httpShared.ts"
import type { StreamBody } from "./content.ts"

export interface HttpChannelOptions {
  readonly baseUrl: string
  readonly fetch?: typeof fetch
}

const bodyInit = (body: StreamBody | undefined): BodyInit | undefined => {
  if (body === undefined) {
    return undefined
  }
  if (body instanceof Uint8Array) {
    const copy = new Uint8Array(body)
    return new Blob([copy.buffer])
  }
  if (typeof body === "string") {
    return body
  }
  return JSON.stringify(body)
}

const problem = (status: number, message: string): StreamProblem => {
  switch (status) {
    case 404:
      return { _tag: "NotFound", code: "NOT_FOUND", message }
    case 410:
      return { _tag: "Gone", code: "GONE", message }
    case 400:
      return { _tag: "BadRequest", code: "BAD_REQUEST", message }
    default:
      return { _tag: "Conflict", code: "CONFLICT", message }
  }
}

const requestUrl = (baseUrl: string, path: string, params?: URLSearchParams): string => {
  const url = new URL(path.replace(/^\/?/, "/"), baseUrl)
  if (params !== undefined) {
    url.search = params.toString()
  }
  return url.toString()
}

const fetchEffect = (
  fetchImpl: typeof fetch,
  input: string,
  init: RequestInit,
): Effect.Effect<Response> =>
  Effect.tryPromise({
    try: () => fetchImpl(input, init),
    catch: (cause) => problem(500, cause instanceof Error ? cause.message : String(cause)),
  }).pipe(Effect.catch((failure) => Effect.succeed(new Response(JSON.stringify(failure), {
      status: problemStatusCode(failure),
      statusText: failure.message,
    }))))

const problemStatusCode = (failure: StreamProblem): number => {
  switch (failure._tag) {
    case "BadRequest":
      return 400
    case "NotFound":
      return 404
    case "Gone":
      return 410
    case "Conflict":
      return 409
  }
}

const requestInit = (
  init: Omit<RequestInit, "body">,
  body: BodyInit | undefined,
): RequestInit => body === undefined ? init : { ...init, body }

interface HttpRead {
  readonly _tag: "HttpRead"
  readonly bytes: Uint8Array
  readonly contentType: string
  readonly nextOffset: Offset
  readonly upToDate: boolean
  readonly closed: boolean
}

const fetchRead = (
  fetchImpl: typeof fetch,
  baseUrl: string,
  command: ReadStreamCommand,
): Effect.Effect<HttpRead | StreamProblem> => {
  const params = new URLSearchParams()
  if (command.offset !== undefined) {
    params.set("offset", command.offset)
  }
  return fetchEffect(fetchImpl, requestUrl(baseUrl, command.path, params), { method: "GET" }).pipe(
    Effect.flatMap((response): Effect.Effect<HttpRead | StreamProblem> => {
      if (!response.ok) {
        return Effect.succeed(problem(response.status, response.statusText))
      }
      return responseBytes(response).pipe(
        Effect.mapError((error) => problem(500, error.message)),
        Effect.map((bytes): HttpRead => ({
          _tag: "HttpRead",
          bytes,
          contentType: response.headers.get("content-type") ?? "application/octet-stream",
          nextOffset: (response.headers.get(STREAM_NEXT_OFFSET) ?? "") as Offset,
          upToDate: response.headers.get(STREAM_UP_TO_DATE) === "true",
          closed: response.headers.get(STREAM_CLOSED) === "true",
        })),
        Effect.catch((failure) => Effect.succeed(failure)),
      )
    }),
  )
}

const readControl = (read: HttpRead) => ({
  nextOffset: read.nextOffset,
  upToDate: read.upToDate,
  closed: read.closed,
})

const mapHttpRead = (
  read: HttpRead | StreamProblem,
  f: (read: HttpRead) => ReadStreamOutcome,
): ReadStreamOutcome => read._tag === "HttpRead" ? f(read) : read

const parseCreate = (response: Response, command: CreateStreamCommand): CreateStreamOutcome => {
  if (response.status === 201 || response.status === 200) {
    const tailOffset = response.headers.get(STREAM_NEXT_OFFSET)
    if (tailOffset === null) {
      return problem(500, "missing stream-next-offset")
    }
    return {
      _tag: response.status === 201 ? "Created" : "AlreadyExists",
      metadata: {
        path: command.path,
        contentType: command.contentType,
        tailOffset: tailOffset as never,
        closed: response.headers.get(STREAM_CLOSED) === "true",
      },
    }
  }
  return problem(response.status, response.statusText)
}

const parseAppend = (response: Response, command: AppendStreamCommand): AppendStreamOutcome => {
  const finalOffset = response.headers.get(STREAM_NEXT_OFFSET)
  if (response.status === 204 || response.status === 200) {
    const metadata = {
      path: command.path,
      contentType: command.contentType,
      tailOffset: (finalOffset ?? "") as never,
      closed: response.headers.get(STREAM_CLOSED) === "true",
    }
    return response.status === 200 && response.headers.has(PRODUCER_SEQ)
      ? { _tag: "Duplicate", metadata }
      : { _tag: "Noop", metadata }
  }
  if (response.status === 403) {
    return {
      _tag: "Fenced",
      currentEpoch: Number(response.headers.get(PRODUCER_EPOCH) ?? "0"),
    }
  }
  if (response.status === 409 && response.headers.has(PRODUCER_EXPECTED_SEQ)) {
    return {
      _tag: "SequenceGap",
      expectedSeq: Number(response.headers.get(PRODUCER_EXPECTED_SEQ) ?? "0"),
      receivedSeq: Number(response.headers.get(PRODUCER_RECEIVED_SEQ) ?? "0"),
    }
  }
  if (response.status === 409 && finalOffset !== null) {
    return { _tag: "WriteToClosed", finalOffset: finalOffset as never }
  }
  return problem(response.status, response.statusText)
}

export const makeHttpChannel = (options: HttpChannelOptions): DurableStreamsChannel => {
  const fetchImpl = options.fetch ?? fetch
  return {
    create: (command: CreateStreamCommand) =>
      fetchEffect(fetchImpl, requestUrl(options.baseUrl, command.path), requestInit({
        method: "PUT",
        headers: {
          "content-type": command.contentType,
          ...(command.closed === true && { "stream-closed": "true" }),
        },
      }, bodyInit(command.body))).pipe(Effect.map((response) => parseCreate(response, command))),

    append: (command: AppendStreamCommand) =>
      fetchEffect(fetchImpl, requestUrl(options.baseUrl, command.path), requestInit({
        method: "POST",
        headers: {
          "content-type": command.contentType,
          ...(command.close === true && { "stream-closed": "true" }),
          ...(command.producer !== undefined && {
            [PRODUCER_ID]: command.producer.producerId,
            [PRODUCER_EPOCH]: String(command.producer.epoch),
            [PRODUCER_SEQ]: String(command.producer.seq),
          }),
        },
      }, bodyInit(command.body))).pipe(Effect.map((response) => parseAppend(response, command))),

    head: (path) =>
      fetchEffect(fetchImpl, requestUrl(options.baseUrl, path), { method: "HEAD" }).pipe(
        Effect.map((response): HeadStreamOutcome => {
          if (!response.ok) {
            return problem(response.status, response.statusText)
          }
          const tailOffset = response.headers.get(STREAM_NEXT_OFFSET)
          if (tailOffset === null) {
            return problem(500, "missing stream-next-offset")
          }
          return {
            _tag: "Head",
            metadata: {
              path,
              tailOffset: tailOffset as never,
              contentType: response.headers.get("content-type") ?? "application/octet-stream",
              closed: response.headers.get(STREAM_CLOSED) === "true",
            },
          }
        }),
      ),

    read: (command: ReadStreamCommand) =>
      fetchRead(fetchImpl, options.baseUrl, command).pipe(
        Effect.map((read): ReadStreamOutcome =>
          mapHttpRead(read, (httpRead) => ({
            _tag: "Read",
            records: httpRead.bytes.length === 0
              ? []
              : [{
                path: command.path,
                contentType: httpRead.contentType,
                bytes: httpRead.bytes,
                fromOffset: (command.offset ?? "-1") as never,
                nextOffset: httpRead.nextOffset,
                closed: httpRead.closed,
              }],
            ...readControl(httpRead),
          })),
        ),
      ),

    readJson: (command: ReadStreamCommand) =>
      fetchRead(fetchImpl, options.baseUrl, command).pipe(
        Effect.map((read): ReadStreamOutcome =>
          mapHttpRead(read, (httpRead) => {
            const decoded = httpRead.bytes.length === 0
              ? []
              : maybeJson(httpRead.contentType, httpRead.bytes)
            return {
              _tag: "ReadJson",
              items: Array.isArray(decoded) ? decoded : [decoded],
              ...readControl(httpRead),
            }
          }),
        ),
      ),

    follow: (command) =>
      Effect.succeed(
        Stream.fromEffect(Effect.succeed<StreamEvent>({
          _tag: "CaughtUp",
          offset: (command.offset ?? "-1") as never,
        })),
      ),

    delete: (path) =>
      fetchEffect(fetchImpl, requestUrl(options.baseUrl, path), { method: "DELETE" }).pipe(
        Effect.map((response): DeleteStreamOutcome =>
          response.status === 204
            ? { _tag: "Deleted", path }
            : problem(response.status, response.statusText),
        ),
      ),
  }
}
