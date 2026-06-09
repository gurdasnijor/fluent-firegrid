import {
  HttpApiBuilder,
  HttpApiError,
  HttpServerResponse,
} from "@effect/platform"
import { Effect, Layer, Option } from "effect"
import * as Api from "./Api.ts"
import * as Store from "./Store.ts"
import type { HttpServerRequest } from "@effect/platform"
import type * as Protocol from "./Protocol.ts"
import type * as ProtocolError from "./ProtocolError.ts"

const STREAM_NEXT_OFFSET = "Stream-Next-Offset"
const STREAM_UP_TO_DATE = "Stream-Up-To-Date"
const STREAM_CLOSED = "Stream-Closed"
const PRODUCER_EPOCH = "Producer-Epoch"
const PRODUCER_SEQ = "Producer-Seq"
const PRODUCER_EXPECTED_SEQ = "Producer-Expected-Seq"
const PRODUCER_RECEIVED_SEQ = "Producer-Received-Seq"

type ProducerSuccessDecision = Extract<
  Protocol.AppendDecision,
  { readonly _tag: "ProducerAccepted" | "ProducerDuplicate" }
>

const normalizeContentType = (contentType: string): string =>
  contentType.split(";")[0]!.trim().toLowerCase()

const isClosed = (value: string | undefined): boolean =>
  (value ?? "").toLowerCase() === "true"

const producerSuccessResponse = (
  status: 200 | 204,
  decision: ProducerSuccessDecision,
) =>
  HttpServerResponse.empty({ status }).pipe(
    HttpServerResponse.setHeaders({
      [STREAM_NEXT_OFFSET]: decision.nextOffset,
      [PRODUCER_EPOCH]: String(decision.producerEpoch),
      [PRODUCER_SEQ]: String(decision.highestAcceptedSeq),
      ...(decision.closed ? { [STREAM_CLOSED]: "true" } : {}),
    }),
  )

const applicationStreamPath = (
  path: Protocol.StreamPath,
): Effect.Effect<Protocol.StreamPath, HttpApiError.NotFound> =>
  path === "__ds" || path.startsWith("__ds/")
    ? Effect.fail(new HttpApiError.NotFound())
    : Effect.succeed(path)

const requestBody = (
  request: HttpServerRequest.HttpServerRequest,
): Effect.Effect<Uint8Array> =>
  // Durable Streams stores exact bytes for arbitrary content types. The
  // platform payload decoder pre-parses JSON/text/form bodies before Schema
  // decode, so the HTTP adapter reads the raw body only at this boundary.
  request.arrayBuffer.pipe(Effect.orDie, Effect.map((buffer) => new Uint8Array(buffer)))

const appendProducer = (
  headers: typeof Api.AppendHeaders.Type,
): Effect.Effect<Option.Option<Protocol.IdempotentProducer>, HttpApiError.BadRequest> => {
  const producerId = headers[Api.PRODUCER_ID]
  const producerEpoch = headers[Api.PRODUCER_EPOCH_REQUEST]
  const producerSeq = headers[Api.PRODUCER_SEQ_REQUEST]
  const hasProducer =
    producerId !== undefined && producerEpoch !== undefined && producerSeq !== undefined
  const hasPartialProducer =
    producerId !== undefined || producerEpoch !== undefined || producerSeq !== undefined

  if (!hasProducer && hasPartialProducer) {
    return Effect.fail(new HttpApiError.BadRequest())
  }

  return Effect.succeed(
    hasProducer
      ? Option.some({ id: producerId, epoch: producerEpoch, seq: producerSeq })
      : Option.none(),
  )
}

const lowerStoreError = <A, R>(
  effect: Effect.Effect<A, ProtocolError.ProtocolError, R>,
): Effect.Effect<
  A,
  | HttpApiError.BadRequest
  | HttpApiError.NotFound
  | HttpApiError.Conflict
  | HttpApiError.Gone,
  R
> =>
  effect.pipe(
    Effect.catchTags({
      BadRequest: () => Effect.fail(new HttpApiError.BadRequest()),
      CreateConflict: () => Effect.fail(new HttpApiError.Conflict()),
      NotFound: () => Effect.fail(new HttpApiError.NotFound()),
      RetentionGone: () => Effect.fail(new HttpApiError.Gone()),
    }),
  )

export const StreamApiLive = HttpApiBuilder.group(
  Api.DurableStreamsApi,
  "streams",
  (handlers) =>
    Effect.gen(function* () {
      const store = yield* Store.Store

      return handlers
        .handle("createStream", ({ path, headers, request }) =>
          Effect.gen(function* () {
            const streamPath = yield* applicationStreamPath(path.streamPath)
            const body = yield* requestBody(request)
            const req: Protocol.CreateRequest = {
              path: streamPath,
              contentType: normalizeContentType(headers[Api.CONTENT_TYPE]),
              entityBody: body,
              close: isClosed(headers[Api.STREAM_CLOSED_REQUEST]),
            }
            const decision = yield* lowerStoreError(store.createStream(req))
            return HttpServerResponse.empty({
              status: decision._tag === "Created" ? 201 : 200,
            }).pipe(
              HttpServerResponse.setHeaders({
                [STREAM_NEXT_OFFSET]: decision.tailOffset,
                ...(req.contentType ? { [Api.CONTENT_TYPE]: req.contentType } : {}),
                ...(decision.closed ? { [STREAM_CLOSED]: "true" } : {}),
              }),
            )
          }),
        )
        .handle("appendToStream", ({ path, headers, request }) =>
          Effect.gen(function* () {
            const streamPath = yield* applicationStreamPath(path.streamPath)
            const body = yield* requestBody(request)
            const producer = yield* appendProducer(headers)
            const req: Protocol.AppendRequest = {
              path: streamPath,
              contentType: normalizeContentType(headers[Api.CONTENT_TYPE]),
              entityBody: body,
              close: isClosed(headers[Api.STREAM_CLOSED_REQUEST]),
              streamSeq: Option.fromNullable(headers[Api.STREAM_SEQ]),
              idempotentProducer: producer,
            }
            const result = yield* lowerStoreError(store.append(req))
            switch (result.append._tag) {
              case "PlainAccepted":
                return HttpServerResponse.empty({ status: 204 }).pipe(
                  HttpServerResponse.setHeaders({
                    [STREAM_NEXT_OFFSET]: result.append.nextOffset,
                    ...(result.append.closed ? { [STREAM_CLOSED]: "true" } : {}),
                  }),
                )
              case "ProducerAccepted":
                return producerSuccessResponse(200, result.append)
              case "ProducerDuplicate":
                return producerSuccessResponse(204, result.append)
              case "ProducerFenced":
                return HttpServerResponse.empty({ status: 403 }).pipe(
                  HttpServerResponse.setHeader(
                    PRODUCER_EPOCH,
                    String(result.append.currentEpoch),
                  ),
                )
              case "ProducerGap":
                return result.append.expectedSeq === 0
                  ? HttpServerResponse.empty({ status: 400 })
                  : HttpServerResponse.empty({ status: 409 }).pipe(
                      HttpServerResponse.setHeaders({
                        [PRODUCER_EXPECTED_SEQ]: String(result.append.expectedSeq),
                        [PRODUCER_RECEIVED_SEQ]: String(result.append.receivedSeq),
                      }),
                    )
              case "ClosedConflict":
                return HttpServerResponse.empty({ status: 409 }).pipe(
                  HttpServerResponse.setHeaders({
                    [STREAM_CLOSED]: "true",
                    [STREAM_NEXT_OFFSET]: result.append.finalOffset,
                  }),
                )
              case "ContentTypeMismatch":
              case "StreamSeqRegression":
                return HttpServerResponse.empty({ status: 409 })
            }
          }),
        )
        .handle("headStream", ({ path }) =>
          applicationStreamPath(path.streamPath).pipe(
            Effect.flatMap((streamPath) => lowerStoreError(store.head(streamPath))),
            Effect.map((tail) =>
              HttpServerResponse.empty({ status: 200 }).pipe(
                HttpServerResponse.setHeaders({
                  [STREAM_NEXT_OFFSET]: tail.tailOffset,
                  ...(tail.contentType ? { [Api.CONTENT_TYPE]: tail.contentType } : {}),
                  ...(tail.closed ? { [STREAM_CLOSED]: "true" } : {}),
                }),
              ),
            ),
          ),
        )
        .handle("readStream", ({ path, urlParams }) =>
          applicationStreamPath(path.streamPath).pipe(
            Effect.flatMap((streamPath) =>
              lowerStoreError(store.read(streamPath, urlParams.offset)),
            ),
            Effect.map((chunk) =>
              HttpServerResponse.uint8Array(chunk.entityBody, {
                contentType: chunk.contentType || "application/octet-stream",
              }).pipe(
                HttpServerResponse.setHeaders({
                  [STREAM_NEXT_OFFSET]: chunk.nextOffset,
                  ...(chunk.upToDate ? { [STREAM_UP_TO_DATE]: "true" } : {}),
                  ...(chunk.closed ? { [STREAM_CLOSED]: "true" } : {}),
                  etag: `"${urlParams.offset}:${chunk.nextOffset}:${chunk.closed ? "c" : "o"}"`,
                  "cache-control": "no-store",
                  "x-content-type-options": "nosniff",
                }),
              ),
            ),
          ),
        )
        .handle("deleteStream", ({ path }) =>
          applicationStreamPath(path.streamPath).pipe(
            Effect.flatMap((streamPath) => lowerStoreError(store.deleteStream(streamPath))),
            Effect.asVoid,
          ),
        )
    }),
)

export const layer = HttpApiBuilder.api(Api.DurableStreamsApi).pipe(
  Layer.provide(StreamApiLive),
)
