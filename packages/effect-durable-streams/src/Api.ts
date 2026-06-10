import {
  HttpApi,
  HttpApiEndpoint,
  HttpApiError,
  HttpApiGroup,
  HttpApiSchema,
  OpenApi,
} from "@effect/platform"
import { Schema } from "effect"
import * as Protocol from "./Protocol.ts"

export const CONTENT_TYPE = "content-type"
export const STREAM_SEQ = "stream-seq"
export const STREAM_CLOSED_REQUEST = "stream-closed"
export const PRODUCER_ID = "producer-id"
export const PRODUCER_EPOCH_REQUEST = "producer-epoch"
export const PRODUCER_SEQ_REQUEST = "producer-seq"

const RawStreamPathParams = Schema.Struct({
  "*": Schema.optionalWith(Schema.String, { default: () => "" }),
})

export const StreamPathParams = Schema.transform(
  RawStreamPathParams,
  Schema.Struct({ streamPath: Protocol.StreamPath }),
  {
    decode: (params) => ({ streamPath: params["*"] }),
    encode: ({ streamPath }) => ({ "*": streamPath }),
  },
)

export const ReadParams = Schema.Struct({
  offset: Schema.optionalWith(Schema.String, { default: () => "-1" }),
})

export const CreateHeaders = Schema.Struct({
  [CONTENT_TYPE]: Schema.optionalWith(Schema.String, { default: () => "" }),
  [STREAM_CLOSED_REQUEST]: Schema.optional(Schema.String),
})

export const AppendHeaders = Schema.Struct({
  [CONTENT_TYPE]: Schema.optionalWith(Schema.String, { default: () => "" }),
  [STREAM_CLOSED_REQUEST]: Schema.optional(Schema.String),
  [STREAM_SEQ]: Schema.optional(Schema.String),
  [PRODUCER_ID]: Schema.optional(Schema.String),
  [PRODUCER_EPOCH_REQUEST]: Schema.optional(Protocol.UintFromString),
  [PRODUCER_SEQ_REQUEST]: Schema.optional(Protocol.UintFromString),
})

const streamEndpoint = <const Name extends string>(
  name: Name,
  method: "GET" | "HEAD" | "POST" | "PUT" | "DELETE",
) => HttpApiEndpoint.make(method)(name, "/*").setPath(StreamPathParams)

export class StreamApi extends HttpApiGroup.make("streams")
  .add(
    streamEndpoint("createStream", "PUT")
      .setHeaders(CreateHeaders)
      .addSuccess(HttpApiSchema.Created)
      .addSuccess(HttpApiSchema.Empty(200)),
  )
  .add(
    streamEndpoint("appendToStream", "POST")
      .setHeaders(AppendHeaders)
      .addSuccess(HttpApiSchema.NoContent)
      .addSuccess(HttpApiSchema.Empty(200)),
  )
  .add(
    streamEndpoint("headStream", "HEAD").addSuccess(HttpApiSchema.Empty(200)),
  )
  .add(
    streamEndpoint("readStream", "GET")
      .setUrlParams(ReadParams)
      .addSuccess(HttpApiSchema.Uint8Array()),
  )
  .add(
    streamEndpoint("deleteStream", "DELETE").addSuccess(HttpApiSchema.NoContent),
  )
  .addError(HttpApiError.BadRequest)
  .addError(HttpApiError.Forbidden)
  .addError(HttpApiError.NotFound)
  .addError(HttpApiError.Conflict)
  .addError(HttpApiError.Gone)
  .prefix("/v1/stream")
  .annotateContext(
    OpenApi.annotations({
      title: "Durable Streams data plane",
      description: "PROTOCOL.md stream create, append, head, read, and delete operations.",
    }),
  )
{}

export class DurableStreamsApi extends HttpApi.make("DurableStreamsApi")
  .add(StreamApi)
{}
