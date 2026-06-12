import { Schema } from "effect"
import { HttpApi, HttpApiEndpoint, HttpApiGroup, HttpApiSchema } from "effect/unstable/httpapi"
import {
  PRODUCER_EPOCH,
  PRODUCER_ID,
  PRODUCER_SEQ,
  STREAM_CLOSED,
  STREAM_EXPIRES_AT,
  STREAM_FORKED_FROM,
  STREAM_FORK_OFFSET,
  STREAM_FORK_SUB_OFFSET,
  STREAM_SEQ,
  STREAM_TTL,
} from "./httpShared.ts"

const RawStreamResponse = Schema.Void.pipe(HttpApiSchema.status(204))
const StreamSseResponse = HttpApiSchema.StreamSse({
  data: Schema.Unknown,
})
const OptionalHeader = Schema.optional(Schema.String)

export const CreateStreamHeaders = Schema.Struct({
  "content-type": OptionalHeader,
  [STREAM_CLOSED]: OptionalHeader,
  [STREAM_TTL]: OptionalHeader,
  [STREAM_EXPIRES_AT]: OptionalHeader,
  [STREAM_FORKED_FROM]: OptionalHeader,
  [STREAM_FORK_OFFSET]: OptionalHeader,
  [STREAM_FORK_SUB_OFFSET]: OptionalHeader,
})

export const AppendStreamHeaders = Schema.Struct({
  "content-type": OptionalHeader,
  [STREAM_CLOSED]: OptionalHeader,
  [STREAM_SEQ]: OptionalHeader,
  [PRODUCER_ID]: OptionalHeader,
  [PRODUCER_EPOCH]: OptionalHeader,
  [PRODUCER_SEQ]: OptionalHeader,
})

export const ReadStreamQuery = Schema.Struct({
  offset: OptionalHeader,
  live: OptionalHeader,
  "chunk-size": OptionalHeader,
  cursor: OptionalHeader,
})

export const DurableStreamsHttpApi = HttpApi.make("DurableStreams").add(
  HttpApiGroup.make("Streams")
    .add(
      HttpApiEndpoint.put("createStream", "/*", {
        disableCodecs: true,
        headers: CreateStreamHeaders,
        success: RawStreamResponse,
      }),
    )
    .add(
      HttpApiEndpoint.post("appendStream", "/*", {
        disableCodecs: true,
        headers: AppendStreamHeaders,
        success: RawStreamResponse,
      }),
    )
    .add(
      HttpApiEndpoint.get("readStream", "/*", {
        disableCodecs: true,
        query: ReadStreamQuery,
        success: [RawStreamResponse, StreamSseResponse],
      }),
    )
    .add(
      HttpApiEndpoint.head("headStream", "/*", {
        disableCodecs: true,
        success: RawStreamResponse,
      }),
    )
    .add(
      HttpApiEndpoint.delete("deleteStream", "/*", {
        disableCodecs: true,
        success: RawStreamResponse,
      }),
    ),
)

const methods = new Set<string>()

HttpApi.reflect(DurableStreamsHttpApi, {
  onGroup: () => undefined,
  onEndpoint: ({ endpoint }) => {
    methods.add(endpoint.method)
  },
})

export const durableStreamsHttpMethods: ReadonlySet<string> = methods
