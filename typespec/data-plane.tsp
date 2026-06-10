using Http;

namespace DurableStreams;

@doc("Create or confirm a stream. If `body` is present, `Content-Type` is required. Fork creation may omit `Content-Type` to inherit the source stream content type, but a provided value must match the source.")
model CreateStreamRequest {
  ...StreamUrlPathParam;
  ...StreamCreateHeaders;

  @doc("Initial stream bytes. Empty-body create is allowed, including create-and-close with `Stream-Closed: true`.")
  @body body?: bytes;
}

model CreateStreamCreated {
  ...CreatedResponse;

  @header("Location")
  location?: string;

  ...TailHeaders;
}

model CreateStreamAlreadyExists {
  ...OkResponse;
  ...TailHeaders;
}

@doc("Append bytes to a stream. If `body` is present, `Content-Type` is required. Empty body is only meaningful for close-only requests with `Stream-Closed: true`.")
model AppendStreamRequest {
  ...StreamUrlPathParam;
  ...StreamAppendHeaders;

  @doc("Bytes to append. Servers reject empty append bodies unless the request also closes the stream.")
  @body body?: bytes;
}

@doc("No-content append result. Producer headers are present when this is an idempotent producer duplicate; otherwise this is a plain append or close-only success.")
model AppendNoContent {
  ...NoContentResponse;

  @header("Stream-Next-Offset")
  streamNextOffset: Offset;

  @header("Stream-Closed")
  streamClosed?: "true";

  @header("Producer-Epoch")
  producerEpoch?: ProducerEpoch;

  @header("Producer-Seq")
  producerSeq?: ProducerSeq;
}

model ProducerAppendAccepted {
  ...OkResponse;
  ...ProducerSuccessHeaders;
}

model ProducerFenced {
  ...ForbiddenResponse;
  ...ProducerFencedHeaders;
}

model ProducerGap {
  ...ConflictResponse;
  ...ProducerGapHeaders;
}

@doc("Header-only closed-stream conflict. Clients should use `Stream-Closed` and `Stream-Next-Offset` instead of parsing a body.")
model ClosedStreamConflict {
  ...ConflictResponse;
  ...ClosedConflictHeaders;
}

model HeadStreamOk {
  ...OkResponse;
  ...TailHeaders;

  @header("Stream-TTL")
  streamTtl?: string;

  @header("Stream-Expires-At")
  streamExpiresAt?: Rfc3339DateTime;

  @header("Cache-Control")
  cacheControl?: string;
}

@TypeSpec.Streams.streamOf(bytes)
model ReadStreamOk {
  ...OkResponse;
  ...ReadHeaders;
  @body body: bytes;
}

model ReadStreamNoContent {
  ...NoContentResponse;
  ...LiveNoContentHeaders;
}

model ReadStreamNotModified {
  ...NotModifiedResponse;
}

model StreamSseControl {
  streamNextOffset: Offset;
  streamCursor?: string;
  upToDate?: true;
  streamClosed?: true;
}

@TypeSpec.Events.events
@summary("SSE event variants emitted while following a stream.")
union StreamSseEvents {
  data: string,
  control: StreamSseControl,
}

model SseStreamOk {
  ...OkResponse;

  @header("Stream-SSE-Data-Encoding")
  streamSseDataEncoding?: "base64";

  ...TypeSpec.SSE.SSEStream<StreamSseEvents>;
}

@route("/v1/stream/{+path}")
@tag("Streams")
interface Streams {
  @doc("Create a stream, create a fork, or idempotently confirm an existing stream.")
  @summary("Create or confirm a stream")
  @returnsDoc("Create or confirm result, or a documented protocol error.")
  @errorsDoc("The request is invalid, conflicts with an existing stream, references a missing fork source, or is rate limited.")
  @opExample(
    #{ parameters: #{ path: "events/orders", contentType: "application/json" } },
    #{ title: "Create an appendable JSON stream" }
  )
  @put
  createStream(...CreateStreamRequest):
    | CreateStreamCreated
    | CreateStreamAlreadyExists
    | BadRequest
    | Conflict
    | NotFound
    | TooManyRequests;

  @doc("Append bytes to a stream and optionally close it atomically.")
  @summary("Append bytes to a stream")
  @returnsDoc("Append result, including idempotent producer metadata when supplied, or a documented protocol error.")
  @errorsDoc("The append is invalid, references a missing or closed stream, violates producer sequencing, or is unsupported.")
  @opExample(
    #{
      parameters: #{
        path: "events/orders",
        contentType: "application/json",
        producerId: "orders-writer",
        producerEpoch: 1,
        producerSeq: 0,
      },
    },
    #{ title: "Append with producer fencing" }
  )
  @post
  appendToStream(...AppendStreamRequest):
    | AppendNoContent
    | ProducerAppendAccepted
    | ProducerFenced
    | ProducerGap
    | ClosedStreamConflict
    | BadRequest
    | NotFound
    | Gone
    | MethodNotAllowed
    | NotImplemented
    | PayloadTooLarge
    | TooManyRequests;

  @doc("Read stream metadata without transferring body bytes.")
  @summary("Read stream metadata")
  @returnsDoc("Stream metadata result or a documented protocol error.")
  @errorsDoc("The stream is missing, deleted, or currently rate limited.")
  @head
  headStream(...StreamUrlPathParam):
    | HeadStreamOk
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Read catch-up bytes, long-poll for new bytes, or stream SSE events.")
  @summary("Read or follow a stream")
  @returnsDoc("Read, long-poll, SSE, cache validation, or documented protocol error result.")
  @errorsDoc("The request is invalid, the stream is missing or deleted, or the server is rate limiting.")
  @opExample(
    #{ parameters: #{ path: "events/orders", offset: "-1" } },
    #{ title: "Initial catch-up read" }
  )
  @opExample(
    #{ parameters: #{ path: "events/orders", offset: "128", live: "sse" } },
    #{ title: "Follow using SSE" }
  )
  @get
  readStream(
    ...StreamUrlPathParam,
    @query offset?: Offset | "-1" | "now",
    @query live?: "long-poll" | "sse",
    @query cursor?: string,
    @query timeout?: WaitTimeout,
    @header("If-None-Match") ifNoneMatch?: string,
  ):
    | ReadStreamOk
    | ReadStreamNoContent
    | ReadStreamNotModified
    | SseStreamOk
    | CommonStreamErrors;

  @doc("Delete a stream. Streams with active forks become soft-deleted.")
  @summary("Delete a stream")
  @returnsDoc("Delete result or a documented protocol error.")
  @errorsDoc("The stream is missing, already deleted, or deletion is unsupported.")
  @delete
  deleteStream(...StreamUrlPathParam):
    | NoContentResponse
    | NotFound
    | Gone
    | MethodNotAllowed
    | NotImplemented;
}
