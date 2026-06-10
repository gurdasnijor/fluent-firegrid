using Http;

namespace DurableStreams;

@doc("Opaque stream offset emitted by the server.")
@minLength(1)
scalar Offset extends string;

@doc("Stream-root-relative path. The protocol treats this as an application path, not an absolute URL.")
@minLength(1)
scalar StreamRootRelativePath extends string;

@doc("RFC 3339 date-time string.")
@format("date-time")
scalar Rfc3339DateTime extends string;

@doc("Absolute HTTP or HTTPS URL.")
@format("uri")
scalar AbsoluteUrl extends string;

@doc("HTTP media type, such as application/json or application/octet-stream.")
@minLength(1)
scalar MediaType extends string;

@doc("Base64url-encoded bytes used for non-JSON scheduled append bodies.")
@minLength(1)
@pattern("^[A-Za-z0-9_-]+={0,2}$")
scalar Base64UrlBytes extends string;

@doc("Subscription identifier.")
@minLength(1)
scalar SubscriptionId extends string;

@doc("Schedule identifier.")
@minLength(1)
scalar ScheduleId extends string;

@doc("Durable wake identifier.")
@minLength(1)
scalar WakeId extends string;

@doc("Pull-wake worker identifier.")
@minLength(1)
scalar WorkerId extends string;

@doc("Common Expression Language expression.")
@minLength(1)
scalar CelExpression extends string;

@doc("Opaque writer sequence for Stream-Seq. Values are compared byte-wise lexicographically by the server.")
@minLength(1)
scalar StreamSeq extends string;

@doc("Optional long-poll timeout. Servers define the accepted duration syntax and limits.")
@minLength(1)
scalar WaitTimeout extends string;

@doc("Non-empty producer identifier used for idempotent appends.")
@minLength(1)
scalar ProducerId extends string;

@doc("Monotonic producer epoch.")
@minValue(0)
scalar ProducerEpoch extends safeint;

@doc("Zero-based producer sequence number.")
@minValue(0)
scalar ProducerSeq extends safeint;

@doc("Pull-wake lease generation.")
@minValue(0)
scalar LeaseGeneration extends safeint;

@doc("Lease duration in milliseconds. Protocol-valid values are from 1 second to 10 minutes.")
@minValue(1000)
@maxValue(600000)
scalar LeaseTtlMs extends int32;

@doc("Stream TTL in seconds. Decimal notation only: no leading zeroes except zero itself, no sign, decimals, or scientific notation.")
@pattern("^(0|[1-9][0-9]*)$")
scalar StreamTtlSeconds extends string;

@doc("Non-negative fork sub-offset refining Stream-Fork-Offset.")
@minValue(0)
scalar StreamForkSubOffset extends safeint;

alias JsonValue = unknown;

model StreamUrlPathParam {
  @doc("Stream-root-relative path. This value may contain slash-separated path segments. TypeSpec models the route with `{+path}` so implementations preserve slashes; OpenAPI 3.1 renders this as `{path}` and cannot fully express reserved expansion.")
  @path
  path: StreamRootRelativePath;
}

model ControlPathParam {
  @doc("URL-encoded stream-root-relative path used inside reserved control APIs. TypeSpec models the route with `{+path}` so implementations preserve slashes; OpenAPI 3.1 renders this as `{path}` and cannot fully express reserved expansion.")
  @path
  path: StreamRootRelativePath;
}

model SubscriptionIdParam {
  @path id: SubscriptionId;
}

model ScheduleIdParam {
  @path id: ScheduleId;
}

model ContentTypeHeader {
  @header("Content-Type")
  contentType: string;
}

model OptionalContentTypeHeader {
  @doc("Request Content-Type. Required when a create or append request carries a non-empty body. Optional for close-only requests and fork creation where protocol-specific inheritance rules apply.")
  @header("Content-Type")
  contentType?: string;
}

model StreamClosedRequestHeader {
  @header("Stream-Closed")
  streamClosed?: "true";
}

model StreamCreateHeaders {
  ...OptionalContentTypeHeader;

  @header("Stream-TTL")
  streamTtl?: StreamTtlSeconds;

  @header("Stream-Expires-At")
  streamExpiresAt?: Rfc3339DateTime;

  ...StreamClosedRequestHeader;

  @header("Stream-Forked-From")
  streamForkedFrom?: StreamRootRelativePath;

  @header("Stream-Fork-Offset")
  streamForkOffset?: Offset;

  @header("Stream-Fork-Sub-Offset")
  streamForkSubOffset?: StreamForkSubOffset;
}

model StreamAppendHeaders {
  ...OptionalContentTypeHeader;

  @header("Stream-Seq")
  streamSeq?: StreamSeq;

  ...StreamClosedRequestHeader;

  @header("Producer-Id")
  producerId?: ProducerId;

  @header("Producer-Epoch")
  producerEpoch?: ProducerEpoch;

  @header("Producer-Seq")
  producerSeq?: ProducerSeq;
}

model TailHeaders {
  @header("Stream-Next-Offset")
  streamNextOffset: Offset;

  @header("Stream-Closed")
  streamClosed?: "true";
}

model ReadMetadataHeaders {
  @header("Cache-Control")
  cacheControl: string;

  @header("ETag")
  eTag?: string;

  @header("Stream-Cursor")
  streamCursor?: string;

  @header("Stream-Next-Offset")
  streamNextOffset: Offset;

  @header("Stream-Up-To-Date")
  streamUpToDate?: "true";

  @header("Stream-Closed")
  streamClosed?: "true";

  @header("X-Content-Type-Options")
  xContentTypeOptions?: "nosniff";
}

model LiveNoContentHeaders {
  @header("Stream-Next-Offset")
  streamNextOffset: Offset;

  @header("Stream-Up-To-Date")
  streamUpToDate: "true";

  @header("Stream-Cursor")
  streamCursor?: string;

  @header("Stream-Closed")
  streamClosed?: "true";
}

model ProducerSuccessHeaders {
  @header("Stream-Next-Offset")
  streamNextOffset: Offset;

  @header("Stream-Closed")
  streamClosed?: "true";

  @header("Producer-Epoch")
  producerEpoch: ProducerEpoch;

  @header("Producer-Seq")
  producerSeq: ProducerSeq;
}

model ProducerFencedHeaders {
  @header("Producer-Epoch")
  producerEpoch: ProducerEpoch;
}

model ProducerGapHeaders {
  @header("Producer-Expected-Seq")
  producerExpectedSeq: ProducerSeq;

  @header("Producer-Received-Seq")
  producerReceivedSeq: ProducerSeq;
}

model ClosedConflictHeaders {
  @header("Stream-Closed")
  streamClosed: "true";

  @header("Stream-Next-Offset")
  streamNextOffset: Offset;
}

model EmptyError {
  code?: string;
  message?: string;
}

@error
@discriminator("code")
@doc("Protocol error body used directly by stream endpoints. Reserved control endpoints wrap some fencing errors in `{ error: ... }` where PROTOCOL.md requires that envelope.")
model ProtocolError {
  code: string;
  message?: string;
}

@error
@example(
  #{ code: "BAD_REQUEST", message: "offset must be -1, now, or a stream offset" },
  #{ title: "Invalid offset" }
)
model BadRequestError extends ProtocolError {
  code: "BAD_REQUEST";
  message: string;
}

@error
@example(#{ code: "CONFLICT", message: "stream is closed" }, #{ title: "Conflict" })
model ConflictError extends ProtocolError {
  code: "CONFLICT";
  message?: string;
}

@error
@example(#{ code: "NOT_FOUND", message: "stream not found" }, #{ title: "Missing stream" })
model NotFoundError extends ProtocolError {
  code: "NOT_FOUND";
  message?: string;
}

@error
@example(#{ code: "GONE", message: "stream was deleted" }, #{ title: "Deleted stream" })
model GoneError extends ProtocolError {
  code: "GONE";
  message?: string;
}

@error
@example(#{ code: "TOO_MANY_REQUESTS", message: "retry later" }, #{ title: "Rate limited" })
model TooManyRequestsError extends ProtocolError {
  code: "TOO_MANY_REQUESTS";
  message?: string;
}

@error
@example(#{ code: "FENCED" }, #{ title: "Stale generation" })
model FencedError extends ProtocolError {
  code: "FENCED";
}

@error
@example(
  #{ code: "ALREADY_CLAIMED", currentHolder: "worker-a", generation: 4 },
  #{ title: "Claim held by another worker" }
)
model AlreadyClaimedError extends ProtocolError {
  code: "ALREADY_CLAIMED";

  @encodedName("application/json", "current_holder")
  currentHolder: string;

  generation: LeaseGeneration;
}

model BadRequest {
  ...BadRequestResponse;
  ...Body<BadRequestError>;
}

model NotFound {
  ...NotFoundResponse;
  ...Body<NotFoundError>;
}

model Conflict {
  ...ConflictResponse;
  ...Body<ConflictError>;
}

model Gone {
  ...Response<410>;
  ...Body<GoneError>;
}

model TooManyRequests {
  ...Response<429>;
  ...Body<TooManyRequestsError>;
}

model MethodNotAllowed {
  ...Response<405>;
  ...Body<EmptyError>;
}

model NotImplemented {
  ...Response<501>;
  ...Body<EmptyError>;
}

model PayloadTooLarge {
  ...Response<413>;
  ...Body<EmptyError>;
}

model Unauthorized {
  ...UnauthorizedResponse;
  ...Body<EmptyError>;
}

model Forbidden {
  ...ForbiddenResponse;
  ...Body<EmptyError>;
}

alias CommonStreamErrors = BadRequest | NotFound | Gone | TooManyRequests;

@doc("JSON error envelope used by reserved control APIs for fenced or claim-conflict responses. Stream APIs generally return bare protocol errors or header-only conflicts.")
model ErrorEnvelope<TError> {
  error: TError;
}
