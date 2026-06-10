import "@typespec/http";
import "@typespec/streams";
import "@typespec/sse";
import "@typespec/events";
import "@typespec/rest";

using Http;

@service(#{ title: "Durable Streams Protocol" })
@summary("Path-addressed durable byte streams with long-polling, SSE, subscriptions, and scheduled appends.")
@server(
  "{endpoint}",
  "Durable Streams server root",
  {
    endpoint: "https://example.com",
  }
)
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

@doc("Lease duration in milliseconds.")
@minValue(1)
scalar LeaseTtlMs extends int32;

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
  streamTtl?: string;

  @header("Stream-Expires-At")
  streamExpiresAt?: Rfc3339DateTime;

  ...StreamClosedRequestHeader;

  @header("Stream-Forked-From")
  streamForkedFrom?: StreamRootRelativePath;

  @header("Stream-Fork-Offset")
  streamForkOffset?: Offset;

  @header("Stream-Fork-Sub-Offset")
  streamForkSubOffset?: int64;
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

model ReadHeaders {
  ...ContentTypeHeader;

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

enum SubscriptionType {
  webhook: "webhook",
  pullWake: "pull-wake",
}

enum SubscriptionStatus {
  active: "active",
  failed: "failed",
}

enum LinkType {
  glob: "glob",
  explicit: "explicit",
}

model WakeFilter {
  language: "cel";
  expression: CelExpression;
  self?: JsonValue;
}

@example(#{ url: "https://example.com/durable-streams/webhook" }, #{ title: "Webhook endpoint" })
model WebhookConfig {
  url: AbsoluteUrl;
}

model WebhookSigningMetadata {
  alg: "ed25519";
  kid: string;

  @encodedName("application/json", "jwks_url")
  jwksUrl: AbsoluteUrl;
}

model WebhookMetadata extends WebhookConfig {
  signing?: WebhookSigningMetadata;
}

@discriminator("type")
model SubscriptionConfigBase {
  type: SubscriptionType;

  @doc("Glob over stream-root-relative paths. A subscription request must provide at least one of `pattern` or `streams`.")
  pattern?: string;

  @doc("Explicit stream-root-relative paths. A subscription request must provide at least one of `pattern` or `streams`.")
  streams?: StreamRootRelativePath[];

  filter?: WakeFilter;

  @encodedName("application/json", "lease_ttl_ms")
  leaseTtlMs?: LeaseTtlMs;

  description?: string;
}

model WebhookSubscriptionConfig extends SubscriptionConfigBase {
  type: SubscriptionType.webhook;
  webhook: WebhookConfig;
}

model PullWakeSubscriptionConfig extends SubscriptionConfigBase {
  type: SubscriptionType.pullWake;

  @encodedName("application/json", "wake_stream")
  wakeStream: StreamRootRelativePath;
}

@summary("Subscription creation request keyed by subscription type.")
@example(
  #{
    type: SubscriptionType.webhook,
    pattern: "events/*",
    webhook: #{ url: "https://example.com/durable-streams/webhook" },
    filter: #{ language: "cel", expression: "message.type == 'order.created'" },
    leaseTtlMs: 30000,
    description: "Order events webhook",
  },
  #{ title: "Webhook subscription" }
)
union SubscriptionConfig {
  webhook: WebhookSubscriptionConfig,
  pullWake: PullWakeSubscriptionConfig,
}

model SubscriptionStreamLink {
  path: StreamRootRelativePath;

  @encodedName("application/json", "link_type")
  linkType: LinkType;

  @encodedName("application/json", "acked_offset")
  ackedOffset: Offset;
}

@TypeSpec.Rest.resource("subscriptions")
model Subscription {
  @key
  @visibility(Lifecycle.Read)
  id: SubscriptionId;

  @visibility(Lifecycle.Read)
  @encodedName("application/json", "subscription_id")
  subscriptionId: SubscriptionId;

  type: SubscriptionType;
  pattern?: string;
  streams: SubscriptionStreamLink[];
  webhook?: WebhookMetadata;

  @encodedName("application/json", "wake_stream")
  wakeStream?: StreamRootRelativePath | null;

  filter?: WakeFilter;

  @encodedName("application/json", "lease_ttl_ms")
  leaseTtlMs: LeaseTtlMs;

  @visibility(Lifecycle.Read)
  @encodedName("application/json", "created_at")
  createdAt: Rfc3339DateTime;

  @visibility(Lifecycle.Read)
  status: SubscriptionStatus;
  description?: string;
}

model SubscriptionCreated {
  ...CreatedResponse;
  ...Body<Subscription>;
}

model SubscriptionConfirmed {
  ...OkResponse;
  ...Body<Subscription>;
}

model SubscriptionResponse {
  ...OkResponse;
  ...Body<Subscription>;
}

model StreamMembershipRequest {
  streams: StreamRootRelativePath[];
}

model Ack {
  stream: StreamRootRelativePath;
  offset: Offset;
}

model PendingStream {
  path: StreamRootRelativePath;

  @encodedName("application/json", "link_type")
  linkType: LinkType;

  @encodedName("application/json", "acked_offset")
  ackedOffset: Offset;

  @encodedName("application/json", "tail_offset")
  tailOffset: Offset;

  @encodedName("application/json", "has_pending")
  hasPending: boolean;
}

model CallbackRequest {
  @encodedName("application/json", "wake_id")
  wakeId: WakeId;

  generation: LeaseGeneration;
  acks?: Ack[];
  done?: boolean;
}

model CallbackOk {
  ok: true;

  @encodedName("application/json", "next_wake")
  nextWake: boolean;
}

model CallbackOkResponse {
  ...OkResponse;
  ...Body<CallbackOk>;
}

model FencedConflict {
  ...ConflictResponse;
  ...Body<ErrorEnvelope<FencedError>>;
}

model ClaimRequest {
  worker: WorkerId;
}

model ClaimSuccess {
  @encodedName("application/json", "wake_id")
  wakeId: WakeId;

  generation: LeaseGeneration;

  @secret
  token: string;
  streams: PendingStream[];

  @encodedName("application/json", "lease_ttl_ms")
  leaseTtlMs: LeaseTtlMs;
}

model ClaimSuccessResponse {
  ...OkResponse;
  ...Body<ClaimSuccess>;
}

model AlreadyClaimedConflict {
  ...ConflictResponse;
  ...Body<ErrorEnvelope<AlreadyClaimedError>>;
}

model ReleaseRequest {
  @encodedName("application/json", "wake_id")
  wakeId: WakeId;

  generation: LeaseGeneration;
}

model JsonWebKey extends Record<JsonValue> {
  kty: string;
  crv?: string;
  kid: string;
  use?: "sig";
  alg?: string;
  x?: string;
}

model JsonWebKeySet {
  keys: JsonWebKey[];
}

model JwksResponse {
  ...OkResponse;

  @header("Content-Type")
  contentType: "application/jwk-set+json";

  @header("Cache-Control")
  cacheControl: string;

  @body body: JsonWebKeySet;
}

@route("/v1/stream/__ds")
@tag("Subscriptions")
interface Subscriptions {
  @doc("Create or idempotently re-confirm a subscription.")
  @summary("Create or confirm a subscription")
  @returnsDoc("Create or confirm result, or a documented protocol error.")
  @errorsDoc("The subscription request is invalid, conflicts with an existing subscription, or is rate limited.")
  @opExample(
    #{
      parameters: #{
        id: "sub-orders",
        contentType: "application/json",
        body: #{
          type: SubscriptionType.webhook,
          pattern: "events/*",
          webhook: #{ url: "https://example.com/durable-streams/webhook" },
          leaseTtlMs: 30000,
        },
      },
    },
    #{ title: "Create a webhook subscription" }
  )
  @TypeSpec.Rest.createsOrReplacesResource(Subscription)
  @put
  @route("/subscriptions/{id}")
  createSubscription(
    ...SubscriptionIdParam,
    @header("Content-Type") contentType: "application/json",
    @body body: SubscriptionConfig,
  ):
    | SubscriptionCreated
    | SubscriptionConfirmed
    | Conflict
    | BadRequest
    | TooManyRequests;

  @doc("Read subscription configuration, membership, cursor, and delivery status.")
  @summary("Get a subscription")
  @returnsDoc("Subscription state result or a documented protocol error.")
  @errorsDoc("The subscription is missing, tombstoned, or currently rate limited.")
  @TypeSpec.Rest.readsResource(Subscription)
  @get
  @route("/subscriptions/{id}")
  getSubscription(...SubscriptionIdParam):
    | SubscriptionResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Tombstone a subscription.")
  @summary("Delete a subscription")
  @returnsDoc("Delete result or a documented protocol error.")
  @errorsDoc("The subscription is missing, tombstoned, or currently rate limited.")
  @TypeSpec.Rest.deletesResource(Subscription)
  @delete
  @route("/subscriptions/{id}")
  deleteSubscription(...SubscriptionIdParam):
    | NoContentResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Add explicit stream links to a subscription.")
  @summary("Add subscription stream links")
  @returnsDoc("Stream membership update result or a documented protocol error.")
  @errorsDoc("The request is invalid, the subscription is missing or tombstoned, or the server is rate limiting.")
  @TypeSpec.Rest.collectionAction(Subscription, "streams")
  @post
  @route("/subscriptions/{id}/streams")
  addSubscriptionStreams(
    ...SubscriptionIdParam,
    @header("Content-Type") contentType: "application/json",
    @body body: StreamMembershipRequest,
  ): NoContentResponse | NotFound | Gone | BadRequest | TooManyRequests;

  @doc("Remove one explicit stream link from a subscription.")
  @summary("Remove a subscription stream link")
  @returnsDoc("Stream membership update result or a documented protocol error.")
  @errorsDoc("The subscription is missing, tombstoned, or currently rate limited.")
  @TypeSpec.Rest.collectionAction(Subscription, "streams")
  @delete
  @route("/subscriptions/{id}/streams/{+path}")
  removeSubscriptionStream(...SubscriptionIdParam, ...ControlPathParam):
    | NoContentResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Webhook asynchronous completion callback.")
  @summary("Complete webhook delivery")
  @returnsDoc("Webhook callback result or a documented protocol error.")
  @errorsDoc("The callback token is invalid, stale, references missing state, or uses a stale generation.")
  @TypeSpec.Rest.action("callback")
  @post
  @route("/subscriptions/{id}/callback")
  callback(
    ...SubscriptionIdParam,
    @secret
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: CallbackRequest,
  ): CallbackOkResponse | FencedConflict | Unauthorized | NotFound | Gone;

  @doc("Claim pending work for a pull-wake subscription.")
  @summary("Claim pull-wake work")
  @returnsDoc("Claim result or a documented protocol error.")
  @errorsDoc("The claim is unauthorized, already held, or references missing subscription state.")
  @TypeSpec.Rest.action("claim")
  @post
  @route("/subscriptions/{id}/claim")
  claim(
    ...SubscriptionIdParam,
    @secret
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: ClaimRequest,
  ):
    | ClaimSuccessResponse
    | AlreadyClaimedConflict
    | Unauthorized
    | NotFound
    | Gone;

  @doc("Ack processed offsets and optionally release the current claim.")
  @summary("Acknowledge pull-wake progress")
  @returnsDoc("Acknowledgement result or a documented protocol error.")
  @errorsDoc("The token is unauthorized, stale, or references missing subscription state.")
  @TypeSpec.Rest.action("ack")
  @post
  @route("/subscriptions/{id}/ack")
  ack(
    ...SubscriptionIdParam,
    @secret
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: CallbackRequest,
  ): CallbackOkResponse | FencedConflict | Unauthorized | NotFound | Gone;

  @doc("Release the current pull-wake claim without acking offsets.")
  @summary("Release pull-wake claim")
  @returnsDoc("Release result or a documented protocol error.")
  @errorsDoc("The token is unauthorized, stale, or references missing subscription state.")
  @TypeSpec.Rest.action("release")
  @post
  @route("/subscriptions/{id}/release")
  release(
    ...SubscriptionIdParam,
    @secret
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: ReleaseRequest,
  ): NoContentResponse | FencedConflict | Unauthorized | NotFound | Gone;

  @doc("Discover public webhook signing keys.")
  @summary("Get webhook signing keys")
  @returnsDoc("Public JSON Web Key Set used to verify Durable Streams webhook signatures.")
  @get
  @route("/jwks.json")
  getWebhookSigningKeys(): JwksResponse;
}

model IdempotentProducer {
  id: ProducerId;
  epoch: ProducerEpoch;
  seq: ProducerSeq;
}

model ScheduleRequestBase {
  at: Rfc3339DateTime;
  stream: StreamRootRelativePath;

  producer?: IdempotentProducer;
  close?: boolean;
}

@example(
  #{
    at: "2026-01-01T00:00:00Z",
    stream: "events/orders",
    contentType: "application/json",
    body: #{ type: "order.timeout", orderId: "ord_123" },
    producer: #{ id: "scheduler", epoch: 1, seq: 42 },
  },
  #{ title: "Scheduled JSON append" }
)
model JsonScheduleRequest extends ScheduleRequestBase {
  @encodedName("application/json", "content_type")
  contentType: "application/json";

  @doc("JSON value to append when `content_type` is `application/json`.")
  body: JsonValue;
}

model BinaryScheduleRequest extends ScheduleRequestBase {
  @encodedName("application/json", "content_type")
  contentType: MediaType;

  @doc("Base64url bytes for non-JSON scheduled appends. Mutually exclusive with `body`; `content_type` must not be `application/json`.")
  @encodedName("application/json", "body_base64")
  bodyBase64: Base64UrlBytes;
}

@summary("Scheduled append request. Use `body` for JSON appends and `body_base64` for non-JSON appends.")
union ScheduleRequest {
  json: JsonScheduleRequest,
  binary: BinaryScheduleRequest,
}

enum ScheduleStatus {
  pending: "pending",
  fired: "fired",
  cancelled: "cancelled",
  failed: "failed",
}

@TypeSpec.Rest.resource("schedules")
model Schedule {
  @key
  @visibility(Lifecycle.Read)
  id: ScheduleId;

  @visibility(Lifecycle.Read)
  status: ScheduleStatus;
  at: Rfc3339DateTime;
  stream: StreamRootRelativePath;
}

model ScheduleCreated {
  ...CreatedResponse;
  ...Body<Schedule>;
}

model ScheduleConfirmed {
  ...OkResponse;
  ...Body<Schedule>;
}

model ScheduleResponse {
  ...OkResponse;
  ...Body<Schedule>;
}

@route("/v1/stream/__ds/schedules/{id}")
@tag("Schedules")
interface Schedules {
  @doc("Create or idempotently confirm a scheduled append.")
  @summary("Create or confirm a scheduled append")
  @returnsDoc("Create or confirm result, or a documented protocol error.")
  @errorsDoc("The schedule request is invalid, conflicts with an existing schedule, or is rate limited.")
  @opExample(
    #{
      parameters: #{
        id: "sched-order-timeout",
        contentType: "application/json",
        body: #{
          at: "2026-01-01T00:00:00Z",
          stream: "events/orders",
          contentType: "application/json",
          body: #{ type: "order.timeout", orderId: "ord_123" },
        },
      },
    },
    #{ title: "Schedule a JSON append" }
  )
  @TypeSpec.Rest.createsOrReplacesResource(Schedule)
  @put
  putSchedule(
    ...ScheduleIdParam,
    @header("Content-Type") contentType: "application/json",
    @body body: ScheduleRequest,
  ):
    | ScheduleCreated
    | ScheduleConfirmed
    | BadRequest
    | Conflict
    | TooManyRequests;

  @doc("Read a scheduled append.")
  @summary("Get a schedule")
  @returnsDoc("Schedule state result or a documented protocol error.")
  @errorsDoc("The schedule is missing, tombstoned, or currently rate limited.")
  @TypeSpec.Rest.readsResource(Schedule)
  @get
  getSchedule(...ScheduleIdParam):
    | ScheduleResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Cancel a pending scheduled append.")
  @summary("Cancel a schedule")
  @returnsDoc("Cancel result or a documented protocol error.")
  @errorsDoc("The schedule is missing, tombstoned, or currently rate limited.")
  @TypeSpec.Rest.deletesResource(Schedule)
  @delete
  deleteSchedule(...ScheduleIdParam):
    | NoContentResponse
    | NotFound
    | Gone
    | TooManyRequests;
}
