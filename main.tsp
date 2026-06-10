import "@typespec/http";
import "@typespec/streams";
import "@typespec/sse";
import "@typespec/events";
import "@typespec/rest";

using Http;

@service(#{ title: "Durable Streams Protocol" })
@server(
  "{endpoint}",
  "Durable Streams server root",
  {
    endpoint: "https://example.com",
  }
)
namespace DurableStreams;

alias Offset = string;
alias StreamRootRelativePath = string;
alias Rfc3339DateTime = utcDateTime;
alias JsonValue = unknown;

model StreamUrlPathParam {
  @doc("Stream-root-relative path. This value may contain slash-separated path segments.")
  @path
  path: StreamRootRelativePath;
}

model ControlPathParam {
  @doc("URL-encoded stream-root-relative path used inside reserved control APIs.")
  @path
  path: StreamRootRelativePath;
}

model SubscriptionIdParam {
  @path id: string;
}

model ScheduleIdParam {
  @path id: string;
}

model ContentTypeHeader {
  @header("Content-Type")
  contentType: string;
}

model OptionalContentTypeHeader {
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
  streamSeq?: string;

  ...StreamClosedRequestHeader;

  @header("Producer-Id")
  producerId?: string;

  @header("Producer-Epoch")
  producerEpoch?: safeint;

  @header("Producer-Seq")
  producerSeq?: safeint;
}

model TailHeaders {
  ...ContentTypeHeader;

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
  producerEpoch?: safeint;

  @header("Producer-Seq")
  producerSeq?: safeint;
}

model ProducerFencedHeaders {
  @header("Producer-Epoch")
  producerEpoch: safeint;
}

model ProducerGapHeaders {
  @header("Producer-Expected-Seq")
  producerExpectedSeq?: safeint;

  @header("Producer-Received-Seq")
  producerReceivedSeq?: safeint;
}

model ClosedConflictHeaders {
  @header("Stream-Closed")
  streamClosed?: "true";

  @header("Stream-Next-Offset")
  streamNextOffset?: Offset;
}

model EmptyError {
  code?: string;
  message?: string;
}

@error
model BadRequestError {
  code: "BAD_REQUEST";
  message: string;
}

@error
model ConflictError {
  code: "CONFLICT";
  message?: string;
}

@error
model NotFoundError {
  code: "NOT_FOUND";
  message?: string;
}

@error
model GoneError {
  code: "GONE";
  message?: string;
}

@error
model TooManyRequestsError {
  code: "TOO_MANY_REQUESTS";
  message?: string;
}

@error
model FencedError {
  code: "FENCED";
}

@error
model AlreadyClaimedError {
  code: "ALREADY_CLAIMED";

  @encodedName("application/json", "current_holder")
  currentHolder: string;

  generation: safeint;
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

model ErrorEnvelope<TError> {
  error: TError;
}

model CreateStreamRequest {
  ...StreamUrlPathParam;
  ...StreamCreateHeaders;
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

model AppendStreamRequest {
  ...StreamUrlPathParam;
  ...StreamAppendHeaders;
  @body body?: bytes;
}

model PlainAppendAccepted {
  ...NoContentResponse;

  @header("Stream-Next-Offset")
  streamNextOffset: Offset;

  @header("Stream-Closed")
  streamClosed?: "true";
}

model ProducerAppendAccepted {
  ...OkResponse;
  ...ProducerSuccessHeaders;
}

model ProducerAppendDuplicate {
  ...NoContentResponse;
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
  @put
  createStream(...CreateStreamRequest):
    | CreateStreamCreated
    | CreateStreamAlreadyExists
    | BadRequest
    | Conflict
    | NotFound
    | TooManyRequests;

  @doc("Append bytes to a stream and optionally close it atomically.")
  @post
  appendToStream(...AppendStreamRequest):
    | PlainAppendAccepted
    | ProducerAppendAccepted
    | ProducerAppendDuplicate
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
  @head
  headStream(...StreamUrlPathParam):
    | HeadStreamOk
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Read catch-up bytes, long-poll for new bytes, or stream SSE events.")
  @get
  readStream(
    ...StreamUrlPathParam,
    @query offset?: Offset | "-1" | "now",
    @query live?: "long-poll" | "sse",
    @query cursor?: string,
    @query timeout?: string,
    @header("If-None-Match") ifNoneMatch?: string,
  ):
    | ReadStreamOk
    | ReadStreamNoContent
    | ReadStreamNotModified
    | SseStreamOk
    | CommonStreamErrors;

  @doc("Delete a stream. Streams with active forks become soft-deleted.")
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
  expression: string;
  self?: JsonValue;
}

model WebhookConfig {
  url: string;
}

model WebhookSigningMetadata {
  alg: "ed25519";
  kid: string;

  @encodedName("application/json", "jwks_url")
  jwksUrl: string;
}

model WebhookMetadata extends WebhookConfig {
  signing?: WebhookSigningMetadata;
}

model SubscriptionConfig {
  type: SubscriptionType;
  pattern?: string;
  streams?: StreamRootRelativePath[];
  webhook?: WebhookConfig;

  @encodedName("application/json", "wake_stream")
  wakeStream?: StreamRootRelativePath;

  filter?: WakeFilter;

  @encodedName("application/json", "lease_ttl_ms")
  leaseTtlMs?: int32;

  description?: string;
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
  id: string;

  @encodedName("application/json", "subscription_id")
  subscriptionId: string;

  type: SubscriptionType;
  pattern?: string;
  streams: SubscriptionStreamLink[];
  webhook?: WebhookMetadata;

  @encodedName("application/json", "wake_stream")
  wakeStream?: StreamRootRelativePath | null;

  filter?: WakeFilter;

  @encodedName("application/json", "lease_ttl_ms")
  leaseTtlMs: int32;

  @encodedName("application/json", "created_at")
  createdAt: Rfc3339DateTime;

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
  wakeId: string;

  generation: safeint;
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
  worker: string;
}

model ClaimSuccess {
  @encodedName("application/json", "wake_id")
  wakeId: string;

  generation: safeint;
  token: string;
  streams: PendingStream[];

  @encodedName("application/json", "lease_ttl_ms")
  leaseTtlMs: int32;
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
  wakeId: string;

  generation: safeint;
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
  @TypeSpec.Rest.readsResource(Subscription)
  @get
  @route("/subscriptions/{id}")
  getSubscription(...SubscriptionIdParam):
    | SubscriptionResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Tombstone a subscription.")
  @TypeSpec.Rest.deletesResource(Subscription)
  @delete
  @route("/subscriptions/{id}")
  deleteSubscription(...SubscriptionIdParam):
    | NoContentResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Add explicit stream links to a subscription.")
  @TypeSpec.Rest.collectionAction(Subscription, "streams")
  @post
  @route("/subscriptions/{id}/streams")
  addSubscriptionStreams(
    ...SubscriptionIdParam,
    @header("Content-Type") contentType: "application/json",
    @body body: StreamMembershipRequest,
  ): NoContentResponse | NotFound | Gone | BadRequest | TooManyRequests;

  @doc("Remove one explicit stream link from a subscription.")
  @TypeSpec.Rest.collectionAction(Subscription, "streams")
  @delete
  @route("/subscriptions/{id}/streams/{+path}")
  removeSubscriptionStream(...SubscriptionIdParam, ...ControlPathParam):
    | NoContentResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Webhook asynchronous completion callback.")
  @TypeSpec.Rest.action("callback")
  @post
  @route("/subscriptions/{id}/callback")
  callback(
    ...SubscriptionIdParam,
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: CallbackRequest,
  ): CallbackOkResponse | FencedConflict | Unauthorized | NotFound | Gone;

  @doc("Claim pending work for a pull-wake subscription.")
  @TypeSpec.Rest.action("claim")
  @post
  @route("/subscriptions/{id}/claim")
  claim(
    ...SubscriptionIdParam,
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
  @TypeSpec.Rest.action("ack")
  @post
  @route("/subscriptions/{id}/ack")
  ack(
    ...SubscriptionIdParam,
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: CallbackRequest,
  ): CallbackOkResponse | FencedConflict | Unauthorized | NotFound | Gone;

  @doc("Release the current pull-wake claim without acking offsets.")
  @TypeSpec.Rest.action("release")
  @post
  @route("/subscriptions/{id}/release")
  release(
    ...SubscriptionIdParam,
    @header("Authorization") authorization: string,
    @header("Content-Type") contentType: "application/json",
    @body body: ReleaseRequest,
  ): NoContentResponse | FencedConflict | Unauthorized | NotFound | Gone;

  @doc("Discover public webhook signing keys.")
  @get
  @route("/jwks.json")
  getWebhookSigningKeys(): JwksResponse;
}

model IdempotentProducer {
  id: string;
  epoch: safeint;
  seq: safeint;
}

model ScheduleRequest {
  at: Rfc3339DateTime;
  stream: StreamRootRelativePath;

  @encodedName("application/json", "content_type")
  contentType: string;

  body?: JsonValue;

  @encodedName("application/json", "body_base64")
  bodyBase64?: string;

  producer?: IdempotentProducer;
  close?: boolean;
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
  id: string;

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
  @TypeSpec.Rest.readsResource(Schedule)
  @get
  getSchedule(...ScheduleIdParam):
    | ScheduleResponse
    | NotFound
    | Gone
    | TooManyRequests;

  @doc("Cancel a pending scheduled append.")
  @TypeSpec.Rest.deletesResource(Schedule)
  @delete
  deleteSchedule(...ScheduleIdParam):
    | NoContentResponse
    | NotFound
    | Gone
    | TooManyRequests;
}
