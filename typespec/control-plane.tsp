using Http;

namespace DurableStreams;

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
