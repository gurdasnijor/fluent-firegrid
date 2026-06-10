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

  @doc("Discover public webhook signing keys.")
  @summary("Get webhook signing keys")
  @returnsDoc("Public JSON Web Key Set used to verify Durable Streams webhook signatures.")
  @get
  @route("/jwks.json")
  getWebhookSigningKeys(): JwksResponse;
}
