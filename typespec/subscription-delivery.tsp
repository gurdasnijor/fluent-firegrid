using Http;

namespace DurableStreams;

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

@route("/v1/stream/__ds")
@tag("Subscription Delivery")
interface SubscriptionDelivery {
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
}
