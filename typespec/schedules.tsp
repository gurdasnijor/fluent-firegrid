using Http;

namespace DurableStreams;

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
