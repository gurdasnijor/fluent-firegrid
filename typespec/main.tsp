import "@typespec/http";
import "@typespec/streams";
import "@typespec/sse";
import "@typespec/events";
import "@typespec/rest";

import "./common.tsp";
import "./streams.tsp";
import "./subscriptions.tsp";
import "./subscription-delivery.tsp";
import "./schedules.tsp";

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
