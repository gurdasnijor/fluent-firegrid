import "@typespec/http";
import "@typespec/streams";
import "@typespec/sse";
import "@typespec/events";
import "@typespec/rest";

import "./common.tsp";
import "./data-plane.tsp";
import "./control-plane.tsp";

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
