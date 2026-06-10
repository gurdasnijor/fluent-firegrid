# Client current semantics

## Status

`PROTOCOL.md` is the source of truth for Durable Streams semantics. This note
constrains the first `effect-durable-client` coordination slice so the package
maps protocol invariants into Effect constructs instead of exposing a second
endpoint-shaped API.

The client remains a protocol binding package. It must not become a worker
orchestration layer, scheduler, durable execution layer, or product-specific
deployment layer.

Spec references: `coordination-substrate.CLIENT.1`,
`coordination-substrate.CLIENT.2`, `coordination-substrate.CLIENT.3`,
`coordination-substrate.CLIENT.4`, `coordination-substrate.CLIENT.5`,
`coordination-substrate.CLIENT.6`, `coordination-substrate.CLIENT.7`,
`coordination-substrate.CLIENT.8`, `coordination-substrate.CLIENT.9`,
`coordination-substrate.CLIENT.10`, `coordination-substrate.CLIENT.11`,
`coordination-substrate.CLIENT.12`.

## Design rule

The public API should be named around protocol operations and invariants, not
around HTTP endpoint rows. Endpoint paths, methods, headers, and JSON codecs are
the transport contract. The useful client surface is the semantic mapping:

- append-only log reads become `Stream`;
- appends and control operations become `Effect`;
- producer identity becomes a scoped resource with local sequence state;
- pull-wake claims become scoped leases;
- fencing becomes typed interruption of the work region; and
- webhook verification becomes handler-side middleware over raw request bytes.

An operation may still execute a single reserved HTTP endpoint internally. The
API should expose what the call means to a caller that must preserve distributed
invariants.

## Transport seam

HTTP remains the wire protocol. The client should continue to use the existing
`Endpoint` and `HttpClient` plumbing for URL construction, headers, retry
policy, auth header injection, and protocol error decoding.

`@effect/rpc` is not the first-slice transport. Durable Streams already owns a
public HTTP protocol with stable methods, paths, headers, request bodies,
response bodies, and status codes. Adding RPC request names would create a
parallel catalog instead of binding the protocol.

## Log plane

Stream URLs and stream-root-relative paths are separate concepts:

- normal log reads and writes operate on concrete stream URLs through the
  existing `DurableStream` and `DurableStreamClient` core; and
- reserved subscription and schedule bodies refer to stream-root-relative paths.

Reads should remain `Stream` values. Catch-up and live reads carry opaque
ordered offsets through protocol headers or SSE control events. `Stream-Closed`
or `streamClosed` is the EOF signal; where an API represents a finite read, that
signal should complete the `Stream` rather than requiring application code to
poll forever.

Writes remain `Effect`s. A successful duplicate producer append is successful
control flow, not an application error. A stale producer epoch is a typed,
recoverable fencing error that gives the caller enough protocol data to decide
whether to stop, restart with a higher epoch, or surface the failure.

## Producer identity plane

Producer-scoped APIs should model `(stream, Producer-Id, Producer-Epoch,
Producer-Seq)` as stateful writer identity. The resource owns the next
`Producer-Seq` for its epoch while the scope is open. Closing the scope releases
only client-side state; server-side producer state remains part of the stream's
protocol state.

The producer resource must preserve these distinctions:

- newly accepted append;
- duplicate append accepted by producer deduplication;
- stale epoch fencing;
- sequence gap; and
- closed-stream conflict.

Schedule creation that carries a producer tuple must reuse the same tuple
semantics at fire time. It must not introduce a client-side dedupe key or
higher-layer "already handled" store.

## Coordination plane

Subscription registration is durable protocol state. The client may provide
typed constructors for webhook and pull-wake subscription configs, filters,
stream membership changes, and schedule configs, but it must not evaluate CEL,
maintain a predicate index, or decide durable cursor advancement locally.

Pull-wake claims are scoped leases. A successful claim should expose the wake
snapshot and a claim token only inside a scope. Heartbeat uses the protocol ack
endpoint without `done: true` and is bound to the same scope as the work region.
Final ack with `done: true` and voluntary release are the only operations that
end the lease intentionally.

Generation fencing must stop the work region. When ack or release returns
`FENCED`, the client should surface a typed fencing failure and interrupt any
claim-scoped work that could otherwise commit after its generation lost the
lease.

The wake stream is an ordinary Durable Stream. Consuming it should reuse normal
read APIs and then claim the referenced subscription; it should not introduce a
special wake-stream abstraction.

## Schedules

Scheduled append is delayed producer-fenced append. Client helpers may encode
schedule request and response bodies, but the observable semantics are:

- `PUT` creates or reconfirms by normalized schedule config;
- `GET` reads durable schedule status;
- `DELETE` cancels only pending schedules; and
- fire uses the normal append implementation, including content type checks,
  producer deduplication, stream closure, and subscription wake hooks.

The client must not own the scheduler. It should not run local timers to emulate
durable scheduled append.

## Webhooks and JWKS

Webhook helpers belong at the handler boundary. They should verify the
`Webhook-Signature` header against the exact raw request body, select keys by
`kid` from the server JWKS, cache JWKS responses according to HTTP cache
metadata, and reject timestamps outside the configured replay window.

Verification should produce a typed wake value only after signature and replay
checks pass. Application code should not need to reimplement JWKS lookup or raw
body signature handling for each webhook handler.

## Non-goals

The first coordination slice must not introduce:

- a durable execution layer;
- a worker pool;
- a handler lifecycle manager;
- local CEL evaluation;
- a scheduler;
- a predicate index;
- a dedupe store;
- retry loops around claim processing;
- product-specific authentication policy; or
- durable wait, durable sleep, child, spawn, join, or attachment helpers.

Those behaviors belong either in the server protocol implementation or in a
higher layer after the protocol endpoints and Effect bindings are covered by
conformance tests.
