# Durable Execution Substrate Pushdown Design

## Scope

`effect-durable-execution` is the reference Effect authoring package for Durable
Streams coordination. It provides application-facing primitives such as named
steps, handler definitions, and local Effect composition, while Durable Streams
owns the durable substrate semantics.

This package should stay thin enough that changes to durable scheduling,
predicate wake-up, cursor fencing, and commit-once append behavior land in the
Durable Streams protocol, server, conformance tests, and
`effect-durable-client` client first.

## Package Boundaries

| Package                             | Owns                                                                                             | Must not own                                         |
| ----------------------------------- | ------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `packages/server`                   | Protocol implementation for streams, subscriptions, schedules, filters, leases, producer fencing | Application authoring syntax                         |
| `packages/server-conformance-tests` | Portable behavior coverage for substrate capabilities                                            | Package-specific runtime assumptions                 |
| `packages/effect-durable-client`    | Thin Effect client for protocol and coordination endpoints                                       | A second scheduler, predicate index, or dedupe store |
| `packages/effect-durable-execution` | Authoring primitives and lowering rules                                                          | Durable substrate state machines                     |

## Current Surface

The current package covers the first thin slice:

- `run(action, { name })` for named durable steps;
- `execute(ctx, effect)` for handler-edge execution with an explicit journal
  context;
- `service`, `object`, and `workflow` definition descriptors;
- typed call/send clients over an injected ingress interface; and
- Effect-native `all`, `race`, `select`, and local `spawn` combinators.

This surface deliberately avoids a custom Future scheduler. Local Effect
composition remains local Effect composition unless a future primitive explicitly
lowers to a Durable Streams substrate capability.

## Lowering Rules

### Named Steps

Named steps lower to producer-fenced appends on the session stream:

1. read the session stream;
2. replay a matching `StepSucceeded` or `StepFailed` fact if one exists;
3. run the action only when no fact exists; and
4. append the result with a stable producer tuple.

The package may choose stable step keys and result schemas. Durable Streams owns
append ordering, producer fencing, and duplicate classification.

### Sleep

Durable sleep should not be implemented with a runtime-local timer heap. The
intended lowering is:

1. create a scheduled append for a timer fact through `effect-durable-client`;
2. wait for that timer fact through a filtered or pull-wake subscription; and
3. resume after the subscription wake is claimed and acked.

This depends on scheduled append conformance in the server package.

### Wait / Await Event

Durable wait should not keep a package-owned predicate registry. The intended
lowering is:

1. create or reuse a filtered subscription for the awaited event shape;
2. claim the wake through the subscription client;
3. read and decode the matching fact from the stream; and
4. ack the subscription cursor after the handler commits its reaction.

This depends on filtered subscription conformance in the server package.

### Spawn and Attach

Durable spawn and attach should be modeled with ordinary streams:

1. derive a child stream endpoint;
2. append an invocation fact with a stable producer tuple;
3. subscribe to child progress or terminal facts; and
4. interpret those facts in the parent handler.

Durable Streams does not need to know workflow-specific child semantics. It only
needs stream lifecycle, producer-fenced append, subscriptions, and optional
filters.

## Cross-package Development Order

When adding a durable primitive, use this order:

1. update `PROTOCOL.md`;
2. add or update `packages/server-conformance-tests` coverage;
3. implement the server substrate capability;
4. expose the capability through `packages/effect-durable-client`; and
5. lower `packages/effect-durable-execution` authoring primitives onto that
   client capability.

Skipping directly to this package is a design smell: it usually means substrate
semantics are being rebuilt above Durable Streams.

## Non-goals

This package should not provide:

- HTTP route hosting;
- provider-specific webhook acceptance policy;
- a scheduler service;
- a predicate evaluation engine;
- a dedupe database separate from producer fencing; or
- direct imports from `packages/server`.

Those concerns either belong in Durable Streams substrate packages or in an
application-specific host outside this upstreamable package.
