# SDD: Fluent Firegrid Finish Line

### Remaining Restate-like ergonomics above the proven substrate

|   |   |
| --- | --- |
| Status | Finish-line backlog |
| Date | 2026-06-25 |
| Package focus | `@firegrid/fluent-firegrid`, `@firegrid/fluent-firegrid-http`, `@firegrid/fluent-firegrid-node`, `@firegrid/fluent-firegrid-s2` |
| Foundation | Current `packages/verification` proof registry is good enough for the substrate layer |

---

## Decision

The foundational runtime and verification work is complete enough to stop
expanding proof-only packages. The next work should close product and authoring
surface gaps against the Restate TypeScript experience.

Do not reopen a parallel durable-function engine. Continue building above:

- TanStack Workflow runtime over S2;
- fluent generator handlers yielding `Effect`;
- S2 object owner/runtime binding;
- table-shaped virtual object state;
- transport-neutral HTTP binding;
- Node server binding.

`packages/verification` should continue protecting regressions, but it should no
longer be the main delivery artifact. New proofs should be added only when a new
product behavior needs distributed-systems evidence.

## Current Baseline

Implemented and passing:

- descriptor-first `iface` / `implement`;
- direct `service`, `workflow`, and `object` definitions;
- generator handlers where yielded values are `Effect`s;
- durable `run`, `sleep`, `sleepUntil`, and `waitForSignal` lowering to the
  TanStack/S2 runtime;
- typed service/workflow/object call clients;
- typed send clients returning durable `InvocationHandle`s with `attach()`;
- S2-backed virtual object state with table/materialization semantics;
- same-key object serialization, stale-owner recovery, live-owner fencing,
  replay-safe state reads/writes, and send handles proven against `s2 lite`;
- `@firegrid/fluent-firegrid-http` `Request -> Response` transport binding;
- `@firegrid/fluent-firegrid-node` deployable Node/S2 server binding.

## Restate Surface Comparison

### Durable Webhooks

Restate's durable webhook story is: any handler can be a durable webhook
endpoint, ingress is persisted, duplicate sender retries are deduplicated by
idempotency key, and the handler can use durable calls, sends, timers, and state.

Firegrid has the runtime pieces:

- HTTP call/send ingress;
- Node server binding;
- durable handler execution over S2;
- typed object sends and object state.

Remaining gaps:

- canonical webhook guide and example;
- ergonomic public route shape for webhook mounting;
- first-class idempotency-key option separate from raw `runId`;
- request authentication/signature-verification helpers;
- documented retry/dedupe behavior for external senders.

### Service Communication

This is the closest surface today.

Implemented:

- request-response clients for services, workflows, and objects;
- one-way send clients for services, workflows, and objects;
- object same-key ordering;
- typed send handles with attach/output semantics;
- descriptor-first contract sharing via `iface`.

Remaining gaps:

- `genericCall` and `genericSend` for dynamic service/method names;
- Restate-like option builders for call/send options;
- explicit `idempotencyKey` option;
- delayed send option;
- invocation cancel;
- workflow key ergonomics closer to `workflowClient(workflow, "wf-id")`;
- clearer naming aliases matching Restate muscle memory:
  `serviceSendClient`, `objectSendClient`, `workflowSendClient`.

### Durable Timers

Implemented:

- `sleep`;
- `sleepUntil`;
- S2 persisted timer/signal substrate and host sweeps;
- proof coverage for timer/signal wakeup paths.

Remaining gaps:

- delayed messages on send clients;
- timeout combinators such as `orTimeout`;
- cron/schedule authoring helpers;
- docs explaining long sleeps, deployment version retention, and when delayed
  messages are better than sleeping in a handler.

### External Events

This is the largest product gap.

Implemented substrate:

- `waitForSignal` inside handlers;
- lower TanStack signal delivery APIs;
- S2-backed signal/timer persistence.

Remaining gaps:

- `awakeable<T>()`;
- `resolveAwakeable` / `rejectAwakeable`;
- HTTP resolve/reject endpoints for external systems;
- workflow-scoped durable promises;
- public signal delivery client helpers;
- rejection/terminal-error semantics;
- examples for human approval, webhook callback, and async external task-token
  patterns.

## Finish-Line Acceptance Ladder

### A. Service Communication Parity

**Goal.** Make the fluent client surface feel close enough to Restate that users
do not need to learn hidden `runId` tricks.

Ship:

- aliases: `serviceSendClient`, `objectSendClient`, `workflowSendClient`;
- option builders or plain option types for `idempotencyKey`, `delay`, and
  call/send metadata;
- `genericCall` / `genericSend`;
- `cancel(invocationId)` if the lower runtime can enforce cancellation;
- attach by invocation id without needing a concrete definition where possible.

Tests:

- unit tests for typed and generic request envelopes;
- HTTP tests for option propagation;
- one S2 proof only if delayed send or cancel affects durable execution safety.

### B. Durable External Events

**Goal.** Support Restate-like callback/task-token and human-in-the-loop flows.

Ship:

- `awakeable<T>()` returning `{ id, promise }` in Effect-native form;
- `resolveAwakeable` / `rejectAwakeable`;
- HTTP endpoints for resolving/rejecting awakeables;
- workflow promise API for named workflow-scoped events;
- typed terminal errors for rejected external events.

Tests:

- handler waits, process restarts, external endpoint resolves, handler resumes;
- reject path maps to typed terminal failure;
- duplicate resolve/reject is idempotent or explicitly rejected.

### C. Delayed Messages And Timeouts

**Goal.** Cover the Restate durable timers guide beyond raw sleep.

Ship:

- delayed send option on send clients and generic sends;
- `orTimeout` or Effect-native timeout helpers that preserve typed failures;
- schedule/cron helper if the TanStack schedule substrate is sufficient;
- docs for sleep vs delayed send.

Tests:

- delayed send is admitted, host loop wakes it later, and sender completes
  immediately;
- timeout around a client call produces the expected typed failure.

### D. Durable Webhook Product Example

**Goal.** Demonstrate the complete public product surface without proof-fixture
code.

Ship an example app that uses:

- `serveFluentS2`;
- a webhook service handler;
- idempotency key dedupe;
- `objectSendClient` to route by external entity id;
- table-shaped object state;
- `run` for side effects;
- `sleep` or delayed send for retry/backoff.

Tests:

- example smoke test against the Node server binding;
- docs that explain the route, idempotency key, and retry behavior.

## Non-Goals

- Do not introduce `Operation<T>` / `Future<T>` as separate primitives. `Effect`
  is the operation type.
- Do not put HTTP servers into `@firegrid/fluent-firegrid`.
- Do not replace table-shaped object state with string slot state.
- Do not expand verification into more narrow proofs unless the product surface
  genuinely needs new distributed-systems evidence.
- Do not vendor more TanStack/Restate material unless it is directly shaping a
  production API.

## Order Of Work

1. Service communication parity.
2. Durable external events.
3. Delayed messages and timeout ergonomics.
4. Durable webhook example and guide.
5. Runtime cleanup only where product work exposes pain: object-runtime
   projection duplication, polling completion, config/env parsing, logging, and
   auth hooks.

At that point, Firegrid should credibly support the practical Restate-like
surface for durable webhooks, service communication, durable timers, and external
events while preserving its Effect-native authoring model.
