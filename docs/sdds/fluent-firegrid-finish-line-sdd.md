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

### Durable Table Waits

This is distinct from token-based external events. A handler should be able to
wait until durable table/materialized state satisfies a serializable predicate.

Target direction:

- use CEL as the persisted predicate grammar;
- type-check predicates against the table schema at registration time;
- evaluate predicates against the projected row/materialized view after each
  relevant state change;
- resume the waiting run when the predicate becomes true.

This mirrors the CEL expression direction used by systems such as Inngest while
keeping the predicate portable and inspectable.

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

## Target API Shape

These examples are the intended authoring direction. Names can still move, but
the shape should stay Effect-native: handler bodies are generators and durable
operations are `Effect`s.

### Definitions And Serving

Definitions remain the root of type sharing. Apps should be able to expose them
through the Node/S2 binding without writing fixture server code.

```ts
import { iface, implement, run } from "@firegrid/fluent-firegrid"
import { serveFluentS2 } from "@firegrid/fluent-firegrid-node"
import { Schema } from "effect"

const ordersContract = iface.service("orders", {
  submit: iface.schemas({
    input: Schema.Struct({ orderId: Schema.String }),
    output: Schema.Struct({ accepted: Schema.Boolean })
  })
})

const orders = implement(ordersContract, {
  handlers: {
    *submit(input) {
      yield* run(() => reserveInventory(input.orderId), { name: "reserve" })
      return { accepted: true }
    }
  }
})

await serveFluentS2({
  definitions: [orders],
  namespace: "orders-prod",
  port: 8080,
  s2Endpoint: process.env.S2_ENDPOINT!
})
```

### Typed Service Communication

Call clients should stay simple. Send clients should use Restate-familiar names
while keeping the existing aliases for compatibility.

```ts
import {
  serviceClient,
  serviceSendClient,
  objectClient,
  objectSendClient,
  workflowClient,
  workflowSendClient,
  rpc
} from "@firegrid/fluent-firegrid"

const receipt = yield* serviceClient(orders).submit(
  { orderId: "order-1" },
  rpc.opts({ idempotencyKey: "stripe:event-1" })
)

const handle = yield* serviceSendClient(orders).submit(
  { orderId: "order-2" },
  rpc.sendOpts({
    idempotencyKey: "order-2:submit",
    delay: { seconds: 30 }
  })
)

const submitted = yield* handle.attach()

const counter = objectClient(counters, "user-1")
const value = yield* counter.add({ by: 1 })

yield* objectSendClient(counters, "user-1").add(
  { by: 1 },
  rpc.sendOpts({ idempotencyKey: "counter:user-1:event-1" })
)

const run = workflowClient(reviewWorkflow, "review-123")
const result = yield* run.status(undefined)

yield* workflowSendClient(reviewWorkflow, "review-123").nudge(undefined)
```

The existing `objectClient(definition)(key)` shape may remain, but the finish
line should include the direct Restate-like overload:
`objectClient(definition, key)`.

### Generic Calls And Sends

Generic APIs should exist for dynamic names and cross-language callers that only
have descriptors or route metadata at runtime.

```ts
import { genericCall, genericSend, rpc } from "@firegrid/fluent-firegrid"

const output = yield* genericCall<string>({
  kind: "service",
  name: "orders",
  handler: "submit",
  input: { orderId: "order-3" },
  idempotencyKey: "order-3:submit"
})

const handle = yield* genericSend<{ accepted: boolean }>({
  kind: "object",
  name: "payment-tracker",
  key: "invoice-1",
  handler: "onPaymentSucceeded",
  input: { invoiceId: "invoice-1" },
  delay: rpc.duration({ minutes: 5 })
})
```

### Invocation Handles, Attach, And Cancel

Send handles should be useful immediately and serializable for later attach.

```ts
import { attach, cancel, invocation } from "@firegrid/fluent-firegrid"

const handle = yield* serviceSendClient(orders).submit(
  { orderId: "order-4" },
  rpc.sendOpts({ idempotencyKey: "order-4:submit" })
)

yield* state(SubmittedOrders).set({
  id: "order-4",
  invocationId: handle.invocationId
})

const sameHandle = invocation<typeof handle.output>("orders.submit", handle.invocationId)
const output = yield* sameHandle.attach()

const outputAgain = yield* attach<typeof output>(handle.invocationId)
yield* cancel(handle.invocationId)
```

If the lower runtime cannot enforce cancellation yet, expose `cancel` only after
there is a real terminal/cancel event in the persisted execution state.

### Durable Timers, Delayed Messages, And Timeouts

Raw sleep is already available. The finish line adds send delays and timeout
ergonomics.

```ts
import { orTimeout, rpc, serviceSendClient, sleep, sleepUntil } from "@firegrid/fluent-firegrid"

yield* sleep("10 seconds")
yield* sleepUntil(Date.now() + 60_000)

yield* serviceSendClient(emails).sendReceipt(
  { orderId: "order-5" },
  rpc.sendOpts({ delay: { hours: 1 } })
)

const approved = yield* serviceClient(approvals).request(
  { documentId: "doc-1" }
).pipe(
  orTimeout("5 minutes")
)
```

`orTimeout` should be an Effect combinator returning a typed timeout error, not a
new Future abstraction.

### Durable Table Waits With CEL

State waits should subscribe to materialized table changes using a serializable
predicate, not a JavaScript closure. CEL is the target predicate language.

```ts
import { cel, state } from "@firegrid/fluent-firegrid"

const invoices = state(Invoices)

const paid = yield* invoices.waitFor("invoice-1", {
  name: "invoice-paid",
  when: cel("row.status == 'paid'")
})
```

The predicate environment is derived from the table schema. For keyed row waits,
the minimum environment is:

```ts
cel.env(Invoices)
  .registerVariable("row", "Invoices")
  .registerVariable("old", "Invoices?")
  .registerVariable("change", {
    schema: {
      table: "string",
      key: "string",
      operation: "string"
    }
  })
```

The same API can expose a builder form when users want typed field names without
writing raw CEL strings everywhere:

```ts
const ready = yield* state(Invoices).waitFor("invoice-2", {
  name: "invoice-ready-for-capture",
  when: cel.expr((t) =>
    t.row.status.eq("authorized")
      .and(t.row.amount.greaterThan(0))
      .and(t.change.operation.in(["set", "update"]))
  )
})
```

For object handlers, the most common pattern is waiting on state another handler
will eventually mutate:

```ts
const invoice = object({
  name: "invoice",
  handlers: {
    *charge(input: { readonly invoiceId: string }) {
      const row = yield* state(Invoices).waitFor(input.invoiceId, {
        name: "authorized",
        when: cel("row.status == 'authorized'"),
        timeoutMs: 30 * 60_000
      })

      yield* run(() => capturePayment(row.paymentIntentId), {
        name: "capture-payment"
      })
    },

    *authorized(input: { readonly invoiceId: string; readonly paymentIntentId: string }) {
      yield* state(Invoices).set({
        id: input.invoiceId,
        paymentIntentId: input.paymentIntentId,
        status: "authorized"
      })
    }
  }
})
```

The persisted wait registration should contain the expression text and the table
identity, not a function:

```ts
type StateWaitRegistered = {
  _tag: "StateWaitRegistered"
  waitId: string
  runId: string
  table: string
  key?: string
  name: string
  expression: string
  environmentVersion: string
  timeoutAt?: number
}
```

Resolution is deterministic:

1. On registration, evaluate the CEL expression against the current projection.
   If it is true, return immediately.
2. Otherwise append `StateWaitRegistered`.
3. On every relevant state change, evaluate open waits whose table/key index can
   match the change.
4. When the expression returns true, append `StateWaitReady` with the row or
   selected projection value.
5. The object queue owner resumes the parked run by delivering the corresponding
   TanStack signal, then appends `StateWaitDelivered`.
6. On replay, TanStack's recorded `SIGNAL_RESOLVED` value returns the selected
   row without re-evaluating against newer state.

Keyed waits are the first production slice:

```ts
yield* state(Invoices).waitFor("invoice-1", {
  name: "paid",
  when: cel("row.status == 'paid'")
})
```

Implementation status as of June 25, 2026:

- `cel("...")`, predicate validation/evaluation, and
  `state(Table).waitFor(key, { name, when, timeoutMs })` exist in
  `@firegrid/fluent-firegrid`;
- the S2 object state backend evaluates keyed waits against the materialized row
  projection and appends `StateWaitRegistered` / `StateWaitReady` records;
- the S2 object runtime skips pending state-wait calls, continues draining later
  same-key calls, and resumes ready waits through the queue-owned signal path;
- remaining gaps are timeout scheduling, schema-derived CEL environment
  generation, query/index waits, and richer typed CEL builder ergonomics.

Query waits can come later, but must require an indexable declaration so the
runtime does not scan all rows/waits:

```ts
yield* state(Invoices).waitFor({
  name: "first-ready-in-account",
  index: ["accountId", "status"],
  where: cel("row.accountId == accountId && row.status == 'ready'"),
  vars: { accountId }
})
```

### Awakeables

Awakeables cover task-token and human-in-the-loop patterns for services and
objects.

```ts
import { awakeable, resolveAwakeable, rejectAwakeable, run } from "@firegrid/fluent-firegrid"

const reviews = service({
  name: "reviews",
  handlers: {
    *request(input: { readonly documentId: string }) {
      const review = yield* awakeable<string>()

      yield* run(
        () => sendReviewEmail({ documentId: input.documentId, token: review.id }),
        { name: "send-review-email" }
      )

      return yield* review.await
    },

    *approve(input: { readonly token: string; readonly decision: string }) {
      yield* resolveAwakeable(input.token, input.decision)
    },

    *reject(input: { readonly token: string; readonly reason: string }) {
      yield* rejectAwakeable(input.token, input.reason)
    }
  }
})
```

The Node binding should also expose transport endpoints for external systems:

```text
POST /firegrid/awakeables/:id/resolve
POST /firegrid/awakeables/:id/reject
```

### Workflow Promises

Workflow promises are named, workflow-scoped events. They are more ergonomic
than manually passing awakeable ids when every signal is scoped to one workflow
instance.

```ts
import { workflow, workflowClient, workflowPromise, run } from "@firegrid/fluent-firegrid"

const reviewWorkflow = workflow({
  name: "review",
  handlers: {
    *run(input: { readonly documentId: string }) {
      yield* run(() => askReviewer(input.documentId), { name: "ask-reviewer" })
      const decision = yield* workflowPromise<string>("decision").await
      return { decision }
    },

    *submitDecision(input: { readonly decision: string }) {
      yield* workflowPromise<string>("decision").resolve(input.decision)
    }
  }
})

yield* workflowClient(reviewWorkflow, "review-123").submitDecision({
  decision: "approved"
})
```

### Durable Webhook Example

The webhook product shape should be a normal service handler, plus idempotency
key support at the transport boundary.

```ts
import { objectSendClient, run, service } from "@firegrid/fluent-firegrid"
import { serveFluentS2 } from "@firegrid/fluent-firegrid-node"

const stripeWebhook = service({
  name: "stripe-webhook",
  handlers: {
    *onEvent(event: StripeEvent) {
      yield* run(() => verifyStripeEvent(event), { name: "verify-stripe-event" })

      const invoiceId = event.data.object.id
      if (event.type === "invoice.payment_failed") {
        yield* objectSendClient(paymentTracker, invoiceId).onPaymentFailed(event)
      }
      if (event.type === "invoice.payment_succeeded") {
        yield* objectSendClient(paymentTracker, invoiceId).onPaymentSucceeded(event)
      }
    }
  }
})

await serveFluentS2({
  definitions: [stripeWebhook, paymentTracker],
  namespace: "billing",
  port: 8080,
  s2Endpoint: process.env.S2_ENDPOINT!,
  webhooks: {
    "/webhooks/stripe": {
      definition: stripeWebhook,
      handler: "onEvent",
      idempotencyKey: (request) => request.headers.get("stripe-event-id"),
      verify: verifyStripeSignature
    }
  }
})
```

The `webhooks` option is intentionally transport-specific and belongs in
`@firegrid/fluent-firegrid-node`, not fluent core.

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

### B. Durable Table Waits

**Goal.** Support CEL-backed waits over durable table/materialized state.

Ship:

- `state(Table).waitFor(key, { name, when: cel(...) })`;
- CEL environment generation from table schemas;
- parse/type-check at registration time;
- persisted wait registrations with expression text and environment version;
- keyed wait index by `(table, key)`;
- timeout support;
- replay from the recorded signal resolution value.

Tests:

- wait returns immediately if the current row satisfies the predicate;
- wait parks, another handler mutates the row, predicate evaluates true, and the
  waiting run resumes;
- replay returns the recorded resolved value even if later state changes;
- invalid CEL fails at registration with a typed error.

### C. Durable External Events

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

### D. Delayed Messages And Timeouts

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

### E. Durable Webhook Product Example

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
2. CEL-backed durable table waits.
3. Durable external-token events.
4. Delayed messages and timeout ergonomics.
5. Durable webhook example and guide.
6. Runtime cleanup only where product work exposes pain: object-runtime
   projection duplication, polling completion, config/env parsing, logging, and
   auth hooks.

At that point, Firegrid should credibly support the practical Restate-like
surface for durable webhooks, service communication, durable timers, and external
events while preserving its Effect-native authoring model.
