# effect-durable-execution design

An Effect-native durable execution authoring layer over Durable Streams.

This package is intentionally smaller than a hosted durable-execution platform.
It gives users the primitives needed to build one: durable named steps, sleeps,
signals, deferred values, awakeables, typed handler metadata, and replay-safe
lowering onto the `effect-durable-client` / `effect-durable-streams` substrate.

The design is inspired by Restate's generator API, but the public model is not a
second effect system. Public programs are ordinary `Effect.Effect` values.

This document expands `docs/sdds/effect-durable-execution-sdd.md` and the
`effect-execution.*` ACIDs, especially `effect-execution.API.5`,
`effect-execution.API.6`, `effect-execution.API.20` through
`effect-execution.API.24`, `effect-execution.DELIVERY.6`,
`effect-execution.DELIVERY.7`, `effect-execution.CONFORMANCE.10`, and
`effect-execution.CONFORMANCE.23` through `effect-execution.CONFORMANCE.30`.

The SDD is the source of truth. This file exists to make the SDD less abstract
with concrete TypeScript shapes. If this file and the SDD disagree, fix this
file or update the SDD/spec first.

## Why this exists

Durable execution has two separate problems:

- the user wants to write straight-line Effect code; and
- the host must replay durable facts before deciding whether any side effect is
  allowed to run again.

The package keeps those concerns separated. User code stays Effect code. The
runtime supplies durable services through context and Layers, replays the
operation log before every activation, and turns primitives such as `run`,
`sleep`, `signal`, and `deferred` into durable substrate facts.

There is no public `Operation<T>` abstraction. Effect already provides the
lazy, typed computation model:

```ts
import { Effect } from "effect"

export type DurableProgram<A, E = never, R = never> = Effect.Effect<
  A,
  E,
  R | DurableExecutionRuntime
>
```

That preserves Effect's normal result, error, and requirement channels. Durable
execution adds requirements to `R`; it does not replace `Effect`.

## Authoring model

Users write handlers with `Effect.gen` and free durable primitives:

```ts
import { Duration, Effect, Schema } from "effect"
import {
  handler,
  handlerRequest,
  run,
  signal,
  sleep,
} from "effect-durable-execution"

const PermissionRequest = Schema.Struct({
  userId: Schema.String,
  action: Schema.String,
})

const Approval = Schema.Struct({
  approved: Schema.Boolean,
  reason: Schema.String,
})

export const reviewRequest = handler("reviewRequest", {
  input: PermissionRequest,
  output: Approval,
})(
  Effect.gen(function* () {
    const request = yield* handlerRequest(PermissionRequest)

    const draft = yield* run("draft-response", draftResponse(request), {
      output: Schema.String,
      retry: {
        maxAttempts: 3,
        initialInterval: Duration.millis(100),
      },
    })

    yield* sleep("cooldown", Duration.minutes(5))

    const approval = yield* signal("approval", Approval)

    yield* run("send-response", sendResponse(draft, approval), {
      output: Schema.Void,
    })

    return approval
  })
)
```

The host registers handlers and supplies the execution runtime Layer:

```ts
serve({
  handlers: [reviewRequest],
  layer: ExecutionRuntime.layer({
    client,
    invocationStreamName,
    producerIdForInvocation,
  }),
})
```

Restate-style `service`, `object`, and `workflow` definitions are not exported
by this package. Hosts may group handlers into those or other platform concepts,
but `effect-durable-execution` exports `handler(...)` as its only definition
primitive. First-party examples should avoid public "workflow" vocabulary.

## Free primitive surface

Restate's `free.ts` is a useful reference for ergonomics: module-level
functions read an active runtime slot and delegate to the active operations
object. We want that pattern, not a wholesale copy of the API.

The free surface should contain primitives whose semantics are durable,
replay-aware, or active-handler-specific:

```ts
export const run: Run
export const sleep: Sleep
export const signal: Signal
export const awakeable: Awakeable
export const deferred: Deferred
export const waitForState: WaitForState
export const attach: Attach
export const call: Call
export const send: Send
export const client: ClientFactory
export const sendClient: SendClientFactory
export const cancel: Cancel
export const state: State
export const sharedState: SharedState
export const channel: Channel
export const select: Select
export const handlerRequest: HandlerRequest
```

The package should not expose `currentOps`, `currentRuntime`, or an equivalent
escape hatch. Slot lookup is an implementation detail used to make free
primitives ergonomic.

Surface decisions:

| Category                  | Decision                                                         |
| ------------------------- | ---------------------------------------------------------------- |
| active runtime accessor   | internal only                                                    |
| handler request metadata  | expose as schema-decoded Effect primitive                        |
| durable steps             | expose `run`                                                     |
| durable timers            | expose `sleep`                                                   |
| named incoming facts      | expose receiver-side `signal`                                    |
| externally resolved facts | expose `awakeable`; resolution belongs to ingress clients        |
| named durable promise     | expose as `deferred`, not `workflowPromise`                      |
| state/resource wait       | expose `waitForState`; backed by filtered subscriptions          |
| call/send composition     | expose inside active operations                                  |
| invocation reference ops  | expose only against explicit references or ids                   |
| local composition         | use Effect built-ins where possible                              |
| tagged wait               | optionally expose `select`                                       |
| local channel             | expose only as non-durable local coordination                    |
| state                     | expose read-write `state` only in exclusive contexts             |
| shared state              | expose read-only `sharedState` in shared contexts                |
| date/random               | do not copy blindly; require keyed durable capture if introduced |
| logging                   | use Effect logging services                                      |

That means `Effect.all`, `Effect.race`, `Effect.raceAll`, scoped fibers, and
normal Effect resource management remain the default local composition tools.
The execution package should only add a helper when the helper has durable
meaning or when Effect does not already provide the ergonomic shape.

## Durable step model

`run` is the replay boundary for side effects:

```ts
export interface RunOptions<A, E, EncodedA = unknown, EncodedE = unknown> {
  readonly output?: Schema.Schema<A, EncodedA>
  readonly error?: Schema.Schema<E, EncodedE>
  readonly retry?: RetryPolicy
  readonly idempotencyKey?: string
  readonly cancellation?: CancellationPolicy
}

export interface Run {
  <A, E, R, EncodedA = unknown, EncodedE = unknown>(
    key: string,
    action: Effect.Effect<A, E, R> | RunAction<A, E, R>,
    options?: RunOptions<A, E, EncodedA, EncodedE>
  ): Effect.Effect<A, E | DurableExecutionError, R | DurableExecutionRuntime>
}
```

On activation, the runtime replays terminal step facts before evaluating the
action:

- `StepSucceeded(key, value)` returns the recorded decoded value;
- `StepFailed(key, error)` fails through the declared error schema;
- `StepStarted` and attempt metadata are observability only; and
- no terminal fact means the action may run and append a terminal outcome.

Retry is a policy on `run`. It controls attempts before a terminal step fact is
recorded. Once a terminal fact exists, replay returns the fact and does not
retry the action.

## Operation log model

The runtime folds an invocation operation log. It should not use "journal" as a
catch-all term.

`StepReplayFold` answers one question for `run(key, action)`: return a recorded
success, replay a recorded failure, or evaluate the action. It consumes only
terminal step outcome facts:

```ts
export type StepOutcomeEvent = StepSucceeded | StepFailed
```

`InvocationReplayFold` reconstructs the active runtime state:

```ts
export type OperationLogEvent =
  | InvocationStarted
  | InvocationSuspended
  | InvocationCompleted
  | InvocationFailed
  | InvocationCancelled
  | StepStarted
  | StepSucceeded
  | StepFailed
  | SleepScheduled
  | SleepFired
  | SignalWaitRegistered
  | SignalReceived
  | StateWaitRegistered
  | StateWaitSatisfied
  | DeferredCreated
  | DeferredResolved
  | DeferredRejected
  | ChildStarted
  | ChildCompleted
  | ChildFailed
  | StateSet
  | StateDeleted
```

`StepStarted` and attempt metadata are observability facts only. They must not
make replay skip an action. A crash after `StepStarted` and before
`StepSucceeded` / `StepFailed` makes the step eligible to run again.

## Signals, awakeables, deferreds, and channels

These primitives are intentionally distinct:

| Primitive   | Durable | Addressability         | Completion source                    |
| ----------- | ------- | ---------------------- | ------------------------------------ |
| `signal`    | yes     | stable name/invocation | another invocation or ingress        |
| `awakeable` | yes     | generated opaque id    | ingress client using the id          |
| `deferred`  | yes     | stable name/invocation | invocation/key-scoped operation code |
| `channel`   | no      | local object identity  | local routine inside one operation   |

`signal(name, schema)` is receiver-side:

```ts
const approval = yield * signal("approval", Approval)
```

Sender-side signaling happens through an invocation reference:

```ts
const ref = yield * send(reviewRequest, request)
yield * ref.signal("approval", Approval).resolve({ approved: true })
```

`awakeable` is for externally completed opaque handles:

```ts
const approval = yield * awakeable(Approval)

yield *
  run("notify-human", () =>
    sendEmail({ approveUrl: `/approve/${approval.id}` })
  )

const result = yield * approval.promise
```

`deferred` is a named invocation-scoped durable promise:

```ts
const done = yield * deferred("done", Approval)
yield * done.resolve({ approved: true })
const result = yield * done.get()
```

`channel` is only local coordination. It is not written to the operation log, is
not externally addressable, and does not survive replay:

```ts
const ch = yield * channel<Approval>()
yield * ch.send({ approved: true })
const value = yield * ch.receive
```

## State waits and webhook facts

`waitForState` is the durable wait for shared resource changes. It is backed by
`@durable-streams/state` events and server-side CEL-filtered subscriptions:

```ts
import { Duration, Effect, Schema } from "effect"
import { CEL } from "effect-durable-client/CEL"
import { waitForState } from "effect-durable-execution"

const Payment = Schema.Struct({
  id: Schema.String,
  status: Schema.Literal("pending", "captured", "failed"),
  stripeEventId: Schema.optional(Schema.String),
})

const payment =
  yield *
  waitForState(payments, {
    key: paymentId,
    filter: CEL.and(
      CEL.eq(CEL.path("value", "id"), paymentId),
      CEL.eq(CEL.path("value", "status"), "captured")
    ),
    schema: Payment,
    timeout: Duration.days(3),
  })
```

The primitive records a wait intent, relies on the client/server filtered
subscription substrate for wake delivery, then replays/materializes the state
fact before returning. It is not a local polling loop.

Webhook ingress is host code that writes facts into the substrate:

```ts
const stripeWebhook = Effect.gen(function* () {
  const event = yield* verifyStripeWebhook(StripeEvent)
  const payments = client.stream("state/payments", Payment)
  const producer = yield* payments.producer("stripe-webhook")

  yield* producer.append({
    id: event.paymentId,
    status: "captured",
    stripeEventId: event.id,
  })
})
```

No separate webhook wait backend is needed. A webhook can append a typed
event/state fact, or resolve an awakeable if the product deliberately chose an
opaque task-token flow. `waitForState` only resumes after the durable append can
be replayed and materialized.

Host platforms can make this more agent-friendly by reifying event sources as
state-backed observation sources: a manifest row describes the source, its stream
path, buckets, and named filters; incoming provider payloads append rows to a
typed collection such as `webhook_event`; tools expose list/subscribe/unsubscribe
operations that write those manifest/subscription facts. That is useful
platform ergonomics, but the handler-facing primitive remains `waitForState`.

## Local concurrency

Use Effect's native concurrency by default:

```ts
const result =
  yield *
  Effect.all(
    [
      run("fetch-user", fetchUser(id), { output: User }),
      run("fetch-policy", fetchPolicy(id), { output: Policy }),
    ],
    { concurrency: 2 }
  )
```

Local fibers are scoped Effect fibers. They do not create durable child
invocations:

```ts
yield *
  Effect.scoped(
    Effect.gen(function* () {
      const fiber = yield* Effect.forkScoped(
        run("background-refresh", refreshCache(), { output: Schema.Void })
      )

      return yield* Fiber.join(fiber)
    })
  )
```

Durable child invocation semantics require a separate primitive because they
have different persistence, cancellation, attachment, and signal behavior.

`select` may be useful for tagged waits across durable handles:

```ts
const winner =
  yield *
  select({
    approval: signal("approval", Approval),
    timeout: sleep("approval-timeout", Duration.days(3)),
  })

switch (winner.tag) {
  case "approval":
    return yield * winner.effect
  case "timeout":
    return { approved: false, reason: "timeout" }
}
```

The helper should be implemented in terms of Effect semantics and durable
primitive handles. It should not introduce positional durable counters.

## Schema boundary

All public serialization boundaries use Effect Schema:

- handler input/output;
- step success/failure;
- signal payloads;
- awakeable payloads;
- deferred payloads;
- state values; and
- invocation reference result attachment.

The runtime stores encoded values. Custom wire/storage formats are expressed as
Schema transformations, not a parallel serde API.

## Invocation metadata and projections

The package models invocation metadata. It does not model application sessions.
Hosts may project invocations into their own session, job, request, run, or
agent-thread concepts.

```ts
export type InvocationEvent =
  | InvocationScheduled
  | InvocationInboxed
  | InvocationRunning
  | InvocationSuspended
  | InvocationCompleted
  | InvocationFailed
  | InvocationCancelled
  | InvocationTimedOut

export interface InvocationMetadata {
  readonly invocationId: string
  readonly handler: string
  readonly source?: string
  readonly idempotencyKey?: string
  readonly operationLogStreamPath: string
  readonly operationLogWatermark?: Offset
  readonly activation?: {
    readonly subscriptionId?: string
    readonly wakeId?: string
    readonly generation: number
  }
  readonly currentWait?: {
    readonly reason:
      | "sleep"
      | "signal"
      | "deferred"
      | "state"
      | "child"
      | "external"
    readonly correlationKey?: string
    readonly scheduleId?: string
    readonly subscriptionId?: string
  }
}

export interface StepMetadata {
  readonly invocationId: string
  readonly stepKey: string
  readonly status: "running" | "succeeded" | "failed" | "suspended"
  readonly attempt: number
  readonly startedAt: string
  readonly completedAt?: string
  readonly idempotencyKey: string
}
```

`@durable-streams/state` projections are query/read views over operation-log
facts. They are useful for dashboards, human-in-the-loop queues, and host
indexes, but they are not authoritative for replay.

## Host and client split

Handler-facing primitives are available only inside an active operation runtime.
Ingress clients are separate:

- call or send a handler from outside an operation;
- resolve or reject an awakeable by id;
- send a signal to an invocation reference;
- attach to a result when the host exposes that transport; and
- cancel by invocation id or reference.

Host/control-plane APIs such as `start`, `status`, `list`, `pause`, `resume`,
`restart`, and `delete` are outside this package. A platform can build them on
top of handler metadata, invocation references, state projections, and Durable
Streams substrate facts.

## Cancellation

Cancellation is delivered at durable boundaries, not mid-statement. `run`
actions receive an `AbortSignal` when the action shape supports it:

```ts
yield *
  run("fetch-user", ({ signal }) => fetchUser(id, { signal }), {
    output: User,
  })
```

If cancellation arrives while an action is running, the wrapper should prefer
the canonical durable cancellation outcome over incidental abort errors. Cleanup
that performs durable work is ordinary Effect code:

```ts
Effect.gen(function* () {
  try {
    return yield* run("charge", chargeCard(), { output: ChargeResult })
  } catch (error) {
    yield* run("audit-cancel", audit(error), { output: Schema.Void })
    return yield* Effect.fail(error)
  }
})
```

## Replay and delivery

Wake delivery is at-least-once. Effective once execution comes from ordering:

1. claim a wake or activation;
2. open the operation runtime;
3. read the operation log to tail;
4. replay invocation and step facts;
5. evaluate only missing work;
6. append `InvocationCompleted`, `InvocationSuspended`, or `InvocationFailed`;
   and
7. ack the wake only after the durable outcome or suspension intent exists.

If the host crashes before ack, redelivery replays the same durable facts and
skips completed named steps.

## Single-writer fencing

The execution engine should use one operation-log stream per invocation and a
single active writer for that log. That does not mean no fencing. It means the
operation-log producer epoch is the write-authority fence.

Activation:

1. claim a wake for invocation `I`;
2. open the invocation operation-log producer;
3. auto-claim the current producer epoch;
4. read the operation log to tail;
5. replay invocation and step facts;
6. evaluate missing work;
7. append through the epoch-claimed producer; and
8. ack the wake only after the durable outcome or suspension intent exists.

A stale executor with an older epoch can still run local code, but its append
must fail as stale. Subscription generation remains useful for wake delivery and
stale ack handling; it is not the operation-log correctness fence.

Producer sequence still matters. Epoch rejects stale writers. Sequence
deduplicates retry-ambiguous appends from the current writer. Both are required.

The server must commit operation-log append, producer epoch state, and producer
sequence state atomically. Without that, the single-writer guarantee degrades to
advisory fencing and external effects must rely on idempotency keys or durable
intent/reconciliation.

Concrete runtime shape:

```ts
import { Effect, Either, Option, Schema, Scope } from "effect"

export interface InvocationWake {
  readonly invocationId: string
  readonly wakeId: string
  readonly subscriptionId: string
  readonly generation: number
}

export interface FencedProducer {
  readonly producerId: string
  readonly epoch: number
  readonly append: (
    event: OperationLogEvent
  ) => Effect.Effect<Offset, StaleWriter | AppendFailed>
}

export interface InvocationOperationLog {
  readonly streamPath: string
  readonly readToTail: Effect.Effect<
    ReadonlyArray<OperationLogEvent>,
    ReadFailed
  >
  readonly claimWriter: (
    producerId: string
  ) => Effect.Effect<FencedProducer, ClaimWriterFailed, Scope.Scope>
}

export const activateInvocation = <A, E, R>(input: {
  readonly wake: InvocationWake
  readonly handler: Effect.Effect<A, E, R | DurableExecutionRuntime>
}) =>
  Effect.scoped(
    Effect.gen(function* () {
      const log = yield* InvocationLogs.open(input.wake.invocationId)

      // This is the write-authority transfer. A newer activation claiming the
      // same producer id bumps the epoch and makes older producers stale.
      const producer = yield* log.claimWriter(
        `invocation:${input.wake.invocationId}`
      )

      const events = yield* log.readToTail
      const replay = InvocationReplayFold.from(events)

      const runtime = yield* DurableExecutionRuntime.make({
        invocationId: input.wake.invocationId,
        replay,
        append: producer.append,
      })

      const exit = yield* input.handler.pipe(
        Effect.provideService(DurableExecutionRuntime, runtime),
        Effect.either
      )

      yield* appendTerminalOutcome(producer, exit)

      // Ack is intentionally last. If the process dies before this, wake
      // redelivery replays the terminal outcome and does not re-run completed
      // steps.
      yield* PullWake.ack({
        subscriptionId: input.wake.subscriptionId,
        wakeId: input.wake.wakeId,
        generation: input.wake.generation,
        done: true,
      })

      return yield* Effect.fromEither(exit)
    })
  )

const appendTerminalOutcome = <A, E>(
  producer: FencedProducer,
  exit: Either.Either<A, E>
) =>
  Either.match(exit, {
    onRight: (value) =>
      producer.append({
        _tag: "InvocationCompleted",
        value,
      }),
    onLeft: (error) =>
      producer.append({
        _tag: "InvocationFailed",
        error,
      }),
  })
```

The `run` primitive is just a replay check plus a fenced append:

```ts
export const run = <A, E, R, EncodedA, EncodedE>(
  key: string,
  action: Effect.Effect<A, E, R> | RunAction<A, E, R>,
  options: RunOptions<A, E, EncodedA, EncodedE> = {}
): Effect.Effect<A, E | DurableExecutionError, R | DurableExecutionRuntime> =>
  Effect.gen(function* () {
    const runtime = yield* DurableExecutionRuntime

    const recorded = runtime.replay.step(key)
    if (Option.isSome(recorded)) {
      return yield* decodeRecordedStep(recorded.value, options)
    }

    yield* runtime.append({
      _tag: "StepStarted",
      key,
      attempt: runtime.nextAttempt(key),
    })

    const result = yield* executeRunAction(action, options).pipe(Effect.either)

    if (Either.isRight(result)) {
      const encoded = yield* encodeStepSuccess(result.right, options.output)
      yield* runtime.append({
        _tag: "StepSucceeded",
        key,
        value: encoded,
      })
      return result.right
    }

    const encoded = yield* encodeStepFailure(result.left, options.error)
    yield* runtime.append({
      _tag: "StepFailed",
      key,
      error: encoded,
    })
    return yield* Effect.fail(result.left)
  })
```

And the stale-writer conformance shape is small:

```ts
it.effect(
  "effect-execution.CONFORMANCE.26 stale writer append is rejected by producer epoch",
  () =>
    Effect.gen(function* () {
      const log = yield* TestInvocationLog.make("invocation-1")

      const oldWriter = yield* log.claimWriter("invocation:invocation-1")
      const newWriter = yield* log.claimWriter("invocation:invocation-1")

      yield* newWriter.append({ _tag: "StepStarted", key: "new", attempt: 1 })

      const stale = yield* oldWriter
        .append({ _tag: "StepStarted", key: "old", attempt: 1 })
        .pipe(Effect.flip)

      assert.strictEqual(stale._tag, "StaleWriter")
    })
)

it.effect(
  "effect-execution.CONFORMANCE.27 retry-ambiguous append is deduplicated by producer sequence",
  () =>
    Effect.gen(function* () {
      const log = yield* TestInvocationLog.make("invocation-1")
      const writer = yield* log.claimWriter("invocation:invocation-1")

      const event = {
        _tag: "StepSucceeded",
        key: "charge-card",
        value: { chargeId: "ch_1" },
      } as const

      const first = yield* writer.appendAtSequenceForTest({
        seq: 0,
        event,
      })

      // Same epoch + same seq + same bytes models "HTTP response was lost;
      // client retries the append it cannot tell already committed."
      const retry = yield* writer.appendAtSequenceForTest({
        seq: 0,
        event,
      })

      assert.strictEqual(retry, first)
      assert.deepStrictEqual(yield* log.readToTail, [event])
    })
)
```

## Testing

Conformance should prove:

- public programs are ordinary lazy `Effect.Effect` values;
- no public `currentOps` or custom `Operation` escape hatch is required;
- free primitives fail outside an active runtime and delegate inside one;
- `run` skips recorded successes and replays typed failures;
- `StepStarted` alone does not skip execution;
- Effect Schema round-trips every public value boundary;
- `channel` is local-only and appends no durable facts;
- local Effect composition introduces no positional durable counters;
- operation-log append uses the client producer resource;
- stale writers are rejected by producer epoch;
- retry-ambiguous appends from the current writer are deduplicated by producer
  sequence;
- `waitForState` lowers to filtered subscription plus replay/materialization;
- webhook-originated facts resume matching waits only after durable append;
- sleep, signal, awakeable, deferred, state-change wait, and channel keep
  distinct identity, scope, durability, and completion semantics; and
- wake redelivery after crash does not re-run completed steps.

The package-level tests can run without a server for replay folds and authoring
surface checks. Client/server integration conformance is required before
claiming substrate correctness.
