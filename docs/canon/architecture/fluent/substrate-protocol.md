# Fluent Substrate Protocol Mapping

Doc-Class: canon
Status: superseded
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> Superseded: this page specifies a fluent lowering against an idealized Durable
> Streams substrate with named consumers, leases, claim/ack/release, webhook
> wake, and substrate-side coordination features. The current EffSharp/S2
> implementation uses a smaller set of proven S2 primitives. Use
> [`s2-substrate.md`](s2-substrate.md) for current implementation guidance.

This document maps fluent durable coordination onto Durable Streams operations.
It is the canonical version of the "deferred features wired to the protocol"
design: durable waits, timers, child sessions, attach/query, fork, and GC are
not separate engines. They are specific uses of Durable Streams append/read,
closure, fork, producer fencing, named consumers, pull-wake, claim/ack/release,
and TTL.

`packages/fluent` defines the authoring primitives over a `Journal`.
`packages/fluent-runtime` provides that journal and operates the wake/redrive
loop. Durable Streams owns the transport guarantees.

This document is the wire-level mapping for the shared coordination core. It is
used by both fluent execution models:

| Execution model | First encounter | Continuation after wake |
|---|---|---|
| Authored procedure | a fluent primitive appends intent and parks the Effect body | replay the Effect body; journal hits carry it past the park |
| Managed session | a durable tool call appends Layer 1 evidence plus Layer 2 intent/result or park | reconstruct native harness state and suppress already-observed side effects |

The Durable Streams protocol path is shared. The continuation strategy is not.
The explicit contrast lives in [`execution-models.md`](execution-models.md).

## Sync And Async Durable Primitives

The authored-procedure surface has two classes of primitive.

**Sync primitive:** `run` executes now, records the terminal outcome, and keeps
the worker resident.

**Async primitives:** `sleep`, `awaitEvent` / `wait_for`, and `invoke` register
durable intent, park, and resume later from a wake. The worker may exit between
park and wake.

```ts
run(name, effect, { value, error })

sleep(name, duration)
sleepUntil(name, instant)

awaitEvent(name, {
  match: "event.type == 'review.decided' && event.value.id == self.id",
  schema: Decision,
  timeout: "48 hours",
})

invoke(name, childHandler, input)
```

Every primitive resolves through the journal. In a managed session, the same
resolution appears as a Layer 2 session fact rather than as a user-authored
function return value.

| Primitive | Journal resolution | First encounter registers | Who writes the resolution |
|---|---|---|---|
| `run` | `StepSucceeded` / `StepFailed` | nothing; runs the Effect now | current process |
| `sleep` / `sleepUntil` | `StepSucceeded{void}` / `timer_fired` | timer intent + scheduled append source | wake resumer |
| `awaitEvent` / `wait_for` | `StepSucceeded{event}` / `wait_matched` or timeout | pull-wake subscription over signal stream | wake resumer |
| `invoke` / durable child | `StepSucceeded{child exit}` / `child_terminal` | child stream/session + subscription to terminal | wake resumer |

## Authored Procedure: Resolve-Or-Suspend

Async authored-procedure primitives share a two-phase shape:

1. Look for an existing journal resolution.
2. If present, decode and return it.
3. If absent, append intent / suspended state.
4. Register the protocol artifact that can wake the invocation.
5. Park by returning a parked outcome to the host.
6. On wake, the resumer appends the resolution.
7. Re-drive the same handler; the primitive is now a journal hit.

```text
first drive:
  handler reaches awaitEvent("review")
  -> journal has no review resolution
  -> append WaitIntent / Suspended
  -> register Durable Streams subscription
  -> catch up once to close the lost-wakeup window
  -> return Parked(awaiting="review")

wake drive:
  Durable Streams grants claim
  -> runtime reads events past acked_offset
  -> predicate matches
  -> append StepSucceeded("review", matched event)
  -> re-run handler
  -> awaitEvent("review") reads journal hit and returns
```

The park signal is not a handler domain error. It is host control flow. Whether
it is represented as a defect, interruption reason, or internal exit type is an
implementation detail; it must not leak into the user's `E` channel.

## Managed Session: Reconstruct-Or-Resume

Managed sessions use the same wake and resolution path, but they do not replay a
model loop. The parked unit is a native harness turn or durable tool exchange.

```text
first turn:
  harness emits tool_call(wait_for)
  -> Firegrid records Layer 1 tool-call evidence
  -> runtime appends WaitIntent / parked Layer 2 fact
  -> harness turn ends or pauses

wake drive:
  Durable Streams grants claim
  -> runtime reads events past acked_offset
  -> predicate matches
  -> append wait_matched Layer 2 resolution
  -> harness I/O reconstructs native resume artifact
  -> adapter/conductor feeds committed tool result back to the harness
```

The safety rule is stronger than "do not re-run Firegrid tools." Resume must not
re-execute any already-observed Layer 1 side effect, including shell commands,
file edits, tests, or harness-native tools that Firegrid did not mediate.

## 1. Suspend And Resume Loop

Durable Streams pull-wake subscriptions are the redrive substrate. A
subscription writes a wake event when a linked stream has data beyond the
subscription cursor. `claim`, `ack`, and `release` are fenced by generation and
lease.

One-time setup:

```text
PUT {root}/wake/pool
```

Suspend an invocation waiting on a signal stream:

```text
POST {root}/inv/42
  [{ "_t": "Suspended", "awaiting": "signals" }]

PUT {root}/__ds/subscriptions/wait:42
  {
    "type": "pull-wake",
    "streams": ["inv/42/signals"],
    "wake_stream": "wake/pool",
    "lease_ttl_ms": 30000
  }

GET {root}/inv/42/signals?offset=<lastSeen>
```

The final `GET` is required when a lost wakeup is possible. A subscription links
at the stream's current tail. An event that landed between the last read and the
subscription creation will not produce a wake. Catch up once after subscribing;
if the condition is already satisfied, resolve immediately instead of parking.

External event:

```text
POST {root}/inv/42/signals
  [{ "type": "approval", "value": { ... } }]

# Durable Streams appends a wake to wake/pool.
```

Worker claim and redrive:

```text
GET  {root}/wake/pool?offset=<cursor>&live=sse

POST {root}/__ds/subscriptions/wait:42/claim
  { "worker": "w3" }

200 {
  "wake_id": "w_x",
  "generation": 7,
  "token": "...",
  "streams": [
    {
      "path": "inv/42/signals",
      "acked_offset": "...",
      "tail_offset": "...",
      "has_pending": true
    }
  ]
}

GET  {root}/inv/42/signals?offset=<acked_offset>
GET  {root}/inv/42?offset=-1

# Runtime appends the Layer 2 resolution before acking done:
#   authored procedure: StepSucceeded{key, value}
#   managed session: wait_matched / timer_fired / child_terminal / tool_result

POST {root}/__ds/subscriptions/wait:42/ack
  { "wake_id": "w_x", "generation": 7 }

POST {root}/__ds/subscriptions/wait:42/ack
  {
    "wake_id": "w_x",
    "generation": 7,
    "acks": [{ "stream": "inv/42/signals", "offset": "<consumed>" }],
    "done": true
  }
```

The first ack can extend the lease while redrive is still working. The `done`
ack advances the cursor only after the product outcome is durable.

## 2. Two Fences

Wake redrive has two independent fences.

| Fence | Owned by | Protects |
|---|---|---|
| subscription generation / claim token | Durable Streams subscription plane | cursor movement, ack, release, lease ownership |
| producer epoch | Durable Streams producer fencing | journal writes by the active attempt |

A zombie worker with a stale generation cannot ack or release the subscription.
A zombie writer with a stale producer epoch cannot append to the journal. Fluent
must use these substrate fences rather than adding a parallel task-claim table.

## 3. Durable Wait

Durable wait is the suspend/resume loop with a predicate.

1. Append `WaitIntent` / parked state before the harness turn ends.
2. Register a pull-wake subscription on the signal stream.
3. Catch up after subscription creation.
4. On wake claim, read events past `acked_offset`.
5. Evaluate CEL over each candidate event plus recorded `self`.
6. If no event matches, ack consumed non-matching offsets and remain subscribed.
7. If an event matches, append a recorded match / resolution.
8. Continue the execution model:
   - authored procedure: re-drive; replay serves the recorded match;
   - managed session: reconstruct/resume; the committed tool result is fed back.

In both cases, the recorded match is served and the runtime does not re-evaluate
a moving world after the result is committed.

The wait CEL environment is runtime data, not the trace oracle environment:

| Surface | Evaluated by | Binds |
|---|---|---|
| Wait predicate CEL | fluent-runtime during wake redrive | `event`, `self` |
| Firelab coverage CEL | firelab oracle after a run | `spans` |

`self` is the waiting session's recorded correlation data. It must be recorded
with the wait or resolvable from a recorded reference. Do not re-derive it from a
newer projection after the wake.

Timeout is a durable timer racing the event wait. Whichever resolution is
recorded first wins replay.

## 4. Durable Timer And Cron

Subscriptions wake on append, not on wall time. A durable timer therefore needs
one app-side clock edge: append at T.

```text
POST {root}/inv/42
  [{ "_t": "TimerScheduled", "key": "gap:1", "fireAt": "<T>" }]

PUT {root}/__ds/subscriptions/timer:42
  {
    "type": "pull-wake",
    "streams": ["timers/42"],
    "wake_stream": "wake/pool"
  }

# At T, the timer source appends:

POST {root}/timers/42
  [{ "fire": "<T>" }]
```

Everything after the append-at-T is the normal wake claim/redrive path. The
timer source does not need to know who is waiting or how to resume the session.
That information lives in the invocation/session journal.

At scale, timers can be bucketed:

```text
timers/2026-06-07T12:35
```

One scheduled append wakes a bucket, and the runtime resolves due timers whose
journaled `fireAt` is now eligible.

## 5. Durable Child / Invoke

A durable child that must survive parent crash is its own stream/session, not a
local Effect fiber.

1. Parent appends `ChildSpawned { childId }`.
2. Child stream/session is created and driven independently.
3. Parent subscribes to the child's terminal stream state.
4. Child completes by appending terminal result and closing atomically.
5. Child closure wakes the parent.
6. Parent redrive reads the child's terminal event and `Stream-Closed`, appends
   the recorded child resolution, and continues.

```text
POST {root}/inv/child-1
  [{ "_t": "InvocationSettled", "exit": { ... } }]
  Stream-Closed: true
```

Cancellation is also stream-mediated: append a cancel event to the child's
control stream or close the child stream according to the child policy. The
child's own redrive observes that fact and records terminal state.

## 6. Completion And Attach

Stream closure is the terminal signal. `Stream-Up-To-Date` is not EOF.

Complete by appending terminal state and closing in one operation:

```text
POST {root}/inv/42
  [{ "_t": "InvocationSettled", "exit": { ... } }]
  Stream-Closed: true
```

Attach to progress or result with live read:

```text
GET {root}/inv/42?offset=-1&live=sse
```

If running, the read streams events until the close control frame. If already
closed, the server replays history and closes immediately. A `HEAD` can check
terminal state cheaply when the body is not needed.

## 7. Identity, Dedup, And Single Writer

Invocation/session identity is stream identity.

```text
PUT {root}/inv/42
```

`PUT` is idempotent for matching config. Submitting the same invocation id twice
collapses onto one stream without a side table.

Idempotency over input can be expressed by deriving the stream id from a stable
key, such as a CEL expression over input. TTL bounds how long that idempotency
record lives.

Single-writer fencing is producer epoch. A recovering attempt bumps epoch before
its first write. Older writers are fenced by the server.

## 8. Fork And Tags

Fork is copy-free branch-from-prefix.

```text
PUT {root}/inv/42-branch-a
  Stream-Forked-From: /v1/stream/{root}/inv/42
  Stream-Fork-Offset: <decisionOffset>
```

Branches inherit stream data up to the fork offset and then diverge. Forks do
not inherit producer state, so each branch reboots its producer epoch. Keyed
replay tolerates this because journal identity is by step key, not producer
sequence.

Tags are named offsets. They are control-plane/read-model facts that let a
client or harness say "fork from here" without exposing raw offsets as the only
user-level handle.

## 9. TTL And GC

Create streams with sliding TTL or explicit expiry:

```text
PUT {root}/inv/42
  Stream-TTL: 604800
```

Reads and writes keep active streams warm; abandoned streams expire without a
sweeper. Audit-retained sessions use longer TTL or `Stream-Expires-At`.

## 10. Control Plane Mapping

The external control plane is Durable Streams product spelling.

| Operation | Protocol mapping |
|---|---|
| `send` | append addressed input or state fact |
| `read` | catch-up read and projection |
| `head` | stream head metadata |
| `tag` | record a named offset |
| `fork` | `PUT` with `Stream-Forked-From` / `Stream-Fork-Offset` |
| `schedule` | timer intent plus append-at-T source |
| `delete` | stream close/delete according to substrate rules |

The control plane must not synchronously call handlers. It appends or reads
durable facts. Delivery happens through the wake/redrive path.

## What Fluent Builds

Durable Streams gives:

- append/read/tail/close/fork/TTL;
- idempotent create;
- producer fencing;
- named consumers;
- pull-wake/webhook wake;
- claim/ack/release, leases, retry, stale-generation fencing;
- signed subscription-webhook delivery.

Fluent builds only:

- product facts and schemas;
- predicates, including CEL wait predicates;
- the app-side timer append source;
- the post-claim redrive function;
- harness I/O adapters and native resume/replay-suppression;
- projections and control-plane spelling.

If a proposed implementation adds a Firegrid lease table, cursor store,
subscription retry loop, Durable Streams subscription-webhook signature
verifier, or task-claim lock, it is probably rebuilding substrate. This does not
forbid product/provider verification before appending an admitted state fact.
