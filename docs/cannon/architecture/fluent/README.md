# Fluent Firegrid

Fluent Firegrid is the architecture for Effect-native durable coordination over
Durable Streams. It gives Firegrid a lean substrate for authored durable
procedures, managed-agent durable tools, wake/redrive, projections, and
cross-harness choreography without rebuilding a workflow engine or hiding the
agent loop inside one.

The key inversion is:

- **Durable Streams** owns state and the durable half of coordination.
- **Effect** owns computation and local structured concurrency.
- **`packages/fluent-firegrid`** defines the authoring surface over a journal.
- **`packages/fluent-runtime`** provides that journal over Durable Streams and
  operates wake/redrive for sessions and durable tools.
- **Harness I/O roles** adapt ACP, native, cloud, and model-provider protocols
  while raw harnesses keep their native model loops.

The architecture is not "a second runtime" beside Durable Streams. It is a thin
durable coordination layer that uses Durable Streams' append, read, close, fork,
producer fencing, named consumers, pull-wake, webhook wake, lease, ack, retry,
and TTL primitives directly.

## Two Models, One Core

The fluent architecture has two execution models over one Durable Streams
coordination core.

| Model | Used for | Continues after a wake by | Why |
|---|---|---|---|
| **Authored procedure** | Effect handlers, sagas, durable tool implementations, coordination workflows | replaying the Effect body and serving journal hits | Firegrid owns the body and can make its durable boundary deterministic |
| **Managed session** | Claude ACP, Codex ACP, native/cloud agents, editor/conductor paths | reconstructing native harness state and suppressing already-observed side effects | the external harness owns a non-deterministic model loop |

Both models use the same durable loop:

```text
record intent before park
  -> Durable Streams wake source
  -> claim
  -> materialize stream facts
  -> append one Layer 2 resolution fact
  -> continue by replay or reconstruction
  -> ack/done after the durable append
```

That distinction is the spine of the design. Authored procedures are replayed.
Managed sessions are reconstructed. The wake, claim, materialization, resolution,
and ack path is shared.

See [`execution-models.md`](execution-models.md) for the full contract.

## Providers And Roles

The fluent stack spans two providers and three roles.

```text
STATE                         COORDINATION                       COMPUTATION
what happened                 who runs next, when                running the body

Durable Streams               Durable Streams                    Effect
  append/read/tail              subscriptions/pull-wake            fibers/scopes
  offsets/catch-up              claim/lease/ack/retry              all/race/fork
  close/fork/TTL                producer/generation fencing        interruption/finalizers
  producer epoch                webhook wake                       Schema/Clock/Random

          ┌─────────────────────────────────────────────────────────────┐
          │ packages/fluent-firegrid                                    │
          │ process-free authoring: run, keyed replay, durable          │
          │ primitives, combinators, descriptors, typed definitions      │
          └──────────────────────────┬──────────────────────────────────┘
                                     │ Journal service
          ┌──────────────────────────▼──────────────────────────────────┐
          │ packages/fluent-runtime                                     │
          │ host/session authority: DS-backed Journal, wake redrive,     │
          │ waits, timers, child/session semantics, control/MCP surface  │
          └───────────────┬───────────────────────────────┬─────────────┘
                          │ append/read/claim/ack          │ drive/resume
                          ▼                                ▼
               Durable Streams session logs        Harness I/O roles
               L1 observations + L2 facts          ACP/native/cloud/AI
                                                        │
                                                        ▼
                                                 external harness
                                                 owns model loop
```

The package split follows the role split:

| Layer | Owns | Must not own |
|---|---|---|
| `fluent-firegrid` | Authoring surface, `run`, keyed replay, durable primitive definitions, local Effect composition | Durable Streams clients, leases, worker pools, HTTP/MCP hosts |
| `fluent-runtime` | DS-backed journal, session facts, wait/timer/child/tool semantics, wake redrive, control/MCP surfaces | Raw model loop, DS lease/cursor implementation, provider webhook retry |
| Durable Streams | Storage, offsets, closure, fork, TTL, producer fencing, consumer cursor, claim/ack/release, retry, subscription-webhook wake/signing | Firegrid product semantics, CEL predicates, harness protocol fidelity |
| Harness I/O | Protocol fidelity, Layer 1 recording, native resume artifacts, replay suppression | Wait matching, timer firing, child lifecycle, committed tool-result authority |
| Raw harness | Reasoning/model loop and native side effects | Durable Streams writes, Firegrid coordination decisions |

Three "layer" vocabularies appear in fluent design, and they are orthogonal:

- **Roles:** state, coordination, computation.
- **Substrate levels:** Durable Streams protocol, effect-durable-streams client
  helpers, fluent-runtime product semantics.
- **Session facts:** Layer 1 harness observations and Layer 2 coordination
  facts.

## State, Coordination, Computation

Durable Streams is more than a log in this design. It supplies the substrate
properties that make the runtime small:

- catch-up reads from any offset;
- live tailing over SSE or long-poll;
- stream closure as durable EOF;
- copy-free fork from a prefix;
- producer id / epoch / seq fencing;
- named consumers, wake delivery, claim, ack, release, leases, retry;
- subscription-webhook signing and delivery;
- stream TTL and expiry.

Effect supplies the computation model that Restate-shaped generator schedulers
normally have to rebuild:

- lazy `Effect` values instead of bespoke `Operation`;
- eager `Fiber` handles instead of bespoke `Future`;
- `Effect.all`, `race`, `fork`, `forkScoped`, and scopes;
- interruption with finalizers instead of manual cancellation fan-out;
- `Layer`/`Context` for journal and host services;
- `Schema`, `Clock`, and `Random` as controlled boundary services.

Fluent's job is to join those two providers without losing either side's
strengths.

## Deployment Topology

The target fluent deployment is runtime-fronted. External apps, editors,
providers, peers, and harnesses call Firegrid-owned ingress surfaces. They do
not write Firegrid coordination streams directly.

```text
external app / UI / editor / provider / peer
      │
      │ Firegrid API · ACP · MCP · webhook · source adapter
      ▼
┌──────────────────────────────────────────────────────────────────────┐
│ packages/fluent-runtime                                              │
│ Firegrid host/session authority                                      │
│ - terminates product ingress and adapter callbacks                   │
│ - owns Durable Streams substrate clients for Firegrid coordination    │
│ - supplies Journal to authored Effect bodies                         │
│ - resumes managed harness sessions through Harness I/O roles          │
└───────────────┬───────────────────────────────────────┬──────────────┘
                │ DS append/read/claim/ack              │ native protocol
                ▼                                       ▼
┌──────────────────────────────┐        ┌──────────────────────────────┐
│ Durable Streams substrate     │        │ authored body or raw harness │
│ log, wake, lease, fencing      │        │ no Durable Streams writes     │
└──────────────────────────────┘        └──────────────────────────────┘
```

This is proxy-shaped in the durable-execution sense: a durable broker/runtime is
in front of handlers and harnesses for invocation, journaling, wake, retry, and
fencing. The split is narrower than a monolithic workflow engine:

- Durable Streams is the durable broker and substrate system of record.
- `packages/fluent-runtime` is the Firegrid semantics host in front of that
  broker.
- `packages/fluent-firegrid` is the authoring SDK over `Journal`; it does not
  import Durable Streams or own listeners, workers, leases, or cursors.
- Raw harnesses and process owners speak native protocols only; they do not
  import Durable Streams or write session facts directly.

An embedded or in-process host is allowed for local development, tests, or a
single-process deployment, but it is still running `packages/fluent-runtime` as
the host. The package ownership and Durable-Streams-client-free contracts for
authored code, clients, process owners, and raw harnesses do not change.

`@durable-streams/proxy` is a separate transport edge for resumable upstream
HTTP streams, such as AI token streams or SSE feeds. Fluent may use it inside a
LanguageModel, native, or cloud adapter, but it does not own waits, timers,
children, replay, redrive, tool results, or session authority.

See [`architecture.md`](architecture.md#deployment-topology) for the full system
contracts and proxy-based durable execution comparison.

## Concepts Mapped To The Substrate

| Concept | Fluent spelling | Durable Streams / Effect mechanism |
|---|---|---|
| Durable step | `run(name, effect, { value, error })` | append schema-encoded `StepSucceeded` / `StepFailed` |
| Replay | keyed lookup by `stepKey` | catch-up `GET ?offset=-1`, decode from schema |
| Parallel work | `Effect.all`, `Effect.fork` | local Effect fibers; no journal I/O except leaf `run`s |
| Race | `Effect.race` plus explicit winner policy when durable | Effect interruption locally; journal winner where replay must be stable |
| Time and randomness | journaled `Clock` / `Random` layers | recorded readings in the journal |
| Durable sleep | `sleep` / `sleepUntil` | timer intent + scheduled append + pull-wake redrive |
| Durable wait | `awaitEvent` / `wait_for` | signal stream + CEL predicate + pull-wake claim |
| Child invocation/session | `invoke` or `spawn` at the coordination layer | child stream/session + terminal append-and-close + parent subscription |
| Completion / attach | live read or head of the journal/session stream | `GET ?live=sse`, `HEAD`, `Stream-Closed` |
| Branching | `fork` / tag from offset | Durable Streams fork from prefix; producer state resets |
| Idempotency | stream id, step key, producer id | idempotent `PUT`, keyed replay, producer fencing |
| GC | stream TTL / expiry | sliding `Stream-TTL`, `Stream-Expires-At` |

The exact wire sequences are in [`substrate-protocol.md`](substrate-protocol.md).

The "journal" is the coordination view of a Durable Streams stream. An authored
procedure mostly writes Layer 2 coordination facts. A managed session writes
Layer 1 harness observations and Layer 2 coordination facts to the same stream.
Likewise, `StepSucceeded { key, value }` is the generic authored-procedure
resolution form; managed-session facts such as `wait_matched`, `timer_fired`,
`child_terminal`, and `tool_result` are named members of the same Layer 2
resolution-fact family.

## `fluent-firegrid`: Durable Effect Authoring

`fluent-firegrid` is process-free. It is the authoring package and should be
usable anywhere an Effect program can be built. Its durable concern is the
`Journal` service, not a host process.

The core primitive is `run`: record a named Effect outcome, then replay that
outcome by key.

```ts
const publish = (draftId: string) =>
  Effect.gen(function* () {
    const review = yield* run("submit", submitForReview(draftId), {
      value: ReviewSubmitted,
      error: SubmitError,
    })
    return yield* run("notify", notifyReviewer(review.reviewerId), {
      value: NotificationSent,
    })
  })
```

Authoring invariants:

1. **Replay is keyed, not positional.** Effect concurrency makes positional
   replay unsound. Step keys are part of correctness.
2. **Duplicate step keys fail loudly.** Reusing a key must not silently replay
   another step's outcome.
3. **Schema is the journal boundary.** Values and typed errors are encoded before
   append and decoded on replay. Unknown payload plus `as A` is not acceptable.
4. **Retry belongs inside `run`.** Retrying around `run` replays the journaled
   failure; retrying the wrapped Effect journals only the terminal outcome.
5. **Compensation is ordinary Effect.** Use `onError`, `ensuring`, or
   `acquireRelease`, with compensating side effects wrapped in their own `run`.
6. **Local concurrency is Effect's.** `Effect.all`, `race`, `fork`, scopes, and
   interruption are not reimplemented by fluent.
7. **Durable child/session work is not local `fork`.** A local fiber is an
   in-process computation. A durable child is a stream/session coordination fact.

`fluent-firegrid` may expose service/object/workflow metadata and typed
definition helpers so `fluent-runtime` can bind external ingress later. It does
not open listeners, hold leases, operate worker pools, or directly expose the
external control plane.

## `fluent-runtime`: Durable Host And Session Authority

`fluent-runtime` is the processful side. It provides a DS-backed `Journal`,
drives authored handlers when appropriate, owns session facts, and operates
post-wake product semantics.

The runtime's durable operations are **state x coordination**:

1. register intent before park;
2. use Durable Streams subscriptions or stream operations to arrange wake;
3. on claim, read the provided offsets and materialize facts;
4. evaluate product semantics, such as CEL wait predicates;
5. append the Layer 2 outcome or resolution;
6. re-drive the handler/harness with the recorded result;
7. ack/done only after the durable result is appended.

`fluent-runtime` must not rebuild what Durable Streams already owns: lease
tables, cursor stores, pull queues, Durable Streams subscription-webhook
signing/key discovery/retry, competing claim arbitration, or stale-generation
fencing. Product-specific ingress verification, such as validating a GitHub
webhook before admitting it as a state fact, remains application semantics.

## Managed Agent Sessions

Managed agent sessions use the same Durable Streams substrate, but they are not
modeled as resident workflow bodies. A session is a stream of facts plus host
logic that reacts to wakes. It continues by reconstruction, not by replaying a
model loop.

```text
/support/ticket-42
  L1: user prompt, assistant text, tool call, file edit, turn complete
  L2: wait intent, wait matched, child spawned, tool result, terminal record
```

The raw harness owns the model loop. Firegrid-owned I/O roles observe or accept
native protocol traffic and write facts through `fluent-runtime`.

```text
external input / provider event / timer / child terminal
      │
      ▼
Durable Streams wake or append
      │
      ▼
fluent-runtime
  materialize stream -> evaluate product semantics -> append L2 outcome
      │
      ├── ack/done after durable append
      │
      └── drive or resume the relevant harness I/O role
                │
                ▼
         external harness keeps its native loop
```

This is where the architecture gets its leverage from the substrate: the session
does not need a second database or a workflow table. Layer 1 observations and
Layer 2 coordination facts share one stream, and every wake resolves by
appending another fact to that same durable surface.

Detailed harness roles are in [`harness-io.md`](harness-io.md).

## Durable Tools

Harnesses and authored procedures both rely on a small set of durable
coordination primitives:

- `wait_for` / `awaitEvent`: register a CEL predicate over candidate events,
  catch up after subscription creation, park, and resolve from a recorded match.
- `sleep` / `sleepUntil`: record timer intent, arrange an append-at-T source, and
  resolve from `TimerFired`.
- `spawn` / `invoke`: create or address a child stream/session, subscribe to its
  terminal append-and-close, and resolve the parent from the recorded child exit.
- `execute`: run an external activity and commit the result as a durable fact.
- `send`, `tag`, `fork`, `schedule`, `read`, `head`, `delete`: external control
  spelling over Durable Streams primitives, not direct handler calls.

The tool call itself is Layer 1 evidence. Firegrid's durable interpretation is
Layer 2 authority. The harness receives a committed result or the turn parks.

If a durable tool is implemented as an authored procedure, the implementation
runs as a child invocation on its own stream. The managed session records the
tool call, child spawn, and terminal tool result; it does not inline a replayable
Effect body into the reconstruction-model session stream.

## Control Plane

The external control plane is product spelling over Durable Streams. It should
not become a central scheduler.

| Control operation | Substrate meaning |
|---|---|
| `send` | append addressed input / state fact |
| `read` / `head` | derive projections from stream data and head metadata |
| `tag` | name an offset |
| `fork` | create a stream fork from a tagged/pinned offset |
| `schedule` | record a future append / wake source |
| `delete` | close/delete according to substrate terminal rules |

The causal path for acceptance tests is client ingress -> host ingress ->
runtime/store -> Durable Streams. A host self-call can be a package integration
test, but it is not feature acceptance.

## Choreography

Fluent supports choreography instead of a fixed workflow DAG. A harness can
choose at runtime to wait for a webhook, spawn a different harness, run a
sandboxed activity, observe another session, and continue after a wake.

```text
client prompt
  -> Claude ACP session receives the prompt
  -> Claude calls wait_for("github.pr.merged && repo == self.repo")
  -> Firegrid records wait intent and parks the turn
  -> GitHub webhook becomes a queryable state-change fact
  -> Durable Streams wakes fluent-runtime
  -> Firegrid records wait_matched and redrives the session
  -> Claude spawns a Codex child session to verify the merge
  -> Codex calls execute("sandbox", "pnpm test")
  -> Firegrid records the child terminal fact
  -> parent wakes and continues
```

No authored DAG coordinates this sequence. The harness chooses the next durable
tool. Firegrid makes each tool durable, observable, and recoverable.

## Safety Rules

The numbered safety invariants live in
[`architecture.md`](architecture.md#safety-invariants). This
README carries only the summary:

- raw harnesses do not write Durable Streams facts;
- parking records intent before turn end and closes lost-wakeup windows with
  catch-up reads;
- wake claims fence Layer 2 decision-writing, while producer epochs fence stream
  writes;
- redrive serves recorded matches/results rather than re-evaluating a moving
  world;
- ack/done happens only after the durable product outcome is appended;
- resume does not re-execute already-observed Layer 1 side effects;
- cancel/interrupt records durable terminal or continuation facts before process
  teardown;
- waits, timers, child joins, promises, and tool results do not use a second
  authoritative journal.

## Current Build Priorities

1. Keep Durable Streams consumer-substrate adoption green and pinned.
2. Prove post-claim redrive: claim -> materialize -> append L2 -> ack.
3. Implement DS-native durable wait with CEL and recorded matches.
4. Implement durable sleep through timer intent + append-at-T source + wake.
5. Stand up the thin control-plane ingress/projection host with client-to-host
   trace propagation.
6. Bind real ACP/native harnesses and prove no duplicate side effects on resume.
7. Expose durable tools through a thin Effect `Tool` / `Toolkit` / `McpServer`
   edge.
8. Prove cross-harness spawn: parent harness A spawns child harness B and wakes
   on child terminal state.
9. Fix the fluent-firegrid journal write path so concurrent Effect appends do
   not depend on unsafe producer sequence ordering.

## Read Next

- [`architecture.md`](architecture.md): canonical high-level package,
  deployment, process, stream ownership, and safety contract.
- [`execution-models.md`](execution-models.md): replay vs reconstruction over
  the shared Durable Streams coordination core.
- [`substrate-protocol.md`](substrate-protocol.md): concrete Durable Streams
  operation sequences for suspend/resume, wait, timer, child, attach, fork, and
  TTL.
- [`harness-io.md`](harness-io.md): ACP client/conductor, native/cloud, and
  LanguageModel I/O boundaries.
- [`../../../sdds/fluent-firegrid-sdd.md`](../../../sdds/fluent-firegrid-sdd.md):
  execution-focused design details and acceptance context.
