# Fluent Execution Models

Doc-Class: canon
Status: active
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: idealized

> S2 status: replay vs. reconstruction remains the target model split, but the
> wake/claim/ack language below is idealized. Current S2 host coordination is
> cooperative and proof-driven; see [`s2-substrate.md`](s2-substrate.md).

Fluent has two execution models over one Durable Streams coordination core.
They share wake delivery, claim/ack/release, producer fencing, CEL predicates,
resolution facts, and stream closure. They differ in how execution continues
after a wake.

The split is **replay vs reconstruction**.

## One Coordination Core

Every durable operation follows the same substrate loop:

```text
first encounter
  -> record intent before parking
  -> register or rely on a Durable Streams wake source
  -> return control to the host

wake
  -> Durable Streams grants a fenced claim
  -> fluent-runtime reads the provided offsets
  -> fluent-runtime materializes facts
  -> fluent-runtime appends one durable resolution fact
  -> host continues the appropriate execution model
  -> ack/done only after the resolution fact is durable
```

The core is not a queue, lock table, workflow table, or hidden scheduler inside
Firegrid. It is Durable Streams plus Firegrid product semantics.

## Model A: Authored Procedures Resume By Replay

Authored procedures are Effect programs written against `fluent`
primitives. They are replayable because the body is deterministic at the durable
boundary: completed steps are journal hits, not re-executed effects.

```text
Effect handler body
  yield* run("submit", ...)
  yield* awaitEvent("review", ...)
  yield* run("publish", ...)

first drive
  run("submit") appends a resolution fact
  awaitEvent("review") appends wait intent and parks

wake drive
  runtime appends wait resolution
  handler body re-runs
  run("submit") is a journal hit
  awaitEvent("review") is a journal hit
  run("publish") executes and appends
```

Replay is the right mechanism when Firegrid owns the body being re-run. The body
can be an authored handler, saga, coordination workflow, background procedure,
or durable tool implementation. It is not the managed-agent reasoning loop.

## Model B: Managed Sessions Resume By Reconstruction

Managed agent sessions are not replayed as Effect bodies. The raw harness owns a
non-deterministic model loop, and Firegrid must not pretend that loop can be
deterministically re-run.

Managed sessions continue by reconstruction:

```text
harness emits or receives protocol traffic
  -> Firegrid records Layer 1 observations
  -> harness calls a durable Firegrid tool
  -> Firegrid records Layer 2 intent/result or park

wake drive
  -> runtime materializes the session stream
  -> runtime appends the matching Layer 2 resolution
  -> harness I/O rebuilds the native resume artifact
  -> adapter/conductor resumes or re-enters the harness
  -> already-observed Layer 1 side effects are suppressed, not repeated
```

Reconstruction is the right mechanism when the execution engine is external:
Claude ACP, Codex ACP, native Claude Code, native Codex, a cloud agent, an ACP
editor/conductor path, or a future model-provider harness. Firegrid coordinates
around the loop. It does not own the loop.

## Shared Loop, Different Continuation

| Phase | Authored procedure | Managed session |
|---|---|---|
| First encounter | `sleep`, `awaitEvent`, or `invoke` appends intent and parks the Effect handler | durable tool call records Layer 1 evidence, appends Layer 2 intent/result or park, and ends/pauses the harness turn |
| Wake source | Durable Streams subscription, timer append source, webhook wake, child closure | same |
| Claim | `fluent-runtime` claims wake and reads offsets | same |
| Product decision | runtime resolves the awaiting primitive | runtime evaluates wait/timer/child/tool semantics for the session |
| Durable resolution | one resolution fact is appended to the stream | same |
| Continue | re-run the Effect handler; journal hits carry it past the park | reconstruct native resume artifact and resume/re-enter the harness |
| Safety rule | do not re-execute completed `run` effects | do not re-execute already-observed Layer 1 side effects |

This is the central architecture: **two execution models, one coordination
core**.

## Durable Tools As Authored Procedures

A managed session may call a Firegrid durable tool whose implementation is an
authored procedure. That composition is always a **child invocation**, never an
inline replay body on the managed-session stream.

```text
managed session stream
  L1: tool_call(execute_review)
  L2: ChildSpawned(toolInvocationId -> child stream)
  L2: tool_result / child_terminal

child authored-procedure stream
  L2: StepSucceeded(submit)
  L2: WaitIntent(review)
  L2: StepSucceeded(review)
  L2: StepSucceeded(done)
  Stream-Closed
```

This keeps one stream on one execution model:

- The managed-session stream is reconstruction-model: Layer 1 observations plus
  Layer 2 coordination facts.
- The child stream is replay-model: an authored Effect body with keyed journal
  resolutions.
- The seam between them is a Layer 2 child/tool resolution fact on the session
  stream.

Inline runtime services are allowed only when they are not replayable authored
procedure bodies: for example, validating a request, appending an immediate
coordination fact, or returning an already-recorded result. If the tool body
contains `run`, `sleep`, `awaitEvent`, `invoke`, compensation, retry, or any
multi-step durable authoring semantics, it runs as a child invocation.

## Journal And Session Stream

The "journal" and the "session stream" are not competing stores.

- The Durable Streams stream is the durable source of record.
- The journal is the coordination view of that stream.
- An authored procedure usually writes only coordination facts.
- A managed session writes Layer 1 harness observations and Layer 2 coordination
  facts to the same stream.

```text
authored procedure stream
  L2: StepSucceeded(submit)
  L2: WaitIntent(review)
  L2: StepSucceeded(review)
  L2: StepSucceeded(publish)

managed session stream
  L1: user prompt
  L1: assistant text
  L1: tool_call(wait_for)
  L2: WaitIntent(review)
  L2: WaitMatched(review)
  L1: tool_result(wait_for)
  L1: assistant text
```

The stream shape can differ by domain, but there is one durable surface.

## Resolution Facts

`StepSucceeded { key, value }` is the generic authored-procedure resolution
shape. The managed-session names, such as `wait_matched`, `timer_fired`,
`child_terminal`, `tool_result`, and `approval_resolved`, are domain-specific
members of the same Layer 2 resolution-fact family.

The invariant is:

```text
every durable primitive records intent before park
every completed durable primitive resolves by one durable Layer 2 fact
replay/redrive serves the recorded resolution instead of re-deciding it
```

That invariant is what lets Firegrid use Durable Streams as the one log instead
of rebuilding a workflow database beside it.

## What Not To Infer

- Do not wrap a managed agent session in a long-lived fluent handler body.
- Do not use model-loop replay as the durable mechanism for a managed session.
- Do not create a second journal for wait results, timer results, child exits, or
  tool results.
- Do not build a Firegrid lease table, cursor table, webhook retry loop, or
  task-claim lock on top of Durable Streams.
- Do not treat a host-only package integration test as proof of external
  client-to-host ingress or harness I/O.

## Read Next

- [`s2-substrate.md`](s2-substrate.md): the current S2 operation mapping.
- [`substrate-protocol.md`](substrate-protocol.md): the superseded idealized
  Durable Streams operation sequences shared by both execution models.
- [`harness-io.md`](harness-io.md): the reconstruction side of managed sessions.
- [`README.md`](README.md): the top-level provider, package, and responsibility
  map.
