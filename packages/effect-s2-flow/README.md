# effect-s2-flow

`effect-s2-flow` is the durable function runtime being shaped by proof-driven
development. The current production boundary is Capability A: run a function,
journal completed steps to S2, crash after a durable step ack, restart, fold the
journal, and continue without re-running completed work.

This is deliberately below the eventual fluent authoring layer. The closest
analogy is a Pulsar Functions-style worker: the runtime consumes invocation
records, applies user handler code, and appends the result. The SDK surface
exists only where the handler needs access to durable runtime behavior.

## Capability A Surface

The authoring shape for this layer is intentionally small:

```ts
import * as Effect from "effect/Effect"
import { client, run, runHostMain, service } from "effect-s2-flow"

export const greeter = service({
  name: "greeter",
  handlers: {
    process: (input: { readonly name: string }) =>
      Effect.gen(function*() {
        const greeting = yield* run("format", Effect.succeed(`Hello, ${input.name}`))
        return { greeting }
      })
  }
})

runHostMain({ services: [greeter] })
```

- `service(...)` defines an invocation-oriented durable function surface. Each
  client invocation has its own S2-backed invocation journal.
- `client(definition, ...)` appends an invocation request to S2 and waits for a
  completion record.
- `sendClient(definition, ...)` appends an invocation request and returns an
  `InvocationHandle` without waiting. `attach(handle)` waits for the completion
  record later. This is the async call surface later orchestration builds on.
- `run(name, effect)` is a durable step. If the host dies after the step is
  journaled, restart folds the journal and returns the recorded value instead
  of re-running the effect. The step checkpoint is committed as one atomic S2
  append batch: `StepCompleted` plus `CheckpointAdvanced`.
- `serve({ services })` is the host loop. It discovers invocation streams,
  starts one resident owner per active stream, folds its journal, drains pending
  handlers in order, keeps the owner alive briefly for follow-up work, and then
  idles it out.
- `runHostMain({ services })` is the Node process entrypoint. It uses Effect's
  `NodeRuntime.runMain` and wires the host from environment supplied by the
  verifier or deployment wrapper.
- `FlowRuntime.layer({ s2Endpoint, basin })` wires the runtime to S2.

The invocation journal substrate is available as
`effect-s2-flow/invocation-journal`:

- `InvocationJournal.ts` owns S2 basin/stream access, record encoding, stream
  naming, stream discovery, fold-from-S2 journal reads, and the atomic batch
  guard. An over-budget commit fails locally with `BatchTooLarge`; it is never
  silently split into separately-recoverable appends.
- `runtime.ts` owns handler execution, `run` step checkpointing, service/client
  authoring, and the host loop.

The examples exported from `effect-s2-flow/examples/*` are proof fixtures. They
are not product API.

## Capability B Surface

The root package also exports the current durable object primitives:

- `object(...)` defines a per-key durable object backed by one S2 stream per
  key.
- `state(name, initial)` defines owner-local durable state folded from
  `StateChanged` records. Reads inside a handler observe the owner fold.

This surface is still intentionally small, but it is no longer proof-only: the
runtime exports it as the product entrypoint for the current Capability B work.

## Capability C Surface

The first durable timer primitive is also exported:

- `sleep(name, duration)` records a durable timer fact in the invocation
  journal. If the timer is not due, the owner suspends the invocation without
  appending `Failed`. On replay, the owner folds `TimerSet`; once due it appends
  `TimerFired` and lets the handler continue. A host can be killed while the
  invocation is parked and a restarted host resumes from S2.

The current timer driver is intentionally simple: the host scan loop retries
parked invocations and fires due timers from the folded journal. The broader
Capability C scheduler surface is not claimed yet.

## Verification Contract

This package is being built proof-first. Proofs live in
`packages/verification/proofs`, run through `@firegrid/verification`, and use a
real `s2 lite` process plus OpenTelemetry spans exported into chDB.

The important rule is that proofs must exercise this package through the same
surface an application would use for Capability A: `service`, `client`, `run`,
`serve`, `runHostMain`, `FlowRuntime`, and the invocation journal substrate.
Proof-only support code does not belong in this package unless it is a real
production feature.

The load-bearing green proofs establish:

- Durable step replay survives a `kill -9` after a step journal ack and does not
  re-run the completed step.
- Explicit service invocation IDs are idempotent: retrying the same request
  attaches to the existing S2 journal, returns the recorded result, and does
  not append a second `Invoke`.
- The first internal Capability B slices are real-substrate proofs: state folds
  after a fresh process, a stale object-stream token is rejected by S2 with
  `FencingTokenMismatchError`, and two would-be owners of one object stream
  contend without a lost update because the active lease admits one owner while
  the other backs off. A live owner refreshes its fence while processing work
  that runs longer than the initial lease, so a successor cannot steal the
  object mid-handler. A killed owner also stops blocking progress: after its
  lease expires, a successor claims the object stream and completes the pending
  invocation from the S2 journal.
- Durable sleep records `TimerSet`, survives host loss while parked, folds the
  timer fact from S2 after restart, appends `TimerFired` once, and completes the
  invocation exactly once.

## Not Yet Production

The package is no longer just stubs, but the product claim is intentionally
small. These are deferred until their own proofs force them:

- Public durable object/state APIs.
- Eviction semantics and host-health policy for long-running fenced owners.
  Lease refresh for active owners is implemented and proven, but the broader
  production ownership lifecycle still needs explicit proofs.
- A production timer scheduler beyond the current host-scan driven durable
  sleep primitive.
- Request de-duplication beyond the current explicit service invocation id
  path.
- Backpressure, stream discovery pagination, and long-running host lifecycle
  controls.
- Fluent higher-level authoring APIs.

Until those are proven, read this package as the durable function spine over S2,
not as a finished durable execution framework.
