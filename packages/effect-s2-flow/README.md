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
    *process(input: { readonly name: string }) {
      const greeting = yield* run("format", Effect.succeed(`Hello, ${input.name}`))
      return { greeting }
    }
  }
})

runHostMain({ services: [greeter] })
```

- `service(...)` defines an invocation-oriented durable function surface. Each
  client invocation has its own S2-backed invocation journal.
- `client(definition, ...)` appends an invocation request to S2 and waits for a
  completion record.
- `run(name, effect)` is a durable step. If the host dies after the step is
  journaled, restart folds the journal and returns the recorded value instead
  of re-running the effect. The step checkpoint is committed as one atomic S2
  append batch: `StepCompleted` plus `CheckpointAdvanced`.
- `serve({ services })` is the host loop. It discovers invocation streams,
  folds their journals, runs pending handlers, and appends completion records.
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
- The first internal Capability B slices are real-substrate proofs: state folds
  after a fresh process, and two would-be owners of one object stream contend
  through S2 fencing with a real `FencingTokenMismatchError`.

The root package export still stays Capability-A-only. The object/state/fence
work is intentionally behind examples and package-internal modules until the
Capability B authoring surface is cleaned up.

## Not Yet Production

The package is no longer just stubs, but the product claim is intentionally
small. These are deferred until their own proofs force them:

- Public durable object/state APIs.
- Lease refresh, expiry, and eviction semantics for fenced owners.
- Idempotent client retries and request de-duplication beyond the current
  explicit invocation id path.
- Backpressure, stream discovery pagination, and long-running host lifecycle
  controls.
- Fluent higher-level authoring APIs.

Until those are proven, read this package as the durable function spine over S2,
not as a finished durable execution framework.
