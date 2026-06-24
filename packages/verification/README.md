# @firegrid/verification

Trace-native verification for distributed-system properties.

This package is not a cucumber-style acceptance-test harness. It is a runner for
executable proofs: start real substrate processes, drive the system with ordinary
Effect programs, inject process faults, collect OpenTelemetry evidence, and
verify the observed history after the run.

The goal is to make distributed-system claims concrete:

- "This acknowledged write is visible to later reads."
- "This replay did not duplicate a durable side effect."
- "This stale owner was fenced by the stream tail."
- "This crash happened after the journal append, and restart folded the real
  journal instead of re-running the effect."

## Prior Art

The shape is intentionally close to the verification loop described in S2's
posts on [linearizability testing](https://s2.dev/blog/linearizability) and
[deterministic simulation testing](https://s2.dev/blog/dst):

1. Run the real system, or as much of it as possible, under a controlled runner.
2. Drive concurrent client workloads.
3. Inject faults such as crashes, restarts, network hardship, and timing stress.
4. Record what clients observed and what the system emitted internally.
5. Check the resulting history after the fact.
6. Preserve enough evidence to reproduce or explain the failure.

S2 uses turmoil-based deterministic simulation to run distributed components as
logical hosts with reproducible seeds and fault injection. Their
linearizability work then feeds collected histories into a Porcupine-style model
checker to ask whether the concurrent calls can be arranged into a legal
sequential order.

[Porcupine](https://github.com/anishathalye/porcupine) is the important
reference point for the checking side: it consumes an executable sequential
specification plus a concurrent operation history and decides whether the
history is linearizable. We are not exposing a Porcupine modeling DSL here. If
we need that class of checker, it should be an adapter that consumes the same
OpenTelemetry-backed evidence as every other verifier.

## Firegrid's Adaptation

The main design difference is the evidence format. We do not want one dataset
for diagnostics and another for verification. OpenTelemetry is the evidence
ledger; chDB is the query engine over that ledger.

Production code should be instrumented normally with Effect tracing,
`Effect.withSpan`, `Effect.annotateCurrentSpan`, and package-local
instrumentation. The verifier records runner-owned spans only for runner events:
trial lifecycle, process lifecycle, fault injection, and optional client
operation boundaries.

That gives us one canonical question after a trial:

> Given the workload result and the trial-scoped OTel spans, did the system
> exhibit the property we claimed?

## Design Principles

- Prove production behavior. Proof files should drive production APIs and
  production instrumentation. Avoid proof-local "support" code that reimplements
  the system under test.
- Keep hosts opaque. A host is a process or runtime the verifier can start,
  stop, kill, and configure. The verifier should not need to know its internal
  service graph.
- Use ordinary Effect. Workloads are normal `Effect` programs. Concurrency,
  fibers, resources, scopes, retries, and interruption should come from Effect,
  not from a verification-specific control-flow DSL.
- Use real process faults. A crash proof needs a real process kill path, not a
  fake callback.
- Verify after the run. SQL checks, expected workload results, and future
  linearizability checkers all read the same trial evidence.
- Make client boundaries explicit only when needed. Most proofs should rely on
  passive production spans. Use `operation(...)` only when the property needs
  the verifier's externally observed call/return interval.

## Mental Model

A proof has four phases:

1. **Provision** - `property(...)` starts scoped dependencies such as `s2 lite`
   and any runner-owned process hosts.
2. **Drive** - the workload drives clients, hosts, and substrate APIs with
   ordinary Effect code.
3. **Record** - production spans and runner spans are exported through
   `@effect/opentelemetry`, `BatchSpanProcessor`, `ChdbSpanExporter`, and chDB.
4. **Verify** - verifiers inspect the workload result and trial-scoped evidence
   views such as `trial_spans` and `verification_operations`.

The runner owns trial identity. `runProperty` annotates the trial with
`firegrid.trial.id`; process hosts receive generated trial, host, S2 endpoint,
and OTel configuration automatically.

## Current Surface

```ts
export default proof("effect-s2.capability-a.atomic-replay")
  .describedAs(
    "Proves atomic append + replay rejection for the effect-s2 substrate."
  )
  .spec(({ property, trialId }) => {
    const streamName = `invocation-${trialId}`
    const journalTypes = ["StepCompleted", "CheckpointAdvanced"] as const
    const journal = [
      AppendRecord.string({
        body: "step-1:ok",
        headers: [["durable.record.type", "StepCompleted"]]
      }),
      AppendRecord.string({
        body: "input-cursor:1",
        headers: [["durable.record.type", "CheckpointAdvanced"]]
      })
    ]

    return property("capability-a.effect-s2.atomic-replay-proof")
      .s2Lite({ persistence: "local-root" })
      .workload(({ s2 }) =>
        Effect.gen(function*() {
          const stream = yield* s2.stream({
            basin: "capability-a-proof",
            stream: streamName
          })
          const initialTail = yield* stream.checkTail()
          const matchSeqNum = initialTail.tail.seqNum

          const commitAck = yield* stream.append(
            AppendInput.create(journal, { matchSeqNum })
          )

          const staleReplayError = yield* stream.append(
            AppendInput.create(journal, { matchSeqNum })
          ).pipe(
            Effect.flip,
            Effect.filterOrFail(
              (error): error is SeqNumMismatchError => error instanceof SeqNumMismatchError,
              (error) => error
            )
          )

          const replayRecordTypes = yield* stream.readSession({
            start: { from: { seqNum: commitAck.start.seqNum } },
            stop: { limits: { count: 2 } }
          }).pipe(
            Stream.runCollect,
            Effect.map((records) =>
              Array.from(
                records,
                (record) => record.headers.find(([key]) => key === "durable.record.type")?.[1] ?? ""
              )
            )
          )

          const finalTail = yield* stream.checkTail()

          return {
            replayRecordTypes,
            staleReplayRejectedAtSeqNum: staleReplayError.expectedSeqNum,
            tailAdvancedBy: finalTail.tail.seqNum - initialTail.tail.seqNum
          }
        })
      )
      .verify(({ expect, traceSql }) => [
        expect.workloadResult({
          replayRecordTypes: journalTypes,
          staleReplayRejectedAtSeqNum: 2,
          tailAdvancedBy: 2
        }),
        traceSql(
          "append-observed",
          `
          SELECT countIf(
            SpanName = 'effect-s2.append'
            AND SpanAttributes['s2.operation.status'] = 'ok'
          ) >= 1 AS ok
          FROM trial_spans
        `
        )
      ])
  })
```

The example is intentionally ordinary Effect and ordinary `effect-s2` usage. The
full proof in `proofs/effect-s2-capability-a.ts` also verifies the specific
`SeqNumMismatchError`, final tail movement, replayed record types, and
production `effect-s2` spans.

## Evidence Views

`traceSql(name, sql)` runs one read-only query against the chDB evidence store.
Queries must be a single `SELECT` or `WITH` statement and must return either an
`ok` column or a truthy first column.

Available views:

- `trial_spans` - every OTel span in traces marked with the active trial id.
- `verification_operations` - runner-owned client boundary spans emitted by
  `operation(...)`.

Use `traceSql` for evidence claims:

```sql
SELECT countIf(
  SpanName = 'effect-s2.append'
  AND SpanAttributes['s2.operation.status'] = 'error'
  AND SpanAttributes['s2.error.kind'] = 'SeqNumMismatchError'
) = 1 AS ok
FROM trial_spans
```

Use `expect.workloadResult(...)` for the direct value returned by the workload.
This is useful for invariant summaries that would be awkward to reassemble from
spans alone.

## Operation Boundaries

Most proofs should not call `operation(...)`. Instrument the system under test
normally and verify the passive production spans.

Use `operation(name, input, effect, options)` when the verifier needs the
client-observed call/return interval. Linearizability is the main reason: the
checker needs to know when the client invoked an operation, when the client
observed the return, and what input/output was observed at that boundary.

The operation boundary is still OTel evidence. It is not a second diagnostic
dataset, and it does not change Effect execution semantics. The wrapped effect
still succeeds, fails, retries, catches, and interrupts normally.

## Process And Fault Model

`S2LiteSupervisor` owns a scoped `s2 lite` child process, waits for HTTP
readiness, and exposes graceful stop plus force-kill paths.

`processHost(config)` marks an opaque host as runner-owned. The runner starts it
inside the trial scope, injects:

- `FIREGRID_TRIAL_ID`
- `FIREGRID_HOST_ID`
- `S2_ENDPOINT`
- OTel exporter/resource configuration

`hosts.kill(name)` sends `SIGKILL`. `hosts.restart(name)` starts the host again.
`hosts.killAfterSpan(name, match)` waits for a trial-scoped span and then kills
the process. `faults` remains as a compatibility alias for the older method
names.

This is the minimum process substrate needed before higher-level durable replay
proofs can be meaningful.

## Running Proofs

From the repository root:

```sh
pnpm --filter @firegrid/verification proofs
```

From the package:

```sh
tsx proofs/main.ts proof list
tsx proofs/main.ts proof run all
tsx proofs/main.ts proof run effect-s2.capability-a.atomic-replay --report-dir reports
```

`runProperty(..., { reportDir })` writes a JSON report with span counts, trace
coverage, and failed-check context. Failed checks also include an observed span
summary in the thrown `VerificationError`.

## Implemented Today

- `property(name)` for scoped trials, opaque hosts, workloads, and post-run
  verifiers.
- `TraceRuntime.layer()` with real OTel SDK wiring, `BatchSpanProcessor`,
  `ChdbSpanExporter`, and chDB-backed SQL verification.
- Trial-scoped `waitForSpan(...)`.
- `S2LiteSupervisor` for real `s2 lite` process lifecycle.
- `.s2Lite({ persistence: "local-root" })` wired into `runProperty`.
- `processHost(...)` plus `hosts.kill`, `hosts.restart`, and
  `hosts.killAfterSpan`.
- `traceSql`, `traceOperation`, and workload-result verifiers.
- The proof CLI under `proofs/main.ts`.
- `proofs/effect-s2-capability-a.ts`, a live substrate proof for
  `packages/effect-s2`.

## Not Yet Claimed

This package is not yet a full deterministic simulator like turmoil. It does not
control all sources of time, entropy, scheduling, or network I/O.

This package is not yet a full linearizability checker. The evidence model is
designed so a Porcupine-style checker can consume operation-boundary spans, but
that adapter is not implemented.

This package is not yet the full durable replay proof. The current
`effect-s2` proof validates the atomic append and stale replay rejection
primitive that durable replay depends on. The next load-bearing proof is the
composed host crash: crash a real host after the journal append, restart it, and
prove from trace evidence that the journal is folded and the side effect is not
duplicated.
