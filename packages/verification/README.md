# @firegrid/verification

Trace-native verification for distributed-system properties.

Implemented pieces:

- `property(name)` defines a trial, opaque hosts, an ordinary Effect workload, and post-run checks.
- `TraceRuntime.layer()` wires `@effect/opentelemetry`, `BatchSpanProcessor`, `ChdbSpanExporter`, and chDB. `VerificationRuntime.flush` calls the real processor.
- `operation(name, input, effect, options)` is an optional runner-owned client-boundary span for properties that need externally observed call/return evidence. Production code should still use normal Effect tracing with `Effect.withSpan`, `Effect.annotateSpans`, and package-local instrumentation.
- `traceSql(name, sql)` verifies the OTel/chDB evidence dataset with one read-only query.
- `traceOperation(name, match)` is the common helper for asserting a `verification.operation` span by operation name, status, attributes, output fragments, and expected count.
- `trial_spans` expands to every OTel span in any trace that contains the trial marker span.
- `waitForSpan` is scoped to the active trial id; it cannot satisfy a wait from another trial's spans in the same chDB session.
- `S2LiteSupervisor` owns a scoped `s2 lite` child process, waits for HTTP readiness, and exposes separate graceful stop and force kill paths.
- `.s2Lite({ persistence: "local-root" })` is wired into `runProperty`; tests can override the binary, port, and local root through `runProperty(..., { s2Lite })`. Workloads receive `s2`, a low-level handle to the supervised S2 endpoint for substrate proofs.
- `processHost(config)` marks an otherwise opaque host as runner-owned. The runner starts it in the trial scope, injects `FIREGRID_TRIAL_ID`, `FIREGRID_HOST_ID`, `S2_ENDPOINT`, and OTel resource attributes, and records host lifecycle spans.
- `hosts` is backed by supervised process hosts: `hosts.kill` sends `SIGKILL`, `hosts.restart` starts the host again, and `hosts.killAfterSpan` is trial-scoped `waitForSpan(...)` followed by a real process kill. `faults` remains as a compatibility alias for the older method names.
- `runProperty(..., { reportDir })` writes a JSON report with span counts, trace coverage, and failed-check context. Failed checks also include an observed span summary in the thrown `VerificationError`.
- `proofs/effect-s2-capability-a.ts` is a live substrate proof for Capability A's `packages/effect-s2` dependency: under real `s2 lite`, an atomic own-journal batch guarded by `matchSeqNum` commits `StepCompleted + CheckpointAdvanced`, the stale replay append is rejected by `SeqNumMismatchError`, and replay reads back only the original batch. The proof is verified through workload result checks and trace SQL over production `effect-s2` spans in the OTel/chDB evidence.
- `tsx src/main.ts proof run all` runs registered proofs through the verification system. `proof list` shows available proofs; `proof run <name> --report-dir <dir>` writes JSON trial reports.

Still missing before this should be treated as the complete verification system:

- host-process SDK/runtime packaging that uses the same `NodeSdk` + `ChdbSpanExporter` pattern as the runner, so host-emitted spans are available to `waitForSpan(...)` without per-host setup;
- the higher-level durable replay gate against a real crashed host.

Example authoring shape:

```ts
const stepReplay = property("durable.step-replay")
  .s2Lite({ persistence: "local-root" })
  .host(
    "worker",
    processHost({
      command: "node",
      args: ["dist/worker.js"]
    })
  )
  .workload(({ hosts, operation }) =>
    Effect.gen(function*() {
      const pending = yield* Effect.fork(
        operation("greeter.process", { name: "Ada" }, client(greeter).process({ name: "Ada" }))
      )

      yield* hosts.killAfterSpan("worker", {
        span: "durable.journal.append.ack",
        attributes: {
          "durable.record.type": "StepCompleted",
          "durable.step.name": "step-1"
        }
      })

      yield* hosts.restart("worker")
      return yield* Fiber.join(pending)
    })
  )
  .verify(
    expectWorkloadResult({ greeting: "Hello, Ada!" }),
    traceSql(
      "step-1-side-effect-once",
      `
      SELECT countIf(
        SpanName = 'example.side_effect'
        AND SpanAttributes['durable.step.name'] = 'step-1'
      ) = 1 AS ok
      FROM trial_spans
    `
    )
  )
```

Proof queries must be a single `SELECT` or `WITH` query and must return either
an `ok` column or a truthy first column.

## Operation boundaries

Most proofs should verify passive production telemetry. Instrument the system
under test normally with Effect tracing and assert over those spans through
`traceSql`.

Use `operation(...)` only when the property needs the verifier/client's
externally observed call/return interval, such as a future linearizability
check. It does not replace Effect execution semantics: the wrapped effect still
succeeds, fails, retries, catches, and interrupts normally.
