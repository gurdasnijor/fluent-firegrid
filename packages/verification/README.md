# @firegrid/verification

Trace-native verification for distributed-system properties.

Implemented pieces:

- `property(name)` defines a trial, opaque hosts, an ordinary Effect workload, and post-run checks.
- `TraceRuntime.layer()` wires `@effect/opentelemetry`, `BatchSpanProcessor`, `ChdbSpanExporter`, and chDB. `VerificationRuntime.flush` calls the real processor.
- `operation(name, input, effect, options)` emits runner-owned `verification.operation` spans with input, output, status, client, id, and key attributes.
- `traceSql(name, sql)` verifies the OTel/chDB evidence dataset with one read-only query.
- `trial_spans` expands to every OTel span in any trace that contains the trial marker span.
- `waitForSpan` is scoped to the active trial id; it cannot satisfy a wait from another trial's spans in the same chDB session.
- `S2LiteSupervisor` owns a scoped `s2 lite` child process, waits for HTTP readiness, and exposes separate graceful stop and force kill paths.

Still missing before this should be treated as the complete verification system:

- a real host supervisor that starts opaque host descriptors and injects trial/host/S2 config into NodeRuntime processes;
- concrete `Faults` backed by that host supervisor;
- a shared OTel sink topology for host processes, so `waitForSpan(...)` can see host-emitted spans before killing them;
- `killHostAfterSpan` implemented as trial-scoped `waitForSpan(...)` plus a real process kill;
- report/counterexample artifacts;
- the first durable replay gate against real `s2 lite` and a real crashed host.

Example authoring shape:

```ts
const stepReplay = property("durable.step-replay")
  .s2Lite({ persistence: "local-root" })
  .host("worker", workerProcess)
  .workload(({ faults, operation }) =>
    Effect.gen(function*() {
      const pending = yield* Effect.fork(
        operation("greeter.process", { name: "Ada" }, client(greeter).process({ name: "Ada" }))
      )

      yield* faults.killHostAfterSpan("worker", {
        span: "durable.journal.append.ack",
        attributes: {
          "durable.record.type": "StepCompleted",
          "durable.step.name": "step-1"
        }
      })

      yield* faults.restartHost("worker")
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
