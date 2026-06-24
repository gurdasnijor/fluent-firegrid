# @firegrid/verification

Trace-native verification for distributed-system properties.

The package is intentionally small:

- `property(name)` defines the trial, opaque hosts, workload, and post-run checks.
- Workloads are ordinary Effect programs. Use `Effect.all`, `Effect.fork`, `Fiber.join`, scopes, layers, and normal client APIs directly.
- OTel traces are the evidence dataset. Checks do not get a separate model DSL.
- `traceSql(name, sql)` verifies the OTel/chDB data with one read-only query.
- `S2LiteSupervisor` owns a scoped `s2 lite` process for local trials.

```ts
const stepReplay = property("durable.step-replay")
  .s2Lite({ persistence: "local-root" })
  .host("worker", workerProcess)
  .workload(({ faults }) =>
    Effect.gen(function*() {
      const pending = yield* Effect.fork(client(greeter).process({ name: "Ada" }))

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

`trial_spans` expands to the OTel rows tagged with
`firegrid.trial.id = {trial_id:String}`. Proof queries must be a single
`SELECT` or `WITH` query and must return either an `ok` column or a truthy first
column.

Host values are opaque to the property builder. A host may be an in-process
layer, a NodeRuntime entrypoint descriptor, a process wrapper, or any package
local value. The runner only needs named host slots so workload fault APIs can
refer to `"worker"` consistently.
