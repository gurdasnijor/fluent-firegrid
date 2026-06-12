---
title: Benchmarking
description: >-
  How to compare Durable Streams server implementations, including the in-memory
  and S2 Lite-backed Fluent Firegrid spike backends.
outline: [2, 3]
---

# Benchmarking

Benchmarks should answer two different questions:

- **Protocol performance:** how much latency and throughput overhead the Durable
  Streams HTTP/SSE protocol adds for a client.
- **Backend performance:** how a stream-log implementation behaves under the
  same protocol surface.

Conformance passing is required before benchmark numbers matter. It proves the
server obeys the protocol contract; it does not prove a backend is fast enough
for production.

## Current Comparable Backends

| Backend | Package | Persistence | What it is good for | Main caveat |
| --- | --- | --- | --- | --- |
| Baseline reference memory | `@durable-streams/server` | Process memory | Upstream protocol reference point for in-memory measurements | Not this codebase; used as comparison target, not implementation substrate |
| Baseline reference filesystem | `@durable-streams/server` | Local filesystem | Upstream protocol reference point for local durable-record-path measurements | Filesystem-backed reference implementation, not the Fluent storage substrate |
| Fluent in-memory | `@firegrid/fluent-stream-log-inmemory` | Process memory | Fast semantic reference and regression oracle | Not durable; restart loses all stream state |
| Fluent S2 Lite | `@firegrid/fluent-stream-log-s2-lite` | S2 stream records on local S2 Lite storage | Durable record-path spike using the official S2 SDK | Stream metadata, producer state, forks, and soft-delete state are still process-local |

The S2 Lite backend intentionally follows the S2 SDK object model:

```text
S2 -> basin(...) -> streams.ensure(...) -> stream(...).append/read/delete
```

The Durable Stream identity maps to one S2 stream inside the configured basin.
This keeps the storage path close to the S2 quick-start and avoids treating S2
basins as per-stream implementation detail.

## Correctness Baseline

Run the in-memory conformance suite:

```bash
pnpm --filter @firegrid/fluent-durable-streams-spike test:conformance
```

Run the S2 Lite conformance suite:

```bash
pnpm --filter @firegrid/fluent-durable-streams-spike test:conformance:s2-lite
```

As of the S2 Lite backend spike, the S2 Lite path passes:

```text
326 passed | 7 skipped (333)
```

Treat skipped tests as explicit remaining surface, not as performance signal.

## What To Measure

Use the same client, protocol route set, payload mix, and concurrency level for
each backend. The minimum useful comparison is:

| Scenario | Why it matters |
| --- | --- |
| Create latency | Exercises stream metadata creation and backend control path |
| Append latency | Measures the write path, producer checks, content-type checks, and backend append |
| Read latency | Measures catch-up read and offset mapping |
| Append then read round trip | Captures end-to-end client-visible latency |
| Small message throughput | Stresses per-message overhead and batching behavior |
| Large message throughput | Stresses payload limits, copy costs, and backend write bandwidth |
| SSE tail latency | Measures live delivery after the backlog-to-live boundary |
| Delete and recreate | Exercises path reuse, physical stream cleanup, and metadata behavior |

## Expected Shape

The in-memory backend should be fastest because it avoids network hops, storage
sync, and serialization through S2. It is the semantic floor for overhead inside
the server implementation.

The S2 Lite backend should be slower on create, append, read, and delete because
every persisted record goes through the S2 Lite HTTP API and local storage
engine. That cost is useful: it approximates the shape of a durable backend and
surfaces ordering, payload, and offset-mapping issues that pure memory hides.

Do not compare S2 Lite numbers against in-memory as a pass/fail threshold.
Compare them to answer:

- Does durable append latency stay within the intended product budget?
- Does throughput scale with batching and concurrency?
- Do reads degrade with long streams or large payloads?
- Do SSE readers receive live records without large tail latency?
- Does cleanup or path recreation introduce visible stalls?

## Running Benchmarks

The reference Durable Streams docs describe an `@durable-streams/benchmarks`
package that runs against a live HTTP server:

```typescript
import { runBenchmarks } from "@durable-streams/benchmarks"

runBenchmarks({
  baseUrl: "http://localhost:4437",
  environment: "local",
})
```

In this repository, the conformance package also contains a client benchmark
harness for the upstream reference server:

```bash
pnpm --dir packages/conformance exec tsx src/cli.ts client --bench ts
```

For backend comparison, use the Fluent Firegrid backend runner. It starts the
upstream `@durable-streams/server` reference server twice, once in memory and
once with a temporary filesystem `dataDir`. It then starts the same Fluent spike
HTTP server twice, once with the in-memory log and once with the S2 Lite log:

```bash
pnpm --filter @firegrid/fluent-durable-streams-spike benchmark:backends -- \
  --out docs/reference/durable-streams/docs/benchmark-results-fluent-backends.json
```

To emit local OpenTelemetry traces for the Fluent backends, add `--trace-out`.
The trace file is JSONL and can be regenerated; keep large trace artifacts out of
source control unless a specific investigation needs them.

```bash
pnpm --filter @firegrid/fluent-durable-streams-spike benchmark:backends -- \
  --out docs/reference/durable-streams/docs/benchmark-results-fluent-backends.json \
  --trace-out docs/reference/durable-streams/docs/benchmark-trace-fluent-backends.jsonl
```

Recommended labels:

```text
baseline-reference-memory
baseline-reference-filesystem
local-inmemory
local-s2-lite
```

Record these environment details with every result:

- Git commit.
- Node version.
- Effect / Effect Platform version.
- S2 SDK version.
- S2 Lite commit or binary version.
- Machine CPU and memory.
- Storage medium for S2 Lite local root.
- Payload sizes, concurrency, warmup count, and sample count.

## Interpreting Results

Latency reports should include min, max, mean, p50, p75, p95, and p99. Throughput
reports should include messages per second and bytes per second. Keep raw JSON
results as artifacts so regressions can be compared across commits.

The most useful first table is a ratio table. This run was captured with Node
`v24.14.1` at `2026-06-12T07:34:56.550Z`.

Raw benchmark result files are generated artifacts. Keep them in CI or local
run artifacts when needed; do not check them into the repository.

| Metric | Baseline reference memory | Baseline reference filesystem | Fluent in-memory | Fluent S2 Lite | In-memory / memory baseline | S2 Lite / filesystem baseline |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Create p50 | 0.28 ms | 5.18 ms | 0.49 ms | 6.45 ms | 1.72x | 1.24x |
| Append 100B p50 | 0.31 ms | 14.77 ms | 0.44 ms | 6.24 ms | 1.43x | 0.42x |
| Read 10KB p50 | 0.52 ms | 5.26 ms | 0.51 ms | 1.72 ms | 0.98x | 0.33x |
| Append + long-poll p50 | 1.51 ms | 41.69 ms | 2.35 ms | 39.05 ms | 1.55x | 0.94x |
| Small-message append throughput | 5,542.10 ops/sec | 68.50 ops/sec | 2,918.67 ops/sec | 153.50 ops/sec | 0.53x | 2.24x |
| Large-message 256KiB append throughput | 637.88 MB/sec | 16.19 MB/sec | 378.18 MB/sec | 31.94 MB/sec | 0.59x | 1.97x |
| SSE first-event p95 | 1.68 ms | 38.33 ms | 3.93 ms | 17.15 ms | 2.34x | 0.45x |

For latency rows, ratios above `1.00x` are slower than baseline. For throughput
rows, ratios below `1.00x` are lower throughput than baseline. Compare Fluent
in-memory against the memory reference baseline. Compare Fluent S2 Lite against
the filesystem reference baseline; comparing a durable record path to the memory
reference is useful only as a rough upper-bound cost signal.

## Trace Findings

The traced run writes an OpenTelemetry JSONL artifact and prints an aggregate
span table. The traced run adds export overhead, so use it for attribution, not
for final benchmark numbers.

Current traced-run attribution for the Fluent backends:

| Span | Count | Total | Mean | p95 |
| --- | ---: | ---: | ---: | ---: |
| `durable_stream_log.s2_lite.append` | 1,208 | 7,091.88 ms | 5.87 ms | 6.88 ms |
| `durable_stream_log.s2_lite.sdk.append_records` | 1,208 | 6,981.90 ms | 5.78 ms | 6.72 ms |
| `durable_stream_log.s2_lite.create` | 137 | 780.78 ms | 5.70 ms | 7.99 ms |
| `durable_stream_log.s2_lite.sdk.ensure_stream` | 137 | 767.47 ms | 5.60 ms | 7.83 ms |
| `durable_stream_log.s2_lite.read` | 352 | 437.75 ms | 1.24 ms | 1.99 ms |
| `durable_stream_log.s2_lite.sdk.read_records` | 375 | 420.26 ms | 1.12 ms | 1.73 ms |
| `durable_stream_log.inmemory.append` | 1,208 | 45.85 ms | 0.04 ms | 0.06 ms |

The main S2 Lite bottleneck is below the Durable Streams semantic layer:
`append` is dominated by `sdk.append_records`, and `create` is dominated by
`sdk.ensure_stream`. The Fluent in-memory append path is sub-0.1 ms at p95 in
the same traced run, so the durable-record-path cost is not coming from producer
validation, content-type checks, or the HTTP status/header mapping.

## Current Limitations

The S2 Lite backend currently persists record bytes through S2 stream storage,
but it does not yet persist all Durable Streams control state. In particular,
stream metadata, producer fencing state, fork references, and soft-delete state
are still process-local.

Until that state is durable, S2 Lite benchmark results should be read as
"durable record-path" results, not as full production durability results.

## See Also

- [Protocol specification](../PROTOCOL.md)
- [Building a server](building-a-server.md)
- [Deployment](deployment.md)
