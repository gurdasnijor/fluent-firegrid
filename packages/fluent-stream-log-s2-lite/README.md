# @firegrid/fluent-stream-log-s2-lite

S2 Lite-backed `DurableStreamLog` implementation for the Durable Streams spike.

## Storage Mapping

- A configured S2 basin is the storage namespace for the backend.
- Each Durable Stream identity is persisted as one S2 stream inside that basin.
- S2 stream append/read is used directly for record storage.
- Stream names are derived from the configured prefix plus the stream identity, not only the stream path. This lets a path be recreated immediately after hard delete without reusing the old physical S2 stream name.

## S2 Lite

Use `startS2Lite` from `scripts/s2-lite-process.ts` for tests and local runs. It starts a real S2 Lite process and points the SDK at the local endpoint with `S2Endpoints`.

The adapter uses the official `@s2-dev/streamstore` SDK with the same object model as the SDK quick-start: `S2` -> `basin(...)` -> `streams.ensure(...)` / `stream(...).append(...)` / `stream(...).read(...)`. It sets `appendRetryPolicy: "noSideEffects"` so ambiguous append failures are not retried in a way that can duplicate Durable Streams records.

## Current Limits

This backend stores record bytes durably in S2. Stream metadata, fork references, producer state, and soft-delete state are still held in the process-local stream-log state machine. Persisting that control metadata as S2 control records or snapshots is the next backend hardening step.
