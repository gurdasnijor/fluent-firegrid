# SDD: `@firegrid/log` — an Effect-native client for S2 (durable streams)

Doc-Class: SDD
Status: frozen
Date: 2026-07-07
Owner: Firegrid Architecture
Substrate: S2

| | |
|---|---|
| **Status** | Frozen historical plan; superseded by `src/Firegrid.Log` |
| **Date** | 2026-06-15 |
| **Target stack** | `effect@beta` (smol / v4 API), TypeScript (strict), Node + Bun + Deno runtimes |
| **Wraps** | `@s2-dev/streamstore` (S2 official TS SDK, ≥ 0.22.x) |
| **Structural reference** | `floydspace/effect-kafka` (engine-layer + role-modules pattern; v3, **structure only**) |
| **Idiom authority** | the in-repo `effect-ts` skill (`./skills/effect-ts/references/*`) — its guides are normative for all Effect patterns |
| **Audience** | a coding agent (coordinator/worker). Each `T-###` task below is a bead; each milestone `M#` is an epic. |

---

## 0. How to read this document

This is a spec, not a tutorial. Directives use RFC-2119 force: **MUST**, **SHOULD**, **MUST NOT**.

The single most important rule: **do not implement any module whose S2 API assumptions are not yet verified against source.** All such assumptions are enumerated in §4 and gated behind milestone **M0**. If M0 is incomplete, M2/M3/M4 are blocked. Record every verification outcome in the Verification Log (§11) before writing the dependent module.

The frozen public contract is §6. Treat it as an interface lock: implementations may change, signatures may not, without an explicit decision-register entry (§12).

---

## 1. Summary & goals

Build a thin, idiomatic Effect facade over the S2 TypeScript SDK. The facade turns the SDK's promises, async-iterables, and callback/ticket APIs into `Effect`, `Stream`, `Sink`, and `Schema`-typed errors, with resource lifetimes owned by `Scope` and dependencies injected via `Layer`.

**Goals**

- G1. Every public SDK operation family (basins, streams, access tokens, locations, metrics, append/read/session/producer) is reachable through an Effect-native surface with SDK-native input/output types and typed errors.
- G2. Read sessions are exposed as `Stream`; their underlying connection is released by the enclosing `Scope` (no manual `close()` in user code).
- G3. Append throughput path (Producer/batcher) is exposed as a scoped handle and as a `Sink`, preserving the SDK's batching and backpressure.
- G4. A `Schema`-typed channel layer: encode/decode record bodies so a stream reads as `Stream<A>` and publishes `A`, with decode failures isolated to the error channel.
- G5. First-class exactly-once support via `matchSeqNum` (compare-and-set append) surfaced as a typed conflict, suitable for journal/replay callers.
- G6. Avoid duplicating upstream SDK behavioral tests; this package verifies wrapper surface and typing, while transport/service behavior remains the SDK's responsibility.

## 2. Non-goals

- N1. **Do not** reimplement the S2 wire protocol (S2S framing / HTTP-2 transport). The SDK owns transport. This library is a facade.
- N2. **Do not** re-test or reimplement SDK control-plane behavior. The facade wraps those operations one-to-one; upstream SDK tests remain the behavioral oracle.
- N3. **Do not** add an Effect-level retry/backoff layer that duplicates the SDK's transport retries (see §7.4 for the division of responsibility).
- N4. No browser-specific build target in v1 (the SDK supports it; we do not block on it). Code MUST NOT depend on Node-only globals in `src/` so a browser build remains possible later.

## 3. Constraints & conventions (normative)

The agent **MUST** follow the `effect-ts` skill. Specifically:

- C1. **No `any`. No `as` casts. No non-null assertions. No `namespace`.** (`SKILL.md` → TypeScript Preferences.) External/boundary values are decoded with `Schema`, never asserted.
- C2. Reusable effectful operations are defined with `Effect.fn("Name")(...)`, not bare `Effect.gen`. (`guide-trace.md`.) Use `Effect.fnUntraced` only with a measured reason (none expected here).
- C3. Services are `Context.Service`; layers are plain exported constants or `static` members, never hidden in `namespace`. (`guide-layers.md`.)
- C4. Resource-owning construction uses `Layer.effect` + `Effect.acquireRelease`. `Layer.scoped` does not exist in this API; `Layer.effect` is the scoped constructor. (`guide-layers.md`.)
- C5. Errors are `Schema.TaggedErrorClass` with `Schema.Defect` preserving the foreign cause. (`guide-error-handling.md`.)
- C6. `Layer`/`Effect.provide` only at the edge (entrypoints, examples, tests). Library code requires services; it never self-provides. (`guide-layers.md`.)
- C7. Schemas use `Class`/`TaggedClass`/`TaggedErrorClass` for named models; construct with `X.make(...)`, not `new X(...)`. (`guide-schema.md`.)
- C8. Tests use `@effect/vitest` with `layer(...)`/`it.layer(...)`; no per-test `Effect.provide`, no `Effect.runPromise` in test bodies. (`guide-testing.md`.)
- C9. Installation: `effect@beta`; every `@effect/*` package version-aligned. (`SKILL.md` → Installation.)
- C10. **Source-verified claims only.** No signature, field name, or behavior asserted in code without a corresponding entry in the Verification Log (§11).

Build tooling: match the host monorepo. Lint with the repo's configured linter (the SDK itself uses Biome; align with the consuming repo, not the SDK). Type-check in CI with no errors.

---

## 4. S2 facts: verified vs. to-verify

### 4.1 Verified from S2 docs + SDK README (treat as ground truth)

- Client: `new S2({ accessToken, retry? })`. `S2Environment.parse()` supplies environment-derived config. Construction is synchronous; the top-level client has no documented `close()`.
- Namespacing: `s2.basin(basinName)` → basin client; `basin.streams.create({ stream })` (control plane); `basin.stream(name, opts?)` → stream client (data plane). `opts` includes `forceTransport: "fetch" | "s2s"`.
- `stream.append(AppendInput)` → `Promise<AppendAck>`. One call writes exactly one batch; a batch is ≤ 1000 records or ≤ 1 MiB.
- `AppendInput.create([records])`; `AppendRecord.string({ body, headers? })`; `AppendRecord.bytes({ body, headers? })`. `AppendAck` has `start.seqNum` and `end.seqNum` (end exclusive).
- `stream.read(options, { as: "bytes" }?)` → single batch (≤ 1000 records / 1 MiB), `.records`.
- `stream.readSession(options)` → async-iterable of records. No stop condition ⇒ follows live indefinitely. With a stop (`count`/`bytes`/`untilTimestamp`/`waitSecs`) ⇒ ends when the condition is met **or** the tail is reached, whichever first.
- Read `options.start.from` is one of `{ seqNum }`, `{ timestamp }`, `{ tailOffset }`; `start.clamp?: boolean`; `options.stop` includes `{ limits: { count?, bytes? } }`, `{ untilTimestamp }`, `{ waitSecs }`.
- `stream.appendSession({ maxInflightBytes?, maxInflightBatches? })` → session; `session.submit(AppendInput)` → `Promise<BatchSubmitTicket>` (submit applies backpressure); `ticket.ack()` → `Promise<AppendAck>` (resolves on durability); `session.close()`.
- `Producer` = `new Producer(new BatchTransform({ lingerDurationMillis?, maxBatchRecords?, maxBatchBytes? }), await stream.appendSession())`. `producer.submit(AppendRecord)` → ticket; `ack.seqNum()` returns the per-record seq number; `producer.close()`. Batching defaults: linger 5ms, maxBatchRecords 1000, maxBatchBytes 1 MiB. Backpressure defaults: `maxInflightBytes` 5 MiB.
- `stream.checkTail()` → `{ tail: { seqNum } }`; `tail.seqNum` is the seq number the next appended record will receive.
- Errors: SDK throws `S2Error` with `.status: number`. Idempotent retry knob: `retry.appendRetryPolicy: "noSideEffects" | "all"` — `"noSideEffects"` retries only appends idempotent via `matchSeqNum`.
- Rate limit: ≤ 200 append batches/sec per stream; over-limit unary callers may get HTTP 429 with `Retry-After`; sessions/producers use backpressure.
- A higher-level `packages/patterns` package ships typed append/read sessions, framing, and dedupe helpers.

### 4.2 Verification tasks (BLOCKING — milestone M0)

Each MUST be resolved by reading source before the dependent module is written. Targets: SDK source under `@s2-dev/streamstore` `packages/streamstore` and `packages/patterns`; the vendored Effect source under `./.repos/effect`.

| ID | Question | Read | Blocks |
|----|----------|------|--------|
| V1 | Does the TS read-session async-iterator's `return()` close the underlying connection, or must the caller invoke `stream.close()` / a session close? Exact teardown path. | SDK `packages/streamstore` stream + read-session impl | `Reader.ts` release fn |
| V2 | Exact name/arity of `Stream.fromAsyncIterable` (or equivalent) in smol, incl. the error-mapping arg. | `./.repos/effect/packages/effect/src/Stream.ts` | `Reader.ts` |
| V3 | Field names + types for compare-and-set and fencing on append: `matchSeqNum`, `fencingToken` — on `AppendInput`, on `append(opts)`, or both. | SDK source + S2 concurrency-control docs | `Writer.conditionalAppend`, `Channel` |
| V4 | Confirm `S2Error` is exported and `.status` is the documented field; enumerate which statuses map to which conditions (404/409/412/429). | SDK error module | `S2Error.ts` mapper |
| V5 | Confirm `AppendAck` (`start.seqNum`, `end.seqNum`) and the Producer ticket type (`ack()` → object with `seqNum()` method) shapes. | SDK types | `Writer.ts` |
| V6 | Confirm `ReadSessionOptions` / `ReadOptions` exact TS types (start/stop discriminants, `clamp`, `waitSecs`). | SDK types | `Reader.ts` option types |
| V7 | Confirm `S2Environment.parse()` env var names so `layerConfig` aligns 1:1. | SDK config module | `S2Client.layerConfig` |
| V8 | Evaluate `packages/patterns`: do its typed-session / framing / dedupe helpers obviate parts of `Channel.ts`? Decide wrap-vs-build. | SDK `packages/patterns` | `Channel.ts` scope |

If any answer diverges from §4.1, update §4.1, the affected contract in §6, and the Verification Log, then proceed.

---

## 5. Architecture

### 5.1 Layer graph

```
S2Client            ← connection + config (wraps `new S2`, holds basinName)
   │  service shape is BEHAVIORAL (methods), not the raw SDK object  [see §6.1 rationale]
   ├─ public service accessors: createStream / checkTail / append / appendSession / read / readBytes / producer
   ├─ Channel (free fns): publish / readDecoded / conditionalAppend
   └─ Sink helper (free fn): sink(producer)

TelemetryLayer      ← @effect/opentelemetry, provided ONCE at the edge (examples/app)
```

Divergence from effect-kafka (recorded decision D1): effect-kafka splits `Producer` and `Consumer` into separate role-services because a Kafka consumer is a long-lived `groupId` subscription. S2 reads are per-call by stream + start position (a query, not a subscription), so a **single behavioral `S2Client` service** is used. The public API exposes service-owned static accessors (`S2Client.append`, `S2Client.read`, etc.) rather than package-level thin accessor functions (decision D2).

### 5.2 Package layout

```
packages/log/
  package.json            # effect@beta, @s2-dev/streamstore; dev: @effect/vitest, @effect/platform-node
  tsconfig.json
  src/
    S2Client.ts           # service + layerConfig + layer (+ scoped appendSession/producer)
    S2Error.ts            # typed errors + fromUnknown mapper
    Reader.ts             # read/readBytes, readDecoded; scoped session teardown
    Writer.ts             # append, appendSession, conditionalAppend, producer (scoped), sink
    Channel.ts            # publish, readDecoded channel facade
    internal/
      record.ts           # S2Record schema/codecs, body (string|bytes) handling
    index.ts              # public barrel
  test/
    api-coverage.test.ts  # wrapper surface and SDK constructor re-export coverage
  examples/
    01-quickstart.ts
    02-config-and-tail.ts
    03-producer-sink.ts
    04-tail-while-write.ts
    05-typed-channel.ts
```

### 5.3 Dependency rules

- `src/` MUST NOT import from `test/` or `examples/`.
- `internal/` is not exported from `index.ts`.
- Only `S2Client.ts` imports the SDK client constructor. `Reader.ts`/`Writer.ts` receive the SDK stream handle via the service methods or via a private accessor exported from `S2Client.ts` — they MUST NOT call `new S2`.
- `index.ts` re-exports: the `S2Client` service + its layers; channel helpers; the error types; the SDK value constructors `AppendInput`, `AppendRecord` (re-export so users have one import).

---

## 6. Frozen public contract

> Signatures are locked. `S2Record`, `AppendAck`, `Tail`, `ReadOptions`, `AppendRecord`, `AppendInput` are the SDK types unless noted (confirm exact shapes via V5/V6). Where a parameter type is SDK-owned, alias it in `internal/` rather than restating it.

### 6.1 `S2Client` service (behavioral)

```ts
import { Context, Effect, Scope, Stream } from "effect"
import type { AppendAck, AppendInput, AppendRecord, ReadOptions, StreamInfo, Tail } from "./internal/sdk.js"
import type { S2Error } from "./S2Error.js"

export interface S2Producer {
  // submit applies the SDK session's backpressure; resolves when the record is durable
  readonly submit: (record: AppendRecord) => Effect.Effect<AppendAck, S2Error>
}

export interface S2AppendSession {
  // submit applies append-session backpressure; resolves when the batch is durable
  readonly submit: (
    records: ReadonlyArray<AppendRecord>,
    options?: AppendOptions
  ) => Effect.Effect<AppendAck, S2Error>
}

export interface S2ClientApi {
  readonly createStream: (name: string) => Effect.Effect<StreamInfo, S2Error>
  readonly checkTail: (name: string) => Effect.Effect<Tail, S2Error>
  readonly append: (
    name: string,
    records: ReadonlyArray<AppendRecord>,
    options?: AppendOptions
  ) => Effect.Effect<AppendAck, S2Error>
  readonly read: (name: string, options: ReadOptions) => Stream.Stream<S2Record, S2Error>
  readonly readBytes: (name: string, options: ReadOptions) => Stream.Stream<S2RecordBytes, S2Error>
  readonly appendSession: (
    name: string,
    config: AppendSessionConfig
  ) => Effect.Effect<S2AppendSession, S2Error, Scope.Scope>
  readonly producer: (
    name: string,
    config: ProducerConfig
  ) => Effect.Effect<S2Producer, S2Error, Scope.Scope>
}

export class S2Client extends Context.Service<S2Client, S2ClientApi>()("S2Client") {}
```

`AppendOptions` (locked, fields pending V3):

```ts
export interface AppendOptions {
  readonly matchSeqNum?: number     // compare-and-set: append iff current tail === matchSeqNum
  readonly fencingToken?: string    // single-writer fence  (confirm name/type via V3)
}
export interface ProducerConfig {
  readonly lingerDurationMillis?: number
  readonly maxBatchRecords?: number
  readonly maxBatchBytes?: number
  readonly maxInflightBytes?: number
  readonly maxInflightBatches?: number
}
export interface AppendSessionConfig {
  readonly maxInflightBytes?: number
  readonly maxInflightBatches?: number
}
```

`S2Record` (locked):

```ts
// internal/record.ts — the normalized record we expose to users
export interface S2Record {
  readonly seqNum: number
  readonly timestamp: number
  readonly headers: ReadonlyArray<readonly [string, string]>
  readonly body: string
}

export interface S2RecordBytes {
  readonly seqNum: number
  readonly timestamp: number
  readonly headers: ReadonlyArray<readonly [Uint8Array, Uint8Array]>
  readonly body: Uint8Array
}
```

### 6.2 Service accessors (public API)

All verbs are exposed as `S2Client` static accessors. Pattern (locked) — shown once; every verb follows it:

```ts
// Effect verb
S2Client.append(name, records, options)

// Stream verb
S2Client.read(name, options)
S2Client.readBytes(name, options)

// Scoped verb
S2Client.appendSession(name, config)
S2Client.producer(name, config)
```

Plus the non-service helpers:

```ts
import { Sink } from "effect"
S2Client.sink(producer)
```

### 6.3 Channel facade

```ts
export const publish: <A, I>(name: string, schema: Schema.Schema<A, I>, value: A)
  => Effect.Effect<AppendAck, S2Error | Schema.SchemaError, S2Client>

export const readDecoded: <A, I>(name: string, schema: Schema.Schema<A, I>, options: ReadOptions)
  => Stream.Stream<A, S2Error | Schema.SchemaError, S2Client>

export const conditionalAppend: <A, I>(
  name: string, schema: Schema.Schema<A, I>, value: A, matchSeqNum: number
) => Effect.Effect<AppendAck, S2Error | Schema.SchemaError, S2Client>
```

### 6.4 Layers

```ts
export declare namespace S2Client {
  // env/Config-driven (uses Config.redacted / Config.string aligned with S2Environment — V7)
  const layerConfig: Layer.Layer<S2Client, ConfigError>
  // explicit options; values may be Config<...> or plain
  const layer: (options: {
    readonly accessToken: Config.Config<Redacted.Redacted<string>> | Redacted.Redacted<string>
    readonly basinName: Config.Config<string> | string
    readonly retry?: S2RetryConfig
    readonly forceTransport?: "fetch" | "s2s"
  }) => Layer.Layer<S2Client, ConfigError>
}
```

(`static` members on the class are acceptable per C3; a `namespace` is shown only for signature grouping — the implementation MUST use `static readonly` members or exported constants, not a runtime `namespace`.)

---

## 7. Cross-cutting concerns

### 7.1 Configuration

`layerConfig` reads `S2_ACCESS_TOKEN` via `Config.redacted` and `S2_BASIN` via `Config.string`; align variable names with `S2Environment.parse()` (V7). The SDK client is constructed inside `Layer.effect` with the token unwrapped via `Redacted.value` **only** at the `new S2(...)` boundary. The connection has no release, so no `acquireRelease` at this layer.

### 7.2 Resource & scope discipline

- Read sessions: opened in the `Stream`'s scope via `Stream.unwrapScoped` + `Effect.acquireRelease`; release per V1.
- Producers/append-sessions: `producer` returns within `Scope.Scope`; the SDK `producer.close()` (which flushes outstanding batches) is the release action. Callers wrap in `Effect.scoped` at the edge.
- No `runPromise`/`runSync` anywhere in `src/`.

### 7.3 Observability

Service-method implementations are `Effect.fn`-named (`"S2.append"`, `"S2.read"`, `"S2.producer.submit"`, etc.). Inside `append`/`submit`, `Effect.annotateCurrentSpan({ stream, matchSeqNum? })`; on success annotate the resulting `seqNum`. The read `Stream` carries `Stream.withSpan("S2.read", { attributes: { stream } })`. The library provides **no** telemetry layer; the consuming app composes `@effect/opentelemetry` once at the top (`guide-trace` → OTel integration). Library code stays trace-agnostic.

### 7.4 Retry division (decision D3)

- SDK owns transport-level retries (`retry.maxAttempts`, `appendRetryPolicy`). Default the library to `appendRetryPolicy: "noSideEffects"` so retries are only applied to idempotent (`matchSeqNum`) appends.
- The library adds **no** Effect-level `Schedule` retry around SDK calls (would double-retry). Application-level policy (failover across streams/basins, retrying a whole pipeline) is the caller's concern using `Effect.retry`/`ExecutionPlan` (`guide-retries`), out of scope here.

### 7.5 Exactly-once / fencing (Firegrid-relevant)

`conditionalAppend(matchSeqNum)` is a compare-and-set: it succeeds iff the current tail equals `matchSeqNum`, otherwise it fails with a typed `S2Conflict` (status 409/412 — confirm via V4). This gives idempotent, replay-safe appends without a dedupe table: a re-executed step computing the same `matchSeqNum` either lands exactly once or no-ops. `checkTail` is the read-side anchor for computing the next `matchSeqNum`. The `S2Conflict` error MUST carry the observed/expected tail when the SDK exposes it, so journal callers can branch on "tail advanced" vs. transport failure.

---

## 8. Test strategy

This package does not ship a fake S2 implementation. The wrapper delegates to
the official SDK, and the SDK's own tests are the behavioral oracle for service
semantics such as append ordering, sessions, trim/fence commands, retry, and
control-plane responses.

Package tests MUST stay focused on facade coverage:

- every public SDK operation family has a corresponding `S2Client.*` Effect or Stream accessor;
- SDK-native input/output types are exported from the package barrel;
- SDK value constructors used at the wrapper boundary (`AppendInput`, `AppendRecord.string`, `AppendRecord.bytes`, `AppendRecord.fence`, `AppendRecord.trim`) are re-exported.

---

## 9. Acceptance scenarios (Gherkin)

These are delegated to upstream SDK tests for behavioral semantics. This package may include live examples for smoke testing, but it MUST NOT rebuild an alternate S2 implementation solely to duplicate SDK coverage.

```gherkin
Feature: Read-back roundtrip
  Scenario: appended records are readable by seqNum
    Given a stream "events"
    When I append ["a","b","c"] as string records
    And I read "events" from seqNum 0 with stop count 3
    Then I receive 3 records with bodies ["a","b","c"] in order
    And each record carries an increasing seqNum

Feature: Live tailing
  Scenario: a reader with no stop condition observes records appended after it started
    Given a stream "live"
    And a reader started at tailOffset 0 following live updates
    When I append "x" then "y" after the reader is active
    Then the reader emits "x" then "y" in order
    And the reader does not terminate on its own

Feature: Scope teardown
  Scenario: ending the read scope releases the session
    Given a stream "t" with an active read session inside a Scope
    When the Scope closes (downstream interruption or completion)
    Then the underlying session/subscription is released exactly once
    And no further records are pulled

Feature: Exactly-once append
  Scenario: conditionalAppend is idempotent under replay
    Given a stream "journal" whose tail is N
    When I conditionalAppend value V with matchSeqNum N
    Then it succeeds and the tail becomes N+1
    When I replay conditionalAppend value V with matchSeqNum N
    Then it fails with S2Conflict carrying expected=N observed=N+1
    And the stream contains V exactly once

Feature: Poison record isolation
  Scenario: a body that fails Schema decode does not kill the tail
    Given a stream "orders" containing one record whose body violates the schema
    When I readDecoded "orders" with the Order schema and catchTag SchemaError to a dead-letter sink
    Then the bad record is routed to the dead-letter sink
    And subsequent valid records continue to be decoded and emitted

Feature: Producer backpressure (live-only, skip in CI)
  Scenario: submit suspends when in-flight limit is reached
    Given a producer with a small maxInflightBytes
    When I submit faster than acks resolve
    Then submit suspends (does not buffer unboundedly) until capacity frees
```

---

## 10. Build plan (milestones & tasks)

Ordered; later milestones depend on earlier. Each task ships with tests; a milestone is "done" only when its acceptance gate passes and the Verification Log is updated.

### M0 — Verify (BLOCKING)
- **T-001** Resolve V1–V8 (§4.2); write findings to §11. Gate: every V-row has a source citation (file path + line/symbol) and a resolved value. No code in M2/M3/M4 may start until this passes.

### M1 — Foundations
- **T-010** Scaffold package: `package.json` (`effect@beta`, `@s2-dev/streamstore`; dev `@effect/vitest`, `@effect/platform-node`, all `@effect/*` aligned), `tsconfig` (strict), lint/test wiring.
- **T-011** `internal/sdk.ts`: alias the SDK types we depend on (per V5/V6) — single import boundary for SDK types.
- **T-012** `S2Error.ts`: `S2Error` + status-narrowed variants (`S2NotFound` 404, `S2Conflict` 409/412, `S2Throttled` 429) + `fromUnknown(operation)` mapper (per V4).
- **T-013** `S2Client.ts`: service shape (§6.1), `layerConfig`, `layer`, private stream-handle accessor.
- **Gate M1**: `layerConfig` decodes env and constructs a live client; a trivial `checkTail` integration test (skip-if-no-token) passes; unit test asserts `fromUnknown` maps each status to the right tag.

### M2 — Read path *(depends V1, V2, V6)*
- **T-020** `internal/record.ts`: SDK record → `S2Record` normalization (headers to string pairs; body string handling; bytes decision D5).
- **T-021** `Reader.ts` `read`: `Stream.unwrapScoped` + `acquireRelease` session, `fromAsyncIterable` mapping (per V2), release per V1, `Stream.withSpan`.
- **T-022** `readDecoded` (in `Channel.ts`, but read-side): `Stream.mapEffect` + `Schema.decodeUnknownEffect`.
- **Gate M2**: wrapper read APIs type-check and map SDK records to `S2Record`/`S2RecordBytes`; live smoke examples may be run with credentials.

### M3 — Write path *(depends V3, V5)*
- **T-030** `Writer.ts` `append`: `Effect.fn` + `tryPromise` + span annotate seqNum.
- **T-031** `conditionalAppend`: `matchSeqNum` wiring (V3); CAS failure → `S2Conflict` with expected/observed.
- **T-032** `producer`: scoped handle over SDK `Producer`+`BatchTransform`; `submit` (ticket→ack) preserving backpressure; release = `producer.close()`.
- **T-033** `sink(producer)`.
- **Gate M3**: scenarios *Exactly-once append*, *Producer backpressure* (live) pass; unit test asserts producer scope release closes the session exactly once.

### M4 — Channel & exactly-once façade *(depends M2, M3, V8)*
- **T-040** `Channel.ts` `publish` (encode → append) and `readDecoded` channel; decode/encode errors surface as `Schema.SchemaError` in the channel.
- **T-041** Decide and implement wrap-vs-build against `packages/patterns` (V8); if patterns' framing/dedupe is adopted, route `Channel` through it and record decision D6.
- **Gate M4**: typed roundtrip test (publish `A` → readDecoded `A`) and CAS-conflict-on-channel test pass.

### M5 — DX & examples
- **T-050** `index.ts` barrel + SDK value-constructor re-exports.
- **T-051** Examples 01–10, each runnable with `NodeRuntime.runMain` and provided `S2Client.layerConfig`.
- **Gate M5**: examples type-check and run against live S2 with env set.

### M6 — Hardening
- **T-060** Property tests (`it.effect.prop` with `Schema.toArbitrary`): append-then-read preserves order and bodies for arbitrary record batches; encode∘decode roundtrip for the channel schema.
- **T-061** Error-taxonomy coverage: every `S2Error` variant has a producing test.
- **T-062** README for the package (usage = the five examples; link this SDD).
- **Gate M6 / DoD**: §13 satisfied.

---

## 11. Verification Log (agent fills during M0; append-only)

| ID | Resolved value | Source (path · symbol/line) | Date | Notes / contract impact |
|----|----------------|-----------------------------|------|--------------------------|
| V1 | `ReadSession` is a `ReadableStream` + `AsyncDisposable`; fetch implementation's iterator `return()` cancels its reader, and `[Symbol.asyncDispose]()` calls `cancel()`. Release should cancel/dispose the session. | `node_modules/.pnpm/@s2-dev+streamstore@0.24.1/node_modules/@s2-dev/streamstore/dist/esm/lib/stream/types.d.ts` · `ReadSession` lines 141-158; `.../transport/fetch/index.js` · iterator return / async dispose lines 258-287 | 2026-06-15 | Implementation releases live read sessions with `cancel()`. |
| V2 | Smol `Stream.fromAsyncIterable` has arity `(iterable, onError)` and returns `Stream<A, E>`. `Stream.unwrap` is the scoped unwrap available in this API; there is no `Stream.unwrapScoped`. | `repos/effect-smol/packages/effect/src/Stream.ts` · `fromAsyncIterable` lines 1458-1461; `unwrap` / `scoped` lines 1823-1856 | 2026-06-15 | Reader uses `Stream.unwrap` around an effect containing `Effect.acquireRelease`. |
| V3 | `matchSeqNum` and `fencingToken` are fields on `AppendInput`; `AppendInput.create(records, options)` accepts both. `BatchTransformOptions` also supports them for batched sessions. | `.../streamstore/dist/esm/types.d.ts` · `AppendInput` lines 101-120; `.../batch-transform.d.ts` · `BatchTransformOptions` lines 1-15 | 2026-06-15 | `append` wires options through `AppendInput.create`; channel CAS uses `matchSeqNum`. |
| V4 | SDK exports `S2Error` with numeric `.status`; conditional append rich errors are `SeqNumMismatchError` / `FencingTokenMismatchError`; range errors are `RangeNotSatisfiableError`. Status mapping implemented: 404 not found, 409/412 conflict, 416 range, 429 throttled, else generic. | `.../streamstore/dist/esm/error.d.ts` · `S2Error` lines 23-45, rich error classes lines 61-122 | 2026-06-15 | Added `S2RangeNotSatisfiable` in addition to the SDD's 404/409/412/429 variants. |
| V5 | `AppendAck` is `{ start, end, tail }` with `StreamPosition { seqNum, timestamp: Date }`. `Producer.submit(record)` returns `RecordSubmitTicket`; `ticket.ack()` resolves `IndexedAppendAck`, whose `seqNum()` returns the per-record seq and `batchAppendAck()` returns the batch ack. | `.../streamstore/dist/esm/types.d.ts` · `StreamPosition` lines 18-23 and `AppendAck` lines 174-178; `.../producer.d.ts` · `IndexedAppendAck` / `RecordSubmitTicket` / `Producer.submit` lines 4-17 and 65-80 | 2026-06-15 | `S2Producer.submit` returns the batch `AppendAck` after annotating the per-record seq. |
| V6 | `ReadInput` uses `start.from` variants `{ seqNum }`, `{ timestamp }`, `{ tailOffset }`, `start.clamp`, and `stop.limits` / `untilTimestamp` / `waitSecs`. | `.../streamstore/dist/esm/types.d.ts` · read types lines 123-170 | 2026-06-15 | Public `ReadOptions` aliases SDK `ReadInput`. |
| V7 | `S2Environment.parse()` reads `S2_ACCESS_TOKEN`, `S2_ACCOUNT_ENDPOINT`, and `S2_BASIN_ENDPOINT`; it does not read a basin name. | `.../streamstore/dist/esm/common.js` · `S2Environment.parse` lines 1-20 | 2026-06-15 | `layerConfig` reads SDK env for token/endpoints via `S2Environment.parse()` and adds package-level `S2_BASIN` for the basin. |
| V8 | Higher-level helpers are in the separate `@s2-dev/streamstore-patterns` package, not the core SDK package. They target bytes framing, serialization sessions, chunking, and dedupe around raw SDK sessions. | `/tmp/s2-patterns/package/package.json` · package/export metadata lines 1-18 and peer dependency lines 31-33; `/tmp/s2-patterns/package/dist/patterns/serialization.d.ts` · session helpers lines 1-59 | 2026-06-15 | v1 channel does not adopt this dependency; it uses Effect `Schema` JSON encode/decode for the locked API. |
| V9 | The upstream README and SDK tests exercise bytes format reads, append sessions, producer batching/order, session CAS, and read-session progress. | `https://github.com/s2-streamstore/s2-sdk-typescript/blob/main/README.md`; `https://github.com/s2-streamstore/s2-sdk-typescript/tree/main/packages/streamstore/src/tests` (`appendSession.test.ts`, `readSession.e2e.test.ts`, `producer.test.ts`, `formats.e2e.test.ts`) | 2026-06-15 | Added service-owned `appendSession`, `readBytes`, and conformance tests for session batches, session CAS, byte reads, and producer ordering. |

Any divergence from §4.1 MUST also edit §4.1 and the affected §6 signature, with a one-line decision in §12.

---

## 12. Decision register

| ID | Decision | Rationale |
|----|----------|-----------|
| D1 | Wrap the SDK; do not reimplement transport. | SDK owns S2S/HTTP-2 framing + pipelining; reimplementation loses it for no gain. |
| D2 | Single behavioral `S2Client` service + service-owned static accessors. | S2 reads are per-call queries, not long-lived subscriptions; putting verbs on the service keeps the external API idiomatic and avoids package-level thin accessor functions. |
| D3 | SDK owns transport retries (`appendRetryPolicy: "noSideEffects"` default); no Effect-level retry in the library. | Avoid double-retry; keep idempotency honest. |
| D4 | Errors are schema-tagged with status-narrowed variants. | `catchTag` ergonomics for 404/409/412/429; `Schema.Defect` preserves the SDK cause. |
| D5 | Expose both normalized string reads and byte-oriented reads. | The SDK supports mixed string/bytes records and `readSession(..., { as: "bytes" })`; `S2Client.read` remains ergonomic for JSON/string workflows while `S2Client.readBytes` preserves binary payloads and headers. |
| D6 | Do not adopt `@s2-dev/streamstore-patterns` in v1 `Channel`. | The package's helpers are bytes/framing/dedupe sessions; the locked channel API only needs schema JSON encode/decode over string records. |
| D7 | `createStream` returns the SDK `CreateStreamResponse`. | The facade preserves SDK-native input/output types instead of synthesizing alternate response models. |

## 13. Definition of done

- All milestone gates pass; Verification Log complete with sourced citations.
- `tsc --noEmit` clean under strict; lint clean; no `any`/`as`/non-null-assertion/`namespace` in `src/`.
- Package API coverage tests pass; live examples can be run with a token for smoke testing.
- Public surface matches §6 exactly (or a §12 entry justifies each change).
- Examples 01–05 run against live S2.
- Package README links this SDD; `@firegrid/log` importable.

---

## Appendix A — reference implementation sketches (non-binding, for the tricky modules)

> These illustrate intended shape. The agent implements to the §6 contract and §9 behavior, not to these literals. Lines marked `// VERIFY` correspond to §4.2.

**Reader (scoped session → Stream):**
```ts
read: (name, options) =>
  Stream.unwrapScoped(
    Effect.gen(function*() {
      const stream = yield* streamHandle(name)              // private accessor from S2Client.ts
      const session = yield* Effect.acquireRelease(
        Effect.tryPromise({ try: () => stream.readSession(options), catch: fromUnknown("readSession") }),
        () => Effect.promise(async () => { /* VERIFY V1: iterator return() vs stream.close() */ })
      )
      return Stream.fromAsyncIterable(session, fromUnknown("read"))  // VERIFY V2: name/arity
        .pipe(Stream.map(toS2Record))
    })
  ).pipe(Stream.withSpan("S2.read", { attributes: { stream: name } }))
```

**Producer (scoped handle):**
```ts
producer: (name, config) =>
  Effect.gen(function*() {
    const stream = yield* streamHandle(name)
    const handle = yield* Effect.acquireRelease(
      Effect.tryPromise({
        try: async () => new Producer(new BatchTransform(config), await stream.appendSession({
          maxInflightBytes: config.maxInflightBytes
        })),
        catch: fromUnknown("appendSession")
      }),
      (p) => Effect.promise(() => p.close())     // close() flushes outstanding batches
    )
    const submit = Effect.fn("S2.producer.submit")(function*(record: AppendRecord) {
      const ticket = yield* Effect.tryPromise({ try: () => handle.submit(record), catch: fromUnknown("submit") })
      const ack = yield* Effect.tryPromise({ try: () => ticket.ack(), catch: fromUnknown("ack") })
      yield* Effect.annotateCurrentSpan({ stream: name, seqNum: ack.seqNum() })  // VERIFY V5
      return ack
    })
    return { submit }
  })
```

**Error mapper:**
```ts
export const fromUnknown = (operation: string) => (cause: unknown): S2Error => {
  const status = (typeof cause === "object" && cause !== null && "status" in cause)
    ? Number((cause as { status: unknown }).status)
    : undefined
  // VERIFY V4: map 404→S2NotFound, 409/412→S2Conflict, 429→S2Throttled, else S2Error
  return S2Error.make({ operation, status, cause })
}
```
