# SDD: effect-s2-flow → Durable Execution

### A layered build from the S2 substrate up to a Restate-style authoring surface

|                       |                                                                                                   |
| --------------------- | ------------------------------------------------------------------------------------------------- |
| Status                | Draft for implementation                                                                          |
| Date                  | 2026-06-23                                                                                         |
| Packages (provisional) | `effect-s2` (transport) · the orchestrator + `TableView` **substrate** — incubates in `effect-s2-stream-db`, graduates to `effect-s2-flow` only once proven (see Packaging) · `effect-s2-durable` (authoring). *The layering is firm; the package cut is deliberately deferred.* |
| Supersedes            | `effect-s2-flow-sdd.md` (folded in as Layers 0–6) and the processor portion of the prior stream-db SDD |
| Reference vocabulary  | Pulsar (Functions, transactions, IO, TableView, schema); the S2 shared-log KV demo; Restate / `restate-sdk-gen` |
| effect-smol           | Effect v4 (`Context.Service`, `Layer`, `Stream`, `Queue`, `PubSub`, `Deferred`, `Fiber`, `FiberRef`, `RequestResolver`, `LayerMap`, cluster entities) |

---

## How to read this document

The design is a stack. Each layer is implementable and testable on its own and depends only on the layers below it. Build them in order; each has a conformance gate (collected in **Build Plan**) that must pass before the next.

```
Layer -1  effect-s2 fidelity             generated S2 protocol + Effect semantic capabilities
Layer 0   S2 substrate                 six native guarantees
Layer 1   Orchestrator                 the per-key engine + the concurrency fork      ← runtime core
Layer 2   Streams                      EventStream · Record · Source · Sink
Layer 3   State materialization        TableView (fold · getStrong · compaction)
Layer 4   Processing                   Processor (Functions) · guarantees
Layer 5   Atomicity                    the batch is the only unit — no coordinator
Layer 6   Ownership                    lease · fence · assignment · subscription types
Layer 7   Durable extensions           TimerService · suspend/resume · child correlation   ← turns flow into a DX substrate
Layer 8   CurrentInvocationScope       the seam the Processor runner provides per invocation
Layer 9   Durable primitives           run · state · sleep · signal · awakeable · deferred · combinators
Layer 10  Authoring surface            service · object · workflow · client     ← restate-sdk-gen-shaped
Layer 11  Worked examples              greeter · basics · counter · blockAndWait · checkout
```

**The thesis.** Pulsar centralizes four mechanisms in a broker (dispatch, transaction coordinator, schema registry, compaction); the S2 KV demo shows the materialization/orchestration engine; the baseline `StreamDb` is the locally-serialized CAS writer (the fenced-multi-writer generalization is the work here). Compose them and you get a **per-key stream as the unit of everything**, with the conditional append as the coordination primitive and `Schema` as the schema system. Layered up, that substrate bottoms out a Restate-style durable-execution authoring surface **without re-implementing a scheduler** — Effect *is* the scheduler `restate-sdk-gen` hand-builds. `Effect` is the `Operation`, `Fiber` is the `Future`, and the durable layer is a thin set of primitives plus three flow extensions. "Effect all the way down" stays literally true because flow's primitives are themselves Effects.

---

# Layer -1 — `effect-s2` Fidelity

Before building flow/durable semantics, `effect-s2` must stop being a manually-curated mirror of the S2 client surface. The wrapper is currently the highest-risk place to accidentally narrow upstream semantics: append sessions, Producer tickets, fencing, dedupe headers, framing/chunking, and exact u64 handling are exactly the primitives flow needs, and several of them are not expressible through today's broad `S2ClientApi` without re-invention above it.

**Decision: generate the S2 protocol layer from S2's OpenAPI spec with HeyAPI as the parser/plugin orchestrator.** Use the same spec source as upstream S2, but emit the protocol binding in the shape this repository actually wants: Effect `Schema`, an Effect `HttpApi` contract, an `HttpApiClient`-derived client, typed protocol errors, and a semantic handwritten layer above the generated protocol.

- spec source: `s2-streamstore/s2-specs/s2/v1/openapi.json` pinned to commit `329de93f7b240a4daef9edbeb98ced0699aab7d0`;
- spec input: the pinned upstream raw URL, not a checked-in copy of the OpenAPI document;
- primary generator: `@hey-api/openapi-ts` with first-class local `effect-schema` and `client-effect` plugins; `client-effect` derives the SDK through Effect's `HttpApiClient`, not through a custom request runtime;
- reference material: Effect's native OpenAPI generator utilities and generated output shape are useful design references, but the CLI is not the repository's generation path;
- handwritten layer: only runtime behaviors OpenAPI cannot describe.

This is not "manual wrapper versus codegen." Codegen is the spine. The correction is that **HeyAPI remains the parser/plugin orchestrator** because it already owns the OpenAPI normalization and symbol pipeline, while the local plugins emit Effect-native artifacts directly. Do **not** post-process generated code, keep `src/heyapi-client.ts` deleted, and do **not** hand-maintain a giant `S2ClientApi` to compensate for generator gaps.

**Initial clean-generator result (2026-06-23).** `effect-s2` now has a local HeyAPI generation path pinned to `s2-specs@329de93f7b240a4daef9edbeb98ced0699aab7d0/s2/v1/openapi.json`. The generator emits `src/generated/effect-schema.gen.ts` from the local `effect-schema` plugin and `src/generated/client-effect.gen.ts` from the local `client-effect` plugin. It does not emit HeyAPI plain TypeScript models or a post-processed runtime adapter. The generated client module defines `S2Api` with Effect `HttpApi`/`HttpApiEndpoint` groups, including the S2 read endpoint's `text/event-stream` success alternative, derives `S2ProtocolClientApi` with `HttpApiClient.ForApi`, and exposes `make`/`layer` for the grouped derived client shape documented by Effect. Snapshot tests follow HeyAPI's own `openapi-ts-tests` file-snapshot pattern and generate from the pinned upstream URL rather than a checked-in spec file.

Recommended physical shape:

```txt
packages/effect-s2/src/
  generated/          generated from s2-specs; no manual edits
  transport/          auth, basin/env config, retry, error normalization
  control-plane/      basins, tokens, metrics, stream CRUD
  streams/            append, read, checkTail, fence/trim command records
  sessions/           append session, ticketed submit, Producer
  patterns/           serialization, framing, dedupe headers, u64 codec
  index.ts            curated exports
```

The generated layer is a protocol binding, not the public architecture. Downstream packages should depend on semantic capabilities:

```ts
interface S2StreamClient {
  readonly append: ...
  readonly read: ...
  readonly checkTail: ...
}

interface AppendSession {
  readonly submit: (input: AppendInput) => Effect.Effect<AppendTicket, S2Error>
}

interface AppendTicket {
  readonly ack: Effect.Effect<AppendAck, S2Error>
}

interface SerializingAppendSession<A> {
  readonly write: (value: A) => Effect.Effect<AppendAck, S2Error>
}
```

**Adopt upstream patterns rather than rebuilding them.**

- Ordered/backpressured writes: S2 append session and Producer API.
- Per-record ordered durability: Producer ticket ack with exact seqnum.
- Large logical messages: `patterns/serialization` framing/chunking.
- Shared-stream logical dedupe: `injectDedupeHeaders` / `DedupeFilter`.
- Exact u64 boundary handling: `encodeU64` / `decodeU64`.
- Fencing and CAS: pass through `fencingToken` and `matchSeqNum` on every append path, including one-shot append, append session, Producer, and serialized writer.

**Conformance gate L-1.** The generator emits the protocol module from the pinned S2 OpenAPI spec through HeyAPI and local Effect-native plugins. The generated output typechecks, includes the load-bearing operations and schemas, and has file snapshots for a focused append/read/checkTail/list-basins slice. The semantic layer then proves the native SDK behavior flow depends on: `matchSeqNum`, `fencingToken`, append-session ticketing/backpressure, Producer per-record seqnums, serialization roundtrip, dedupe headers, and u64 encode/decode. Until this passes, flow must not add wrapper-specific workarounds.

---

# Layer 0 — S2 Substrate

Everything is built on six native S2 guarantees. Layer -1 is responsible for exposing them faithfully through `effect-s2`; flow/durable must not re-invent these behaviors above a narrowed wrapper.

| Guarantee | Semantics | Used for |
| --- | --- | --- |
| **Atomic batch** | ≤1000 records / 1 MiB, all-or-none, multi-AZ-durable before ack | atomic commit (Layer 5) |
| **`match_seq_num`** | optimistic CAS; 412 on mismatch; with retry ⇒ exactly-once | idempotent output, checkpoint CAS |
| **`fence` command record** | pessimistic, **strongly consistent**, **cooperative** (a no-token append still lands), ≤36 B, empty clears | ownership / incarnation (Layer 6) |
| **`check_tail`** | current tail seqnum; cheap, storage-class-independent | linearizable-read barrier (Layer 3) |
| **Append session** | pipelined, submission-ordered, barrier-on-failure; a 412 poisons the session | the ordered writer (Layer 1) |
| **Lease via heartbeat** | model owner identity by periodic in-stream heartbeats; readers confirm with `check_tail` | failover detector (Layer 6) |

Two tiering knobs: **Express (40 ms ack)** vs **Standard (400 ms ack)** storage classes — invocation/inbox streams use Express; cold logs and large views use Standard.

**Load-bearing caveat (cooperative fencing).** A `fence` rejects writers presenting the *wrong* token; a writer presenting *no* token is allowed. Therefore **every owner-write the runtime issues carries the current fencing token**, or the fence protects nothing. The runner enforces this; it is not optional.

**Command records** (`fence`, `trim`) are records with a single empty-name header, seq-numbered and returned to reads, filterable by `headers.length === 1 && headers[0][0] === ""`. Because they are records, they batch atomically with data — which is how a snapshot and its trim land together (Layer 3), and how a fence can be co-committed with data.

**Conformance gate L0.** The native guarantees are available through the Layer -1 semantic capabilities: `append` (with `fencingToken` and `matchSeqNum`), `read`, `checkTail`, faithful append sessions / Producer tickets, serialized/framed logical messages, dedupe headers, and command records. This layer is the contract the rest of the document targets.

---

# Layer 1 — The Orchestrator (runtime core)

One instance per stream (= per key). It is the Effect translation of the S2 KV demo's `orchestrate` loop. It owns the materialized state, a command mailbox, a tailing reader, a pending-cursor heap, and an ordered writer. Everything above is a specialization of this engine with a different record-handler.

### 1.1 The concurrency fork (the single most important decision)

| Model | Apply discipline | Linearizable read | Writers | Used by |
| --- | --- | --- | --- | --- |
| **Fenced single-writer** | apply-on-ack (write → ack → apply own write) | trivial (own state) | one, fenced | **Processors** (Layer 4), exclusive object handlers |
| **Multi-primary** | apply-on-tail (apply only when the record returns on the tailing read) | `check_tail` + defer | many, concurrent | **TableViews** (Layer 3), shared object reads |

`StreamDb` today is apply-on-ack and correct *only because* an in-process semaphore guarantees one writer — it is **CAS-guarded + locally serialized (`Semaphore(1)` + `matchSeqNum`), not yet fenced**; threading a `fencingToken` through its appends is a required change (Build Plan). The KV demo is apply-on-tail because any replica may write concurrently. **You cannot keep apply-on-ack and add a second writer.** The two compose: the fenced owner is the one writer; the view replicas are the many readers of the same stream. This fork resurfaces at every layer — it *is* Restate's `state()`-exclusive vs `sharedState()`-concurrent distinction (Layer 10).

**These are two concrete implementations, not one parametric loop.** (A single loop that both applies-on-ack *and* lets its tail reader apply will **double-apply** own records — the reviewers caught this in the §1.3 sketch.) Build them separately:

- **`OwnedOrchestrator`** (fenced single-writer). Recovers by folding from the last snapshot, then **applies its own writes on ack — but strictly in stream order** (this is the reviewer's finding: a foreign record can take seq `N` while the owner's write takes `N+1`; applying `N+1` on ack before the tail reader has applied `N` would reorder the fold). The rule: an acked own write at `ack.start.seqNum === applied` is **fast-pathed** (apply immediately → advance `applied`); otherwise it is held in a **pending-own** set and applied only when `applied` catches up to it, and the write's reply Deferred (what makes a handler's `set` return) **completes after the ordered local apply, not on raw ack**. So **read-your-writes holds** (the `set` doesn't resolve until its effect is applied) *and* the fold never reorders. The owner still tails for **foreign ingress** (signals, timer-fires, child-completions from *other* producers) and applies those in tail order; own records are recognized (incarnation header / pending-own seq set) so they are applied exactly once, by whichever path reaches them first. (`ctx.emit` to a *separate* downstream stream is not own-state and does not feed RYW; Layer 4.)
- **`ViewOrchestrator`** (multi-primary). No fence, no own-writes; **applies purely on tail** (already in order); `getStrong` is `check_tail` + defer. N replicas run it concurrently over the same stream. This is the loop sketched in §1.3.

RYW is therefore an `OwnedOrchestrator` guarantee and a deliberate *non*-guarantee of `ViewOrchestrator` reads (a view is eventually-consistent by construction; `getStrong` is its linearizable escape hatch).

**Use the SDK's append primitives — don't hand-roll the writer.** The ordered-writer fiber, batching, and backpressure in §1.3 are exactly what the S2 SDK's **append session** and **Producer API** already provide: a session "maintains strict ordering of records across batches" and the Producer gives **per-record ordered durability with the correct seqNum on each ticket** (`ack.seqNum()`), plus built-in backpressure (`maxInflightBytes`, default 5 MiB, blocks `submit()`). So the `OwnedOrchestrator` writer is a thin wrapper over a `Producer`/append-session — not a re-implementation — and the bounded-write-queue / backpressure story (Opus point 5, write side) is the SDK's, not ours to invent. The out-of-order risk above is purely an *apply-side* concern (in-memory fold ordering); the SDK already guarantees durable order = submit order, so this is not an S2-API or wrapper bug.

### 1.2 Idiom mapping (tokio `main.rs` → effect-smol)

| KV demo (tokio) | effect-smol |
| --- | --- |
| `mpsc::UnboundedSender<OrchestratorCommand>` | `Queue` (single consumer = the lock; replaces `Semaphore`) |
| `oneshot::Sender` | `Deferred` |
| `tokio::select!` over command/ack/record | `Stream.merge` consumed by one `forkScoped` fiber |
| `FuturesOrdered` of append tickets | a dedicated ordered writer fiber draining a `Queue` |
| `BinaryHeap<Reverse<seq>>` (`PendingResponses`) | `SortedMap<number, Waiters>`, drained on apply |
| bus-stand `VecDeque` + `sleep_until` | `Request` + `RequestResolver` (batch window) |

### 1.3 Shape

```ts
// runtime/Orchestrator.ts — the ViewOrchestrator loop (multi-primary, apply-on-tail). The OwnedOrchestrator
// reuses this scaffold but (a) applies own writes in the Writer fiber on ack and (b) filters own records in the
// `rec` branch (see §1.1). `pending` strong-reads carry a deadline (cfg.readDeadline) so a stalled tail reader
// can't pin client requests indefinitely. Surfaced via TableView (L3) and Processor (L4).
type Cmd<S> =
  | { _tag: "Write";        records: ReadonlyArray<AppendRecord>; reply: Deferred.Deferred<AppendAck, FlowError> }
  | { _tag: "ReadStrong";   atTail: number; project: (s: S, applied: number) => unknown; reply: Deferred.Deferred<unknown> }
  | { _tag: "ReadEventual"; project: (s: S, applied: number) => unknown; reply: Deferred.Deferred<unknown> }

interface Orchestrator<S> {
  readonly command: Queue.Enqueue<Cmd<S>>                          // many fibers offer; one fiber consumes
  readonly applied: Effect.Effect<number>                          // current applied-prefix cursor
  readonly changes: Stream.Stream<EventRecord<unknown, unknown>>   // post-apply notifications (drives L7 resume)
}

const make = Effect.fn("Orchestrator.make")(function*<S>(opts: {
  readonly stream: string
  readonly initial: S
  readonly reduce: (state: S, record: EventRecord<unknown, unknown>) => S
  readonly fromCursor: number
  readonly fencingToken?: string   // present ⇒ fenced single-writer (apply-on-ack); absent ⇒ multi-primary
}) {
  const command = yield* Queue.bounded<Cmd<S>>(cfg.commandCapacity)   // bounded: backpressure on intake (no unbounded inbox)
  const writes  = yield* Queue.bounded<{ records: ReadonlyArray<AppendRecord>; reply: Deferred.Deferred<AppendAck, FlowError> }>(cfg.writeCapacity)
  const changes = yield* PubSub.dropping<EventRecord<unknown, unknown>>(cfg.changesCapacity)   // slowest-consumer ⇒ drop, never OOM a hot stream
  const appliedRef = yield* Ref.make(opts.fromCursor)
  let state = opts.initial
  let pending = SortedMap.empty<number, Array<Cmd<S>>>(Order.number)

  // ordered writer fiber (FuturesOrdered analog): submit awaits durable ack, FIFO, carries the fence
  const session = yield* S2Client.appendSession(opts.stream)
  yield* Stream.fromQueue(writes).pipe(
    Stream.runForEach(({ records, reply }) =>
      session.submit(AppendInput.create(records, opts.fencingToken ? { fencingToken: opts.fencingToken } : undefined)).pipe(
        Effect.matchCauseEffect({
          onFailure: (c) => Deferred.failCause(reply, Cause.map(c, toFlowError("write"))),
          onSuccess: (ack) => Deferred.succeed(reply, ack) }))),
    Effect.forkScoped)

  // the select!: commands ⊕ tailing reader, one consumer fiber, owns `state` and `pending`
  yield* Stream.merge(
    Stream.fromQueue(command).pipe(Stream.map((c) => ({ k: "cmd" as const, c }))),
    Channel.readDecoded(opts.stream, RecordSchema, { start: { from: { seqNum: opts.fromCursor } } }).pipe(
      Stream.map((r) => ({ k: "rec" as const, r }))),
  ).pipe(
    Stream.runForEach((ev) => Effect.gen(function*() {
      if (ev.k === "rec") {
        state = opts.reduce(state, ev.r)
        const applied = ev.r.cursor.seqNum + 1
        yield* Ref.set(appliedRef, applied)
        yield* PubSub.publish(changes, ev.r)                       // L7 suspend/resume subscribes here
        const [ready, rest] = SortedMap.partitionByKey(pending, (k) => k <= applied)
        pending = rest
        for (const [, waiters] of ready) for (const w of waiters) yield* resolveWaiter(w, state, applied)
      } else {
        const applied = yield* Ref.get(appliedRef)
        switch (ev.c._tag) {
          case "Write":        yield* Queue.offer(writes, ev.c); break
          case "ReadEventual": yield* Deferred.succeed(ev.c.reply, ev.c.project(state, applied)); break
          case "ReadStrong":
            ev.c.atTail <= applied
              ? yield* Deferred.succeed(ev.c.reply, ev.c.project(state, applied))
              : (pending = SortedMap.appendAt(pending, ev.c.atTail, ev.c))
        }
      }
    })),
    Effect.forkScoped)

  return { command, applied: Ref.get(appliedRef), changes: Stream.fromPubSub(changes) }
})
```

`check_tail` batching is a resolver — cleaner than the demo's hand-rolled bus-stand:

```ts
// runtime/CheckTail.ts — coalesce concurrent strong reads into one checkTail per window (the "bus-stand")
class CheckTail extends Request.Class<number, FlowError, { readonly stream: string }>()("CheckTail") {}
const CheckTailResolver = RequestResolver.makeBatched((reqs: ReadonlyArray<CheckTail>) =>
  Effect.forEach(Array.groupBy(reqs, (r) => r.stream), ([stream, group]) =>
    S2Client.checkTail(stream).pipe(
      Effect.flatMap((t) => Effect.forEach(group, (r) => Request.succeed(r, t.tail.seqNum)))))
).pipe(RequestResolver.batchN(256))
```

**Conformance gate L1.** Reproduce the KV demo's externally observable semantics on a single stream: strong vs eventual reads, in-order write acks, recover-from-cursor. If the Effect loop passes the demo's behavior as a harness, the core is sound.

---

# Layer 2 — Streams

The data-plane vocabulary, built directly on `effect-s2`'s `Channel` (schema⇄JSON `publish` / `readDecoded` / `guardedAppend` / `conditionalAppend`).

```ts
// stream/EventStream.ts — a declaration. Physical stream per key: `${name}/${encode(key)}`.
interface EventStreamDef<K, A> { readonly name: string; readonly key: Schema.Codec<K, string>; readonly value: Schema.Codec<A, string> }

// stream/Record.ts
interface EventRecord<K, A> { readonly stream: string; readonly key: K; readonly value: A; readonly cursor: EventCursor; readonly headers: ReadonlyMap<string, string> }
interface EventCursor { readonly stream: string; readonly seqNum: SeqNum }   // SeqNum: logically S2 Rust `SeqNum(u64)`
// The @s2-dev/streamstore TS SDK surfaces seqNum as `number` (f64) but ALSO ships `encodeU64`/`decodeU64`
// (in patterns/serialization) for exact u64 handling — use those at the boundary rather than depending on the f64.
// Practically a non-issue under per-key sharding anyway: per-stream record rate puts 2^53 at millennia, so only a
// single extreme-firehose stream is even theoretically exposed. Disposition: prefer the SDK u64 codec; flag upstream; do NOT gate.

// stream/Source.ts — external → stream
interface SourceDef<K, A> { readonly name: string; readonly output: EventStream<K, A>; readonly run: (sink: EventSink<K, A>) => Effect.Effect<void, FlowError, Scope.Scope> }
// stream/Sink.ts — stream → external, with a declared guarantee
interface SinkDef<K, A> { readonly name: string; readonly input: EventStream<K, A>; readonly guarantee: Guarantee; readonly run: (source: EventSource<K, A>, checkpoint: CheckpointStore) => Effect.Effect<void, FlowError, Scope.Scope> }
```

**Two write paths, by ownership** (this distinction recurs everywhere):

- `Flow.submit(stream, key, value)` — **unfenced** append. Multi-producer command/inbox streams (clients submitting work, signals, child requests). Ordered by S2 seqnum, like the demo's multi-primary writes. No token.
- `ctx.emit(output, value)` inside a `Processor` — **fenced, guarded** append. The processor is the single active writer for its key; emits carry the fence and, under `effectivelyOnce`, a dedup key — positional `match_seq_num` when the output is the owner's *own* stream, an explicit `logicalId` when it is a *shared* downstream (Layer 4).

Wire-format sources/sinks use `Stream.pipeThroughChannel` with the `Ndjson`/`Msgpack` channels as the codec stage.

**`ChangeMessage` stays in `stream-db`.** Flow streams carry domain facts on the generic `Channel` envelope; flow **never imports `ChangeMessage`**, which is the table-changelog protocol layered *above* the engine (Layer 3 note).

**Conformance gate L2.** Schema encode/decode errors are typed (`FlowError reason="decode"`); missing and empty streams resolve consistently (a 404 vs a tail-0 416 both mean "nothing to fold"); `guardedAppend`/fence options pass through to `effect-s2`.

---

# Layer 3 — State Materialization (TableView)

The orchestrator, fold-only, exposed as a `Schema`-typed service. This is the KV demo's read path, generalized over the reducer — and the seed that **moves down** from `stream-db`'s `MaterializedState`.

```ts
// table/TableView.ts
interface TableViewDef<K, V> {
  readonly name: string
  readonly source: EventStream<unknown, unknown> | ReadonlyArray<EventStream<unknown, unknown>>
  readonly key: (record: EventRecord<unknown, unknown>) => K
  readonly reduce: (state: HashMap.HashMap<K, V>, record: EventRecord<unknown, unknown>) => HashMap.HashMap<K, V>
  readonly storageClass?: "express" | "standard"   // default standard for views
}
interface TableView<K, V> {
  readonly get:        (key: K) => Effect.Effect<Option.Option<V>, FlowError>   // eventual (local apply-prefix)
  readonly getStrong:  (key: K) => Effect.Effect<Option.Option<V>, FlowError>   // linearizable: checkTail → defer until applied ≥ tail → read
  readonly refresh:    Effect.Effect<number, FlowError>                         // fold through *at least* the current tail; return that cursor
  readonly entries:    Effect.Effect<ReadonlyArray<readonly [K, V]>, FlowError>
  readonly changes:            Stream.Stream<readonly [K, V], FlowError>        // future changes only (Pulsar forEachAndListen)
  readonly snapshotAndChanges: Stream.Stream<readonly [K, V], FlowError>        // current map, then follow
}
```

- `getStrong`/`refresh` issue `Effect.request(new CheckTail({ stream }), CheckTailResolver)`, then submit a `ReadStrong` whose `atTail` is the returned cursor; the loop blocks it in `pending` until `applied ≥ atTail`. This is the demo's `reflect_applied_state` discipline, exactly.
- `changes` = `Stream.fromPubSub(orchestrator.changes)` projected to `[key, value]`.
- **Compaction** = `Checkpoint` (snapshot-start + entries + snapshot-end + `trim`, one atomic batch, durable-before-trim). S2 has **no key-compaction** — this snapshot+trim *is* the compaction; cold start folds from the last snapshot (`fromCursor` = snapshot point), so snapshot cadence is a real tuning knob.
- **Memory bound = per-key-stream sharding.** A view over a per-key stream holds one key's history; a view over a shared multi-key stream must be cardinality-bounded or use snapshot-only point lookups. **Never a global cross-key view** — that is the OOM cliff.
- **Multi-primary**: apply-on-tail is mandatory here (this surface does *not* assume a single writer), which is why N nodes each run the runner and serve reads of the same view concurrently.

```ts
// table/Checkpoint.ts — snapshot the live set at tail + trim before it, one atomic batch
const checkpoint = (stream: string) => Effect.gen(function*() {
  const cursor = yield* currentTail(stream)
  const entries = liveEntries()
  const records = [ snapshotStart(cursor), ...entries.map(asInsert), snapshotEnd(cursor), AppendRecord.trim(cursor) ]
  yield* S2Client.append(stream, AppendInput.create(records, { matchSeqNum: cursor }))  // trim lands iff snapshot does
})
```

**Conformance gate L3.** A linearizable strong-read immediately after a concurrent write reflects it; cold-start fold from a snapshot equals full-replay state; `changes` delivers every applied mutation once.


---

# Layer 4 — Processing (Functions)

A `Processor` is the orchestrator with a handler spliced into the record path and a guarded-output commit. It implements Pulsar's Function model. **Outputs live in the handler's R channel as typed services** (the cucumber-effect `WorldServices`-at-the-root pattern): a handler physically cannot emit to an undeclared stream — emit is type-checked against the declaration, no string routing.

```ts
// processor/Processor.ts
interface ProcessorDef<K, In, Out extends Record<string, EventStream<any, any>>, St extends Record<string, TableView<any, any>>, R> {
  readonly name: string
  readonly input: EventStream<K, In>           // the per-key ordered inbox (In may be a Schema.Union)
  readonly outputs: Out                          // become typed emit services in R
  readonly state?: St                            // read-side views available to the handler
  readonly guarantee: Guarantee
  readonly keyAffinity?: (record: EventRecord<K, In>) => K   // default: record.key
  readonly retry?: ProcessorRetryPolicy
  readonly handler: (record: EventRecord<K, In>, ctx: ProcessorContext<Out, St>) =>
    Effect.Effect<void, ProcessorFailure, R | OutputServices<Out>>
}
interface ProcessorContext<Out, St> {
  readonly emit:  <N extends keyof Out>(output: N, value: ValueOf<Out[N]>) => Effect.Effect<void>
  readonly state: { readonly [N in keyof St]: St[N] }   // view reads, pinned to the owner's applied state
  readonly cursor: EventCursor                            // the input record's cursor
  readonly attempt: number
}
```

**Execution (per inbound record).** The runner runs `handler`, buffering its own-journal emits + checkpoint movement, then writes them as **one atomic append batch on the owner's own stream** (Layer 5 — there is no transaction resource, just the atomic batch). Reads inside the handler (`ctx.state.*`) are the owner's **apply-on-ack** state (RYW, §1.1).

**Dedup is positional only where position is deterministic — be precise.** `match_seq_num` is a CAS on the *current tail*, **not** a logical append-if-absent on an output id. It yields effectively-once **only** for a stream whose sole writer is this owner *and* whose replay re-issues the identical record sequence — i.e. the owner's **own journal** (the `StepCompleted`/`StateChanged` records of Layer 9): record K's `match_seq_num` is the position after K-1, so a replayed re-append 412s. For an output to a **separate downstream stream shared with other producers**, the tail is moved by those producers and `match_seq_num` gives *no* uniqueness — that path needs an explicit **logical dedup key**, and the S2 SDK already ships it: the `patterns/serialization` **dedupe headers** (`injectDedupeHeaders(writerId, dedupeSeq)` on write + `DedupeFilter` on read) tag each record with a `(writerId, monotonic-seq)` pair that consumers drop duplicates by. Use that (a maintained, tested mechanism) rather than inventing one, or route to a per-`(owner,key)` output stream so the owner is again the sole writer. Each output declares which mode it is in; raw `match_seq_num` is never the shared-stream answer.

**Processing guarantees** — a function of two knobs S2 gives natively: *when the checkpoint commits* and *whether the output is guarded*.

| Guarantee | Loop behavior | Outcome |
| --- | --- | --- |
| `atMostOnce` | commit checkpoint **before** running handler | crash ⇒ record skipped |
| `atLeastOnce` | handler → emit → **then** checkpoint | crash ⇒ reprocess, output may duplicate |
| `effectivelyOnce` | at-least-once **+** guarded emit (deterministic id) | replay 412s ⇒ one durable output per input |

`effectivelyOnce` is exactly-once for *durable outputs* — by **positional dedup on the owner's own journal**, or a **logical dedup key** for shared downstream outputs (above); never raw tail-CAS on a shared stream. For *external side effects* it is at-least-once + the provider's idempotency key — always. The guarantee is a `Layer`: the checkpoint-vs-emit ordering sets at-most/at-least; the dedup mechanism sets the *once*.

**Multi-source inputs.** Pulsar Functions consume from **multiple input topics** (a function instance has "a collection of consumers consuming from different input topics"), so flow supports multi-input too — but the two surfaces differ on ordering and that difference is load-bearing:

- **Durable-execution default = funnel.** For per-key durable execution (objects/workflows), a key's logical sources (commands + timer/promise events + child completions — Layer 7) are written *into* the one per-key inbox upstream, so the handler sees a single ordered `In = A | B | C` union and `Match`es. One stream ⇒ one cursor ⇒ a total per-key order; this is required for deterministic replay.
- **General processing = first-class multi-input.** A `Processor` may declare `input: ReadonlyArray<EventStream>` for order-insensitive stream processing (Pulsar-style fan-in). **Per-input order is preserved; cross-input interleaving is non-deterministic** (N independent cursors). Permitted only when the handler is commutative across inputs or `effectivelyOnce` makes re-order immaterial — never for a handler whose replay correctness depends on cross-input order (funnel those instead). The declaration's cardinality makes the choice explicit at the type level.

**Conformance gate L4.** Replay produces one durable output (`effectivelyOnce`); `atLeastOnce` dupes; `atMostOnce` skips; a handler failure moves no checkpoint; `ctx.emit` to an undeclared output is a type error; a multi-input processor preserves per-input order and a single-input processor sees a total order.

---

# Layer 5 — Atomicity (the batch is the unit; there is no coordinator)

**This layer was over-modeled as "Transactions" and is now demoted — the reviewer's instinct is correct, and the KV-store reference proves it.** S2 has *no* transaction concept: "**batches are the atomic unit; each append writes exactly one batch**" (≤1000 records / ≤1 MiB, all-or-none, durable-before-ack). The KV store achieves durability + linearizability + multi-primary using single-batch atomic appends and *nothing else* — no coordinator, no 2PC, no `Transaction` resource. flow inherits exactly that: **the only way to make N records atomic is to put them in one append batch on one stream.** There is no `Transaction.append/commit/abort` lifecycle and no transaction object; "a transaction" is just the record array a Processor's owned commit already assembles.

```ts
// transaction/AtomicBatch.ts — not a resource, just the budget-enforced atomic append
// The runner assembles {emits-to-own-journal, checkpoint-record} and appends them as ONE batch.
const commitAtomic = (stream: string, records: ReadonlyArray<AppendRecord>, opts: { matchSeqNum?: SeqNum; fencingToken?: string }) =>
  records.length > 1000 || sizeOf(records) > MiB
    ? Effect.fail(new BatchTooLarge({ records: records.length }))   // typed, at assembly time — not a runtime 412
    : S2Client.append(stream, AppendInput.create(records, opts))     // atomic: all-or-none
```

**The batch limit is the isolation boundary, not a fan-out cap.** Because reads are linearizable and the atomic unit is the batch, a `TableView` can never fold a *partial* batch — "uncommitted invisible until commit" holds **only** while an atomic group is exactly one batch. A group that would exceed the budget cannot silently split into multiple appends (each atomic alone, not together — a view could then fold a half-applied group); the assembly helper **fails with a typed `BatchTooLarge`** so an over-budget atomic group is *unconstructible*. Exceeding it is then an explicit modeling choice (a saga, or accepting non-atomic idempotent steps), never a silent 412.

**Checkpoint lives on the owner's own stream; downstream outputs are idempotent, never the checkpoint source.** (This resolves the cross-stream-recovery coupling.) The atomic group is *always* `{own-journal records (StepCompleted/StateChanged/emit-to-own-stream) + checkpoint cursor}` co-located on the **owner's own stream** — so restart recovers the input cursor from the owner's *own* journal, no cross-stream read. Emits to **separate downstream streams** are *not* in the checkpoint batch (they can't be — different stream); they are independent **idempotent appends with a dedup key** (Layer 4), at-least-once + dedup, decoupled from the checkpoint. A downstream output never acts as the recovery source for an input cursor.

**Cross-key atomicity is out of v1** (the only genuinely open item). The per-key stream is the atomic unit; a transactional-outbox + read-committed path is built only against a real cross-key-atomic case, and carries the isolation question (does a fold see uncommitted records?) — answered then, not now.

**Conformance gate L5.** All-or-none batch under crash; an over-budget group fails at assembly (`BatchTooLarge`), never splits; checkpoint + own-journal emits are one atomic append on the owner's stream; a downstream emit replayed twice is deduped, not double-applied.

---

# Layer 6 — Ownership & Subscription Types

Three of Pulsar's four subscription types are native; all bind to the writer side via **one** fence/lease primitive both the fenced and multi-primary surfaces consume.

**Ownership is the `y-s2` distributed-mutex recipe** (validated by S2's own production design), not a bespoke scheme. The lease *is* the source of truth for single-writer; placement (below) is only a contention-reduction optimization.

- **Exclusive / Failover** = a **lease via fencing token**. The token is `"{uuidBase64} {deadlineEpochSec}"` (≤36 B): a unique holder id **plus a deadline**. To claim, a contender appends a `fence` command record carrying its *known current* token; if another holder won the race, it 412s. **The deadline is auto-expiry** — no separate heartbeat stream is needed: the holder re-fences with a fresh deadline before expiry to renew, and a challenger that reads an expired token may claim. **Every owner-write carries the current token** (mandatory — fencing is cooperative; a no-token write still lands). A stale owner's next tokened write 412s and poisons its session. The fence value is the incarnation. Checkpoint co-commits **fence-reset + snapshot + `trim` in one atomic batch** (Layer 3) — y-s2's exact move, keeping ownership and state consistent; multiple contenders may race a checkpoint but only one commits.
- **Key_Shared** = per-key stream + *placement*. Placement decides which node should attempt the lease for a key, to reduce contention — but it is **advisory**, because the S2 fence is authoritative regardless. Two options: (a) **S2-native (default)** — skip explicit placement; whichever node receives work for a key attempts the lease and the fence arbitrates (exactly what y-s2 does). (b) **`LayerMap.Service`** keyed by entity id for single-node dynamic per-key instances (instantiate-on-incoming-event, evict-on-idle — needed by Layer 7.2 durable suspend). Distributed managed placement is a later, additive option; see the cluster note.
- **Shared** (competing consumers, round-robin) = **the gap**, off the durable-execution critical path.

**On `effect/cluster.Entity` (a deliberate non-adoption).** cluster.Entity gives an addressable id → shard → single-active-runner with `maxIdleTime` eviction and a `CurrentAddress` accessor — superficially attractive for Key_Shared placement. But its protocol is an `RpcGroup` and its persistence is **RPC-message-level**: a durable mailbox / dedup of in-flight requests, `KeepAliveRpc` marked `Persisted`+`Uninterruptible`, resume-chunk-sequencing after restart — **not an event-sourced journal of handler steps**. Building on it would run **two transports and two durability models** (cluster RPC *and* the S2 stream) — the redundancy this design exists to avoid. Verdict: **do not build on cluster.Entity.** The S2 fence + per-key stream already deliver single-active-writer with the journal *as* the transport. If distributed managed placement is later wanted, cluster `Sharding` can sit **advisorily on top** — its `toLayerQueue` mailbox feeding routing/wake signals into the S2 orchestrator, the S2 fence remaining authoritative — but that is additive, never foundational.

```ts
// ownership/Assignment.ts
export const assignment = {
  s2Native: /* default: no placement; the lease/fence arbitrates per key (y-s2 model) */,
  layerMap: /* LayerMap.Service: key → orchestrator instance, one node, evictable (durable-suspend) */,
  // clusterSharding: optional, advisory placement only — S2 fence stays authoritative
}

// ownership/Lease.ts — the y-s2 token: identity + auto-expiry deadline, ≤36 B
const leaseToken = (holder: string, deadlineEpochSec: number) => `${base64(holder)} ${deadlineEpochSec}`
```

**Conformance gate L6.** Failover resumes from the last checkpoint; a stale fenced writer is rejected and its session poisons; a lease auto-expires at its deadline and a challenger claims; fence-reset + snapshot + trim commit atomically; under `layerMap`, an evicted key re-instantiates on the next incoming record. Validate with the **Porcupine linearizability spike** (Build Plan step 13).

**The fence is single-writer *on the stream*, not single-executor *in the world*.** Claiming the fence stops the old owner's next **tokened S2 write** (412 + session poison) — it does **not** stop an in-flight handler that is mid-external-call. In the window between losing the lease and discovering the fence on its next write, a deposed owner can still complete an external side effect (the duplicate email). This is exactly why external effects are **at-least-once + idempotency key, always** (Layer 4): the fence protects durable state, the idempotency key protects the outside world. Failover narration should say "logically fenced," not "stopped."

### 6.x Activation & lifecycle (wake · evict · rehydrate)

Per-key physical streams are unlimited in S2, but **you cannot hold millions of live orchestrators** — each is a fiber + tailing reader + append session. Placement (`LayerMap`/cluster `Sharding`) answers *which node* owns a key; it does **not** answer *when an owner exists*. Two reviewer-identified gaps live here and are answered together: (a) child dispatch (Layer 7.3) and durable suspend (Layer 7.2) both assume "someone notices an ingress write to a key," but **if the owner is evicted, nobody is tailing**; (b) idle eviction + rehydration cost is unspecified. This subsection is that missing layer.

**Wake-on-ingress (the activation trigger).** An owner is **demand-instantiated**. Because no one tails a dormant key's stream, the **write path carries the wake**: `Flow.submit(stream, key, …)` to a key with no live owner pokes the **Activator**, which instantiates the `OwnedOrchestrator` for that key (claiming the lease, §6) before/as the record lands. This is what a message broker / the Restate server does — route an inbound message to a (possibly cold) virtual object. v1 = single-node `Activator` over `LayerMap` (submit checks the map; miss ⇒ build). Distributed = a placement service, or cluster `Sharding` advisorily, with the S2 fence still authoritative. **No global tailing of "the namespace" is ever required** — activation is edge-triggered by the submit, not by observing all streams.

**Eviction + rehydration is one coupled knob with failover.** An idle owner (no command and no foreign ingress for `maxIdleTime`) **checkpoints (snapshot + fence-reset + trim, §3) and tears down**. Rehydration on the next touch is **cold-fold-from-last-snapshot — the *same* path and cost as failover recovery (§6).** Therefore snapshot cadence, failover SLA, and idle-eviction-rehydration latency are **a single tuning axis**, not three: a denser snapshot makes failover *and* rehydration cheaper at higher steady write cost. Durable-execution access is sparse and bursty per key, so this is load-bearing — spec it as one knob. The durable-suspend default (Layer 7.2) is just eviction triggered by an *await* rather than by *idle*; the wake that resumes it is the resolving fact's `Flow.submit` hitting the Activator, identical to any other ingress.

```ts
// ownership/Activator.ts — demand instantiation; the submit path is the only trigger
interface Activator {
  readonly ensureOwner: (stream: string, key: string) => Effect.Effect<void>   // idempotent: build if absent (claims lease), else no-op
  readonly evictIdle:   (maxIdleTime: Duration) => Effect.Effect<void>          // checkpoint + tear down dormant owners
}
// Flow.submit wraps ensureOwner: an ingress write to a cold key wakes its owner before returning.
```

**Conformance gate L6.x.** A submit to a cold key instantiates its owner and the record is processed; an owner idle past `maxIdleTime` checkpoints and tears down; a durably-suspended invocation is re-instantiated by the resolving fact's submit and resumes by replay; rehydration latency equals failover-recovery latency for the same snapshot cadence.

---

# Layer 7 — Durable-Execution Extensions

flow as specced (Layers 0–6) is a stream processor. Three additions turn it into a durable-execution substrate. **None touch the orchestrator core** — they are additions at the Processor-runner and host level. These are the only things standing between the existing `effect-s2-durable` engine and running on flow.

The shared shape: durable waiting is **fact-driven**. A handler that awaits something unresolved parks; the resolving event is journaled onto the invocation stream; folding it resumes the handler. The three extensions are three sources of resolving facts (timers, promises/signals, child results) plus the runner machinery that parks and resumes.

## 7.1 TimerService

**Purpose.** Back `sleep(name, duration)` and any delayed delivery. flow is event-driven; a timer needs a durable mechanism to append a `TimerFired` fact at a future wall-clock time.

**Event model.** The **invocation journal is authoritative**; the timer-shard record is a *derived projection* (rebuildable from the journal), not an independent source of truth:

```ts
// on the invocation stream (the journal) — AUTHORITATIVE:
TimerSet   = { _tag: "TimerSet";   name: string; fireAt: number }    // the durable intent; fireAt journaled here
TimerFired = { _tag: "TimerFired"; name: string }                    // delivered by the TimerProcessor at fireAt
// in the TimerProcessor shard — DERIVED (idempotent upsert keyed by timerId; reconcilable from TimerSet facts):
TimerRegistration = { timerId: string; fireAt: number; target: { stream: string; key: string; name: string } }
```

`timerId` is deterministic: `${invocationStream}/${name}`. `name` defaults to journal position (same determinism rule as `run`, Layer 9).

**Mechanism — the invocation journal is the source of truth; the driver registration is a reconcilable projection.** (This closes the reviewer's cross-stream gap: if the runner wrote `TimerSet` to the invocation stream and then crashed before a *separate required* `TimerScheduled` write, replay would see a pending sleep with no driver record.) The fix is to make the driver record **derived from invocation facts, idempotently re-ensured**, never an independent fact that can desync:

1. `sleep` appends `TimerSet(name, fireAt)` to the **invocation stream** — the durable intent, the only required write. `fireAt` is journaled here (so replay doesn't recompute `now()+d`).
2. The owner then **ensures** the driver knows the timer: an idempotent upsert (keyed by `timerId`) to the `TimerProcessor` shard. This is **not** a second source of truth — it is a cache the owner can always rebuild from its own journal.
3. **Eviction is gated on registration being durable.** The durable-suspend path (Layer 7.2) tears the owner down only *after* the driver upsert acks — so an evicted owner's timers are always known to the driver. The only crash window (between the `TimerSet` and the upsert) is one in which the owner has **not yet evicted**, so on restart it replays, and **reconciliation** runs: for every `TimerSet` in the journal without a matching `TimerFired`/`TimerCanceled`, re-ensure the driver upsert (idempotent). No timer is ever lost, and no timer depends on a write that could vanish independently of the journal.
4. The `TimerProcessor` folds its registrations into a min-heap by `fireAt`, `Effect.sleep`s to the head, then `Flow.submit(target, TimerFired(name))` to the invocation stream (an unfenced ingress that **wakes the owner via the Activator**, §6.x). `TimerFired` is idempotent (guarded by `timerId`), so a re-fire after failover delivers once.

**API.** Used by the runner, not the author:

```ts
// runtime/TimerService.ts
interface TimerService {
  readonly ensure:    (timerId: string, target: TimerTarget, fireAt: number) => Effect.Effect<void, FlowError>  // idempotent upsert; reconciled from TimerSet facts
  readonly reconcile: (journal: ReadonlyArray<TimerSet>, fired: ReadonlySet<string>) => Effect.Effect<void>     // re-ensure unfired timers on (re)activation
  readonly cancel:    (timerId: string) => Effect.Effect<void, FlowError>
}
```

**Precision / S2 timestamps.** Best-effort wall-clock; granularity bounded by the driver's wake resolution — S2 has **no native delayed delivery**, so the driver fiber is required. S2 record timestamps (ms since epoch, now GA) are used two ways: (1) **`fireAt` is journaled as the `TimerSet` record's timestamp** (client-specified via `timestamping.mode`), so replay reads the recorded `fireAt` rather than recomputing `now()+d` — this *is* the determinism guarantee for `sleep`; (2) recovery seeks the timer shard **by timestamp** (`read --timestamp` / `--ago`) to bound the re-fold scan. **Monotonicity caveat:** S2 forces per-stream timestamp monotonicity, so a *later-appended* timer with an *earlier* `fireAt` would be clamped forward — therefore the schedulable `fireAt` lives in the **record body**, and the heap orders by that field (the record's own timestamp is just its creation/`fireAt`-journal point). A timer that should already have fired (replay/failover past `fireAt`) fires immediately. A canceled timer (handler completed/interrupted before `fireAt`) delivers harmlessly — the invocation's fold ignores a `TimerFired` whose await is gone.

**Conformance gate L7.1.** A `sleep` survives host kill before `fireAt` (re-fold fires it); `TimerFired` is idempotent across failover; a canceled timer does not resume a completed invocation.

## 7.2 Runner suspend/resume hook

**Purpose.** A handler that awaits an unresolved durable future — a `deferred`/`awakeable`/`signal` not yet resolved, a child result not yet arrived, a timer not yet fired — must **park** (not busy-wait, not fail) and **resume** when the resolving fact folds onto the invocation stream. This is the heart of durable execution and the unifier of its two execution modes.

**Two execution modes, one mechanism.**

- **Suspended (in-incarnation).** The handler fiber is alive. The await creates an Effect `Deferred`, registers it in a per-invocation `DeferredRegistry` keyed by the await's identity, and `yield* Deferred.await`. The orchestrator's apply loop, on folding a resolving fact, looks the key up and completes the `Deferred` → the fiber resumes at the await. Cheap, fast, no re-run.
- **Replay (failover / long wait).** A new owner re-folds the stream; the handler re-runs from the top; each `run(name)` returns its recorded value (Layer 9); when it reaches an await whose resolving fact is already folded, the await returns immediately; an await whose fact is absent re-parks (transition back to suspended mode in the new incarnation).

These are equivalent because **the resolving fact is always journaled** — whether the handler is parked-in-memory or torn-down-and-replayed, it resumes at the same await with the same value. **Durable suspend is the default.** Durable-execution waits are human-in-the-loop and routinely span **days or weeks**, so on hitting an unresolved external await (`deferred`/`awakeable`/`signal`, or a long `sleep`) the runner **tears the handler down and evicts the orchestrator** (via `layerMap`/placement, Layer 6): zero resources consumed while waiting (the Restate FaaS-suspension property), re-instantiated by replay when the resolving fact arrives. In-memory park is the **bounded-wait optimization only** — reserved for short, known-short awaits (a child call expected back in ms) where tear-down + replay would cost more than it saves. The threshold is a runner policy (per-await-kind, or a max in-memory-park duration); the author sees neither mode.

**Mechanism.**

```ts
// runtime/Suspension.ts
type AwaitKey =
  | { _tag: "promise"; name: string }
  | { _tag: "child";   childId: string }
  | { _tag: "timer";   name: string }

interface DeferredRegistry {
  // park (or, in replay mode, return immediately if the resolving fact is already folded)
  readonly await:    <A>(key: AwaitKey, decode: (folded: Option.Option<unknown>) => ResolveOutcome<A>) => Effect.Effect<A, DurableError>
  // called by the orchestrator apply loop on each resolving fact
  readonly complete: (key: AwaitKey, value: unknown) => Effect.Effect<void>
}
```

The registry lives in `CurrentInvocationScope` (Layer 8). The runner subscribes `registry.complete` to the orchestrator's `changes` stream, filtered to resolving fact types. The await's identity must be deterministic (promise `name`, `childId`) so replay re-registers under the same key.

**Serial-drainer interaction (virtual objects).** For an object key, exclusive calls are drained serially. If the resident exclusive call parks, **subsequent exclusive calls queue** (the drainer does not interleave them) — matching Restate's one-exclusive-at-a-time semantics. A long park therefore durably suspends (tear down + evict) so the key is not held in memory; the next exclusive call, or the resolving fact, re-instantiates the owner. Shared reads (`getStrong`) are unaffected — they run against the multi-primary view concurrently.

**Journal safety.** The await itself is not a journal entry; the resolving fact is. On replay the fold reconstructs which awaits are resolved. Parking and resuming perform no observable writes — only the resolving fact (written by the timer service, a signal, a child completion, or an external resolve) is journaled.

**Conformance gate L7.2.** A handler parked on a `deferred` resumes when resolved (in-memory); the same handler, host-killed and restarted, replays to the await and continues if the fact arrived, else re-parks; a durably-suspended (torn-down) handler re-instantiates on the resolving fact and continues; a parked exclusive object call queues subsequent exclusive calls and does not interleave them.

## 7.3 Child-call correlation

**Purpose.** A parent handler calling `serviceClient(S).m(x)` or `objectClient(O, k).m(x)` must (1) trigger the child as its own durable invocation, (2) route the child's terminal result back, (3) stay journal-safe (the call is a durable fact; replay does not re-trigger).

**Event model.**

```ts
// on the PARENT stream (the journal):
ChildRequested = { _tag: "ChildRequested"; childId: string; target: InvocationRef; method: string; input: unknown }
ChildCompleted = { _tag: "ChildCompleted"; childId: string; result: Result<unknown> }   // routed back to the parent
// the Invoke envelope the child receives carries a reply address:
Invoke = { method: string; input: unknown; replyTo?: { stream: string; key: string }; childId?: string; idempotencyKey?: string }
```

`childId` is deterministic: `${parentStream}/${name-or-journalPosition}` — same determinism rule as `run`. Replay reconstructs the same `childId`; **correctness comes from the child's idempotent admission on `childId`, not from replay suppressing dispatch** (the reviewer's point — those two were stated contradictorily before).

**Mechanism.**

1. The parent handler appends `ChildRequested(childId, target, method, encode(input))` to its **own** stream. This fact records the parent's *intent* (so replay reconstructs the same `childId` deterministically) — it does **not** suppress re-dispatch.
2. **Dispatch (v1: inline via the Activator; no namespace observer)** — and **re-issued idempotently on every replay until the parent observes either the child's admission ack or `ChildCompleted`.** Per-key physical streams mean there is **no single "namespace" topic** to tail, so dispatch is inline: the parent runner `Flow.submit`s to the child's stream `Invoke{ method, input, replyTo, childId, idempotencyKey: childId }`, waking the child's owner via the Activator (§6.x). Re-dispatch is **safe and required** — the first dispatch may have been lost (crash between journal and submit), so the parent must keep dispatching until it sees the child accepted; and re-dispatch is **harmless** because the child's run-once admission keys on `idempotencyKey = childId` (`insertOrGet`), so N dispatches admit the child **once**. The child runs exactly once because of the idempotency key, not because replay declines to re-send. *(Deferred scale-out: a sharded **dispatch outbox** `flow.dispatch/{shard}` a router tails — centralized routing/flow-control once the activation layer exists. Not v1.)*
3. The child runs as a normal invocation. On completion it appends `ChildCompleted(childId, result)` to `replyTo` (the parent stream) — reply address taken from the `Invoke` envelope. (Fire-and-forget children have no `replyTo` and append nothing back.)
4. The parent's suspend/resume hook (7.2) parks on `{ _tag: "child", childId }`; when `ChildCompleted(childId, result)` folds onto the parent stream, the parent resumes with the result (or a typed failure if the child failed terminally). Observing `ChildCompleted` (or the admission ack) is also what lets the parent **stop re-dispatching**.

**Two flavors.** Request-response (`yield* serviceClient(S).m(x)`) parks on `childId`. Fire-and-forget (`serviceClient(S).m.send(x)`) appends `ChildRequested` with no `replyTo` and does not park.

**API** is the authoring-side client (Layer 9/10): `serviceClient(S)` / `objectClient(O, key)` return typed clients whose methods, inside a handler, append the request and park (or send).

**Conformance gate L7.3.** Parent calls child and gets the result; the child runs **exactly once across arbitrarily many parent replays/re-dispatches** (childId admission, not replay suppression); a dropped first dispatch is recovered by re-dispatch; a terminal child failure surfaces as a typed error at the parent's await; fire-and-forget runs without the parent parking.


---

# Layer 8 — CurrentInvocationScope (the seam)

This is the single point where the durable-execution authoring layer meets flow. The ergonomic surface (Layer 10) needs **no** `Durable.*` ceremony because the free primitives (`run`/`state`/`sleep`/`signal`/`awakeable`/`deferred`) require `CurrentInvocationScope` in their `R`, and the **Processor runner discharges it per invocation**. Re-targeting the existing `effect-s2-durable` engine onto flow is *only* re-implementing this one service in terms of flow primitives — the authoring layer above does not change.

The reason the swap is clean: the existing owner-stream model already *is* flow. "One schema-addressed S2 stream per object key, ordered ActorEvent log, serial drainer by seq_num, completion from a Completed event, signals as ingress appends, state as a StateChanged projection, single in-process owner per execution" — that is a flow fenced `Processor` over a per-key `EventStream` with a `TableView` projection, term for term. flow is the extraction of that machinery.

```ts
// the per-invocation service the flow Processor runner provides. The free primitives are static accessors over it.
class CurrentInvocationScope extends Context.Service<CurrentInvocationScope, {
  readonly key:       string             // object/workflow key (execution id for services)
  readonly stream:    OwnerStream         // this execution's fenced writer (the orchestrator's single consumer = the serial drainer)
  readonly steps:     StepView            // journaled StepCompleted facts — the replay boundary for `run`
  readonly tables:    TableBindings        // stream-db Tables folded over the same stream — user `state()`
  readonly deferreds: DeferredRegistry     // park/resume (Layer 7.2) for signal · awakeable · deferred · child
  readonly timers:    TimerService         // sleep (Layer 7.1)
  readonly children:  ChildEmitter         // serviceClient · objectClient (Layer 7.3)
  readonly mode:      "live" | "replay"    // suspended-incarnation vs failover-replay (Layer 7.2)
}>()("durable/CurrentInvocationScope") {}
```

**The `run`-action boundary is the flow write-edge.** A `run` action's type **excludes** `CurrentInvocationScope` from its `R`, so `run(state(Cart).set(…))` is a compile error at the `run` call — the Effect analog of Restate's ctx-less `run` closure. A `run` action is the external-effect edge (at-least-once + idempotency key); it cannot issue durable primitives because the runner does not provide the scope inside it.

**Conformance gate L8.** The free primitives resolve the scope inside a handler and fail to typecheck inside a `run` action; the same handler code runs unchanged whether the runner is in `live` or `replay` mode.

---

# Layer 9 — Durable Primitives

Thin primitives over `CurrentInvocationScope`. `Effect` is the `Operation` (lazy, multi-run); `Fiber` is the `Future` (eager, memoized); `Effect.fork` is `spawn`; `Fiber.interrupt` is targeted cancellation. **Effect supplies the entire concurrency engine `restate-sdk-gen` hand-builds** — the fiber tree, `wake`/`advance`, the epoch guard, `onMainExit` abandon/join, `contextLocal` (→ `FiberRef`). None of that is re-implemented.

| Primitive | Lowering |
| --- | --- |
| `run(action, { name?, retry? })` | check `steps` by name (else journal position): present ⇒ return recorded value / replay recorded typed failure, **no re-run**; absent ⇒ run action under the retry `Schedule`, guarded-append `StepCompleted(key, encode(result))` (effectively-once via owner fence + `match_seq_num`), fold. Crash-before-append re-runs (at-least-once). |
| `state(Table).get/set/delete` | binding over the execution stream's fold for that table (`stream-db` reducer in flow's generic `TableView`). `set`⇒`StateChanged` append + fold; `get`⇒read the **owned** fold (apply-on-ack ⇒ read-after-ack sees the write); `delete`⇒delete change. |
| `sleep(name, d)` | `timers.schedule` ⇒ `TimerSet` + `TimerScheduled` (ack) ⇒ park ⇒ `TimerFired` folds ⇒ resume (Layer 7.1). |
| `deferred(name)` / `awakeable()` | `DurablePromise` stream + resolution view; `get` folds (parks via `deferreds` if unresolved); `resolve`/`reject` appends `Resolved`/`Rejected` (idempotent, first wins). |
| `signal(name, payload)` | unfenced `Flow.submit` to the target stream (ingress append) — folded by the target's drainer, **no resident call required**. |
| `serviceClient(S).m` / `objectClient(O,k).m` | `ChildRequested(childId)` + dispatch + park on `childId` ⇒ `ChildCompleted` resumes (Layer 7.3). `.send` = request without park. |
| `all(fs)` / `allSettled(fs)` | **pure `Effect.all` / `Effect.forEach`** — each constituent `run` journals itself; no decision entry; outcome is input-order + journaled constituents. |
| `race(fs)` / `select(branches)` / `any(fs)` | **journaled-decision** combinators: raw `Effect.race` picks a winner by wall-clock, so replay could diverge — these record a `Decision(winner)` fact so replay re-takes the branch. Still free primitives delegating to the scope, not a namespace. |
| `spawn(op)` | `Effect.fork` + a journal-path `FiberRef` bump (deterministic step keys inside spawned fibers: key = `${fiberPath}/${name}`, `fiberPath` = parent path + spawn index). |
| cancellation | a typed `Cancelled` error in the Effect **error channel** (recoverable: `catchTag("Cancelled", …)` then yield more durable steps ⇒ journaled cleanup ⇒ recover) + `Fiber.interrupt` for in-flight `run` I/O abort (the `AbortSignal`). Effect runs finalizers on interrupt (better default than restate-sdk-gen's prompt no-`finally` abandon); finalizers performing durable steps route through the typed-error path. |

**Determinism — keyed journal matching, not issue-position.** Restate matches journal entries positionally, needing a deterministic *issue order* its custom scheduler guarantees. Effect's fiber scheduler does **not** guarantee issue order across concurrent fibers, so flow matches by **deterministic key**: every durable step carries a stable name (`run("reserve")`), the journal is keyed by it, and concurrent steps landing in any stream order replay correctly. Unnamed steps fall back to journal position and therefore require deterministic control flow (branch only on input + already-journaled results, never wall-clock/random/un-journaled reads) — name the steps to track identity instead of position.

**Conformance gate L9.** `run` returns recorded values on replay and never re-runs; positional keys replay under deterministic control flow; `race`/`select` re-take the journaled branch on replay; `Effect.all` needs no decision entry; cancellation is recoverable and journaled as terminal (`CancelledError`, code 409, not retried).

---

# Layer 10 — Authoring Surface

restate-sdk-gen-shaped: group handlers as **bare generator methods** (`*greet(input) { … }`) — the input is the argument, no `handlerRequest`, no `Effect.gen` wrapper — and call through a typed client that hides the execution id and the submit/attach dance. Inside, `yield* run(...)` etc. stay typed (an `Effect` is `yield*`-able). The runner adapts the generator to an `Effect` and installs `CurrentInvocationScope`.

```ts
export const Flow = { /* Layers 2–6 facade: eventStream, tableView, processor, source, sink, submit, transaction, runners, assignment, Guarantee */ }

// effect-s2-durable — the authoring surface (no Durable.* namespace; free primitives over CurrentInvocationScope)
export { service, object, workflow }                       // declarations → flow Processor (+ TableView for objects)
export { run, state, sharedState, sleep, signal, awakeable, deferred }  // free durable primitives
export { all, allSettled, race, select, any, spawn }       // combinators (all/allSettled = pure Effect; race/select/any journaled)
export { serviceClient, objectClient }                     // child calls
export { client, sendClient, attach, poll }                // call surface (hides execution id)
```

| Authoring primitive | flow lowering |
| --- | --- |
| `service({ name, handlers })` | stateless `Processor` over `${name}.invocation/{execId}` — one stream per execution, fenced single-writer (= the "running map" owner) |
| `object({ name, handlers })` | per-key fenced `Processor` over `${name}.events/{key}` (the ActorEvent log) + `ObjectStateView` (the StateChanged projection); the orchestrator's single consumer **is** the serial drainer by seq_num |
| `workflow` | `object` + run-once admission (`insertOrGet` on the run-started event, guarded by workflow id) |
| exclusive vs shared handler | fenced single-writer (apply-on-ack) vs `TableView.getStrong` (apply-on-tail) — the Layer 1 fork; `state()` resolves to the owned fold in exclusive, the view in shared |
| `*method(input)` | the Processor's handler; runner installs `CurrentInvocationScope` and adapts the generator to an `Effect` |
| `client(s).m(x)` | `Flow.submit(invocationStream, execId, Invoke("m", x))` (unfenced ingress) ⇒ `getStrong` the roster view until terminal ⇒ decode |
| `sendClient` / `attach(id)` / `poll(id)` | submit-only ⇒ execId / `getStrong` roster until terminal / view `get` (non-blocking); `idempotencyKey` pins the id via guarded-append admission |
| completion (`return`) | atomic-batch commit result to the roster view, await ack, drop the execution stream, mark `resultAcked` — durable-fact-outlives-destructive-op (snapshot/roster-ack-before-trim) |

The roster/result-outlives-stream invariant is flow's Layer 3 `Checkpoint` discipline (durable-before-trim) applied to invocation completion.

---

# Layer 11 — Worked Examples

The cited surfaces, **byte-for-byte the authoring code**, with the lowering annotated. Only `CurrentInvocationScope`'s implementation moved (from the WorkflowDb/owner-stream engine to a flow Processor runner); "Effect all the way down" stays literally true.

### 11.1 greeter — service + durable step + retry

```ts
import { Duration, Effect } from "effect"
import { run, service } from "effect-s2-durable"
import { client } from "effect-s2-durable/invocation"

const greeter = service({
  name: "greeter",
  handlers: {
    *greet(req: { name: string }) {
      const greeting = yield* run(Effect.sync(() => `Hello, ${req.name}!`), {
        retry: { maxAttempts: 3, initialInterval: Duration.millis(100) },
      })
      return { greeting }
    },
  },
})
// service(greeter)         → stateless Processor over greeter.invocation/{execId}, fenced owner
// run(action, { retry })   → steps miss → run under Schedule → guarded-append StepCompleted(pos, …) → fold (replay: recorded value)
// return { greeting }      → roster commit + ack → drop stream → resultAcked
// client(greeter).greet(x) → submit Invoke("greet", x) → getStrong roster until terminal
```

### 11.2 basics — combinators (pure Effect vs journaled)

```ts
import { service, run, all, race, select } from "effect-s2-durable"

const basics = service({
  name: "basics",
  handlers: {
    *sequential() {                          // two journaled steps, source order
      const a = yield* run(() => fetchA(), { name: "a" })
      const b = yield* run(() => fetchB(), { name: "b" })
      return `${a}-${b}`
    },
    *parallel() {                            // pure Effect.all: each step journaled by name, NO decision entry
      const [a, b] = yield* all([run(() => fetchA(), { name: "a" }), run(() => fetchB(), { name: "b" })])
      return `${a}+${b}`
    },
    *whicheverFirst() {                      // race: winner recorded as Decision(winner) so replay re-takes it
      return yield* race([run(() => fetchFast(), { name: "primary" }), run(() => fetchSlow(), { name: "secondary" })])
    },
    *knowingWhichWon() {                     // select = race + tag
      const r = yield* select({ fast: run(() => fetchFast(), { name: "fast" }), slow: run(() => fetchSlow(), { name: "slow" }) })
      return r._tag === "fast" ? `fast-won: ${r.value}` : `slow-won: ${r.value}`
    },
  },
})
// stateless Processor; each run(name) = journal-check-then-guarded-append StepCompleted(name, …).
// all → two independent journaled steps; race/select → additionally append Decision(winner).
```

### 11.3 counter — virtual object, exclusive write / shared read

```ts
import { object, state, sharedState } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"
import { client } from "effect-s2-durable/invocation"

class CounterState extends Table<CounterState>("counterState")({ id: Schema.String.pipe(primaryKey), value: Schema.Number }) {}

const counter = object({
  name: "counter",
  handlers: {
    *add(amount: number) {                   // exclusive: Accepted(add) drained serially by the orchestrator
      const st = state(CounterState)
      const cur = Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
      yield* st.set({ id: "v", value: cur + amount })   // StateChanged(counterState) append + fold
      return cur + amount                    // → Completed event; client decodes from it
    },
    *value() {                               // shared read: ObjectStateView.getStrong(key), concurrent while no writer holds the key
      const st = sharedState(CounterState)
      return Option.match(yield* st.get("v"), { onNone: () => 0, onSome: (r) => r.value })
    },
  },
})
// object(counter)           → per-key fenced Processor over counter.events/{key} + ObjectStateView projection
yield* client(counter, "user-1").add(5)    // → 5   (counter.events/user-1)
yield* client(counter, "user-2").value()   // → 0   (different key = different stream = isolated)
```

### 11.4 checkout — durable state + the run-action boundary

```ts
import { service, state, run } from "effect-s2-durable"
import { primaryKey, Table } from "effect-s2-stream-db"

class Cart extends Table<Cart>("cart")({ cartId: Schema.String.pipe(primaryKey), items: Schema.Array(Schema.String) }) {}

const checkout = service({
  name: "checkout",
  handlers: {
    *go(_req: { user: string }) {
      const cart = state(Cart)                            // names the `cart` fold over THIS execution's stream — synchronous
      yield* cart.set({ cartId: "c1", items: ["apple"] }) // StateChanged(cart) append + fold
      const current = yield* cart.get("c1")               // owned fold (apply-on-ack) — read-after-ack sees the write
      // yield* run(cart.set(…))  ← COMPILE ERROR: a run action's R excludes CurrentInvocationScope (the flow write-edge)
      return current
    },
  },
})
```

### 11.5 blockAndWait — durable promise + suspend/resume (exercises Layer 7.2)

```ts
import { workflow, state, sharedState, deferred } from "effect-s2-durable"

class WfState extends Table<WfState>("wf")({ id: Schema.String.pipe(primaryKey), input: Schema.String }) {}

const blockAndWait = workflow({
  name: "blockAndWait",
  run: function*(input: string) {
    yield* state(WfState).set({ id: "in", input })
    const done = deferred<string>("done")
    const value = yield* done.get                 // parks the run fiber until resolved (Layer 7.2)
    return value
  },
  handlers: {
    *unblock(output: string) {                    // shared: anyone with the workflow id can resolve
      yield* deferred<string>("done").resolve(output)   // append Resolved("done", output); idempotent, first wins
    },
    *getInput() { return Option.getOrNull(yield* sharedState(WfState).get("in")) },
  },
})
// workflow = object + run-once admission (run-started guarded by workflow id).
// deferred("done") → DurablePromise stream keyed (wfId,"done") + PromiseResolutionView.
// done.get unresolved → Processor parks the run fiber; `unblock` appends Resolved → fold → resume.
```

### 11.6 orchestration — child calls + sleep (exercises Layers 7.1 and 7.3)

```ts
import { service, run, sleep, serviceClient, all } from "effect-s2-durable"

const saga = service({
  name: "saga",
  handlers: {
    *placeOrder(req: { orderId: string }) {
      // child call: ChildRequested(childId) → dispatch → park on childId → ChildCompleted resumes (7.3)
      const reserved = yield* serviceClient(inventory).reserve({ orderId: req.orderId })
      yield* sleep("settle-delay", Duration.seconds(30))   // TimerSet+TimerScheduled → park → TimerFired (7.1)
      // fan-out children, pure Effect.all (each child journaled by its childId)
      const [charged, shipped] = yield* all([
        serviceClient(billing).charge({ orderId: req.orderId }),
        serviceClient(shipping).schedule({ orderId: req.orderId }),
      ])
      return { reserved, charged, shipped }
    },
  },
})
// each serviceClient(_).m(x) → guarded-append ChildRequested(childId=`saga.invocation/{exec}/{pos}`) → dispatch (idempotent by childId)
// → child runs as its own invocation → emits ChildCompleted(childId) to saga's stream → parent resumes.
// sleep survives crash (re-fold fires it); replay re-issues child dispatch idempotently until admission/completion.
```

---

# Build Plan & Conformance Gates (in order)

Each step is gated by its layer's conformance check; do not start a layer until the one below passes.

-1. **`effect-s2` fidelity spike — generate first, wrap second.** Pin `s2-streamstore/s2-specs` to commit `329de93f7b240a4daef9edbeb98ced0699aab7d0`, run HeyAPI into `effect-s2/src/generated` with local `effect-schema` and `client-effect` plugins, and do not check in the OpenAPI spec. The generated protocol must be an Effect `HttpApi` contract plus `HttpApiClient`-derived client, not a custom fetch/request runtime. The public Effect layer is handwritten only around semantics OpenAPI cannot represent: auth/config layers, normalized S2 domain errors, append-session ticketing/backpressure, Producer, serialization/framing, dedupe headers, u64 codecs, fencing/CAS helpers. Gate: generation is reproducible; generated output typechecks; generated Effect output includes `append`/`read`/`checkTail`, `Schema` model declarations, and typed protocol errors; semantic tests prove `matchSeqNum`, `fencingToken`, append-session tickets, Producer per-record seqnums, serialization roundtrip, dedupe headers, and u64 encode/decode. This step explicitly replaces the current broad hand-maintained `S2ClientApi` with generated protocol + narrow semantic capabilities.
0. **Decide packaging + `stream-db`'s fate first** (before any TableView code). The orchestrator + TableView are the primitive substrate; resolve whether they incubate inside `effect-s2-stream-db` or graduate to `effect-s2-flow` (see Resolved Decisions / Packaging) so flow's `TableView` and stream-db's `Table` do not coexist with two concurrency models mid-build. Also land **"thread `fencingToken` through `StreamDb`/keyed-store appends"** as a concrete item — the baseline is CAS-only today.
1. **L1 Orchestrator — two implementations.** `ViewOrchestrator` (apply-on-tail) *and* `OwnedOrchestrator` (apply-on-ack own writes + own-record-filtered tail reader). Bounded queues + drop policy on `changes` + deadline on pending reads. Gate: reproduce the KV-demo observable semantics **and** the property-based linearizability checker below; own-write RYW holds on `OwnedOrchestrator`; the tail reader never double-applies own records.
   - **Property-based linearizability checker (gate on steps 1–3, not optional).** Replaying the demo's *example* trace won't catch a concurrency violation, and linearizability is the headline claim. Generate interleaved `submit`/`read-strong`/`read-eventual` histories and check the strong path is linearizable (eventual no worse than sequentially consistent) with a Porcupine/Knossos-style checker — model the orchestrator + fence as a register `(tail, last_record_hash, fence_token)`, model `match_seq_num`, and set an **indefinitely-failed append's end-time after all other ops** (the Jepsen gotcha that `match_seq_num`-retry resolves; mirror `s2-streamstore/s2-verification`). This is the gate that *earns* the Layer 1 claim.
2. **L2 Streams** — `EventStream`/`Record`/`Source`/`Sink` over `Channel`. Gate: typed codec errors; consistent empty/missing.
3. **L3 TableView** — fold + `getStrong` + `changes` + `Checkpoint`. Gate: linearizable read after concurrent write (checker above); cold-start-from-snapshot equals full replay.
4. **L4 Processor** — handler + guarded emit + guarantee Layers. Gate: idempotent durable output on replay (positional on own journal; logical-id on shared downstream); at-least-once dupes; at-most-once skips.
5. **L5 Atomicity** — the budget-enforced atomic append (no transaction resource). Gate: all-or-none batch under crash; an over-budget group fails at assembly (`BatchTooLarge`), never splits silently; checkpoint + own-journal emits are one atomic append on the owner's stream; a downstream emit replayed twice is deduped (SDK dedupe headers), not double-applied.
6. **L6 Ownership + Activation/Lifecycle (§6.x).** Lease/fence (y-s2 token), the `Activator` (wake-on-ingress), idle eviction + cold rehydration. Gate: failover resumes from checkpoint; stale fenced writer rejected + session poisons; a submit to a **cold** key instantiates its owner; idle owner checkpoints and tears down; rehydration latency = failover latency at equal snapshot cadence.
7. **L7.1 TimerService** — TimerProcessor + durable schedule. Gate: `sleep` survives host kill; `TimerFired` idempotent; canceled timer inert.
8. **L7.2 Suspend/resume** — `DeferredRegistry` + apply-loop completion + durable-suspend/evict (wakes via the Activator). Gate: park-resume in-memory; replay-to-await; torn-down re-instantiate on the resolving fact's submit; exclusive calls queue.
9. **L7.3 Child correlation** — `ChildRequested` + **inline dispatch via the Activator** + `ChildCompleted` reply routing. Gate: result routes back; replay re-dispatches idempotently until admission/completion; child failure typed; fire-and-forget; exactly-once child execution with no namespace observer.
10. **L8 CurrentInvocationScope** — the runner provider. Gate: free primitives resolve in handlers, fail in `run` actions; identical code in live/replay.
11. **L9 Durable primitives** — `run`/`state`/`sleep`/`signal`/`awakeable`/`deferred` + combinators. Gate: replay returns recorded values; journaled `race`/`select`; recoverable cancellation.
12. **L10 Authoring + L11 examples** — `service`/`object`/`workflow`/`client`; the six examples pass, including crash-restart-without-duplicate-output for the saga **and** a read-after-emit-on-own-state case (exercises RYW, which the order-tracker example does not).

---

# Reference — Primitive & Lowering Quick-Reference

**Streams & ownership.** Per-key physical stream `${name}/${encode(key)}`. Cursor `seqNum` is `SeqNum(u64)` (use the SDK's `encodeU64`/`decodeU64`; the f64 `number` is negligible under per-key sharding — flag, don't gate). `OwnedOrchestrator` ⇒ **ordered** apply-on-ack own writes (fast-path only when next; else pending-own; reply after ordered apply ⇒ RYW) + foreign-ingress tail reader; `ViewOrchestrator` ⇒ apply-on-tail + `check_tail`. Ownership = `y-s2` lease/fence token `"{uuid} {deadline}"` (deadline = auto-expiry), authoritative; placement advisory; an owner is demand-instantiated by the **Activator** on ingress. Unfenced `Flow.submit` for ingress/signals/child-requests; fenced `ctx.emit` for processor outputs (positional dedup on own journal, SDK dedupe-headers on shared downstream).

**The resolving facts (what un-parks a handler).** `TimerFired(name)` (7.1) · `Resolved(name,value)` / `Rejected(name,err)` for promises & signals · `ChildCompleted(childId,result)` (7.3). All journaled on the invocation stream; folding any of them completes a `DeferredRegistry` entry.

**Determinism.** Named durable steps ⇒ keyed journal matching (concurrency-safe). Unnamed ⇒ positional (requires deterministic control flow). `race`/`select`/`any` ⇒ `Decision(winner)` fact. `all`/`allSettled` ⇒ pure Effect, no decision entry. Spawned-fiber step keys ⇒ `${fiberPath}/${name}` via `FiberRef`.

**Effect ≙ restate-sdk-gen.** `Operation`=`Effect` · `Future`=`Fiber` · `spawn`=`Effect.fork` · `task.interrupt`=`Fiber.interrupt` · `onMainExit:"abandon"`=fork-in-handler-scope · `onMainExit:"join"`=`Fiber.join` · `contextLocal`=`FiberRef` · the epoch guard / `won` flag / stale-waiter pruning = **gone** (Effect's runtime owns fiber lifecycle). Cancellation = typed `Cancelled` error (recoverable) + `Fiber.interrupt` for I/O.

**Layer map (logical, not a package decree).** `transport` (S2 client) → `runtime` orchestrator + streams + materialization (L1–7) → `authoring` (L8–10). Physically: transport = `effect-s2`; runtime + authoring incubate where the substrate already lives (`effect-s2-stream-db`) and graduate to `effect-s2-flow` only once proven (see Packaging). The arrows are dependency direction, not a mandate to create packages now.

**Use generated S2 protocol + SDK patterns, don't re-implement transport.** The raw control/data-plane protocol is generated from `s2-specs` with HeyAPI plus local Effect-native plugins, not manually mirrored and not post-processed. Ordered + backpressured writes ⇒ S2 SDK **append session / Producer API** (`maxInflightBytes` backpressure, per-record ordered ack with exact `seqNum`). Downstream idempotency ⇒ SDK **dedupe headers** (`injectDedupeHeaders` / `DedupeFilter`). Large (>1 MiB) messages ⇒ SDK **chunking + framing** (note: a framed message spans multiple records and is *not* atomic if it exceeds a batch). Exact seqnums ⇒ SDK **`encodeU64`/`decodeU64`** (use these; do not depend on the f64 `number`). The `effect-s2` public surface is semantic capabilities over those generated/pattern primitives, not a giant hand-maintained client bag.

---

# Non-Goals

- No exactly-once arbitrary external side effects — at-least-once + idempotency key, always.
- **No transaction coordinator or `Transaction` resource** — the S2 atomic batch (one append, one stream) is the only atomicity unit; cross-stream atomicity is replaced by idempotent dedup keys, never 2PC.
- No cross-stream atomic transactions in v1 — the per-key stream is the atomic unit.
- No Shared-subscription competing-consumer dispatch — Key_Shared (per-key streams) is native; Shared is not.
- No schema registry service — `Schema` is the spine; evolution is decode-time migration; a registry-as-stream is available later.
- No re-implementation of a workflow scheduler — Effect is the scheduler. The durable layer is primitives + three flow extensions.
- No broker, worker scheduler, or subscription protocol.

# Resolved Decisions

The prior open questions are now decided (numbering preserved):

1. **Suspend model (7.2) — durable suspend by default.** Waits are human-in-the-loop and span days/weeks, so tear-down + evict + replay-on-resolution is the default; in-memory park is the bounded-wait optimization for known-short awaits only.
2. **Timers (7.1) — use S2 record timestamps.** GA timestamps journal `fireAt` on the `TimerSet` record (replay determinism) and bound recovery via time-indexed reads. `fireAt` lives in the record body (monotonicity caveat); no native delayed delivery, so the driver fiber stands.
3. **Child dispatch (7.3) — v1 is inline via the Activator** (no namespace observer; idempotent by `childId`, replayable, woken through §6.x); semantics modeled on Restate's resilient RPC (no-duplicates, retried). The centralized **dispatch-outbox/router** is the deferred scale-out option, available once the activation layer exists.
4. **Multi-input (L4) — first-class, like Pulsar.** Funnel-into-one-inbox is the durable-execution default (deterministic order); declared multi-input is allowed for order-insensitive/commutative processing, with per-input order preserved and cross-input interleaving non-deterministic.
5. **Ownership / placement (L6) — the `y-s2` distributed-mutex recipe; `cluster.Entity` not adopted** (see the S2 Research Alignment section). The lease/fence is authoritative; placement is advisory.
6. *(was: cluster entity API)* — resolved by #5: **do not build on `cluster.Entity`** (RPC-and-mailbox-persistence, a redundant second transport). S2 fence + per-key stream is the substrate; cluster `Sharding` is an optional advisory placement layer later.
7. **`seqNum` — `SeqNum(u64)` via the SDK's `encodeU64`/`decodeU64`.** Prefer the SDK codec over the f64 `number`; exposure is negligible under per-key sharding anyway (2^53 ≈ millennia per stream). Flag upstream, don't gate.
8. **`stream-db` / packaging — the *layering* is firm, the *package cut* is deferred.** `TableView` is a substrate-level (orchestrator) primitive; whether it lives in `effect-s2-stream-db` or a graduated `effect-s2-flow` is decided **before** TableView work begins (Build Plan step 0), not mid-build — so two concurrency models never coexist.
9. **Naming — `effect-s2-flow`** stands for the *layer*, used here for the substrate whether or not it becomes a standalone package.

Round-2 review (stream-crossing correctness):

10. **No `Transaction` resource (L5) — the atomic batch is the unit.** S2 has no transaction concept; the KV-store reference achieves everything with single-batch appends. Atomicity = co-locate records in one append on one stream; the budget is enforced (`BatchTooLarge`); checkpoint lives on the owner's own stream; downstream is idempotent dedup, not a checkpoint source.
11. **`OwnedOrchestrator` applies own writes in stream order (L1).** Fast-path on ack only when the write is next (`ack.start === applied`); else hold pending-own and apply when the cursor catches up; the write reply completes after ordered apply (RYW preserved, fold never reordered). Adopt the SDK append session / Producer API for the ordered, backpressured writer rather than hand-rolling it.
12. **Timer (7.1) — the invocation journal is the source of truth; the driver registration is a reconcilable projection** (idempotent upsert, re-ensured on activation, eviction gated on its durability). No separately-losable required write.
13. **Child dispatch (7.3) — replay re-issues dispatch idempotently until admission/`ChildCompleted` is observed.** Exactly-once is from `childId` admission, not replay suppression.
14. **`effect-s2` generation — HeyAPI plugin generation is the primary route.** Since HeyAPI owns the parser/plugin orchestration and upstream-friendly symbol model, `effect-s2` generates its protocol module with `@hey-api/openapi-ts` plus local `effect-schema` and `client-effect` plugins. `client-effect` emits an Effect `HttpApi` contract and derives the client via `HttpApiClient`; it must not emit or depend on a bespoke request executor. The handwritten layer is limited to semantics OpenAPI cannot encode: scoped configuration, domain error normalization, append-session/Producer lifecycles, serialization/framing, dedupe headers, exact u64 helpers, and ergonomic fenced/CAS append helpers.

The remaining genuinely-open item: **cross-key transactions (L5)** stay deferred — reserve the per-key atomic boundary now; build the transactional-outbox + read-committed isolation only against a real cross-key-atomic case.

**Packaging (reviewer-raised — resolve before step 0).** Two reviewers flagged that a broad new `effect-s2-flow` package risks becoming "the new runtime drawer," and that `TableView` (flow) coexisting with `Table` (stream-db) reconstructs the two-concurrency-model hazard the design exists to prevent. The resolution: **the layering is the firm commitment; the package boundary is not.** Recommended path — **incubate the orchestrator + `TableView` inside `effect-s2-stream-db`** (as the substrate beneath its relational/IVM surface) and **graduate to `effect-s2-flow` only once the primitive has proven out**, rather than standing up the broad package on day one. This honors the greenfield-cutover principle (decide the *data/concurrency* model before building — which is step 0) without paying premature-package cost. If the user's existing stream-db substrate work already wants the split, take it; the SDD does not mandate the package, only the layering.

# S2 Research Alignment

The design intentionally tracks S2's own published research; the load-bearing mappings:

**Distributed mutex → Layer 6 ownership.** S2's [distributed-mutex recipe](https://s2.dev/blog/durable-yjs-rooms#a-distributed-mutex) (from `y-s2`) *is* the ownership design: lease via a `"{uuid} {deadline}"` fencing token, cooperative fencing, the deadline as auto-expiry (no separate heartbeat), and an atomic fence-reset + trim co-commit on checkpoint. This is a production-validated pattern, not a novel scheme — adopting it verbatim is the lowest-risk path and removes the bespoke heartbeat machinery from the earlier draft. It also demonstrates that **explicit placement is optional** — the lease arbitrates — which is the core argument against needing `cluster.Entity`.

**`cluster.Entity` → not adopted (substrate-minimality).** Reading the [source](https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/unstable/cluster/Entity.ts): an entity is an `RpcGroup` protocol addressed by id, sharded to a single active runner, with idle eviction and a persisted *mailbox* (dedup of in-flight RPCs, `KeepAliveRpc`), but **no event-sourced step journal**. Its durability is at the message layer; ours is the S2 stream. Building on it means two transports + two durability models. The placement/eviction concepts are worth borrowing conceptually (and cluster `Sharding` can sit advisorily on top later), but the S2 fence stays the authoritative single-writer guarantee. This directly answers the "are you advocating for `cluster.Entity`?" question: **no.**

**Timestamping → TimerService.** [S2 timestamps](https://s2.dev/blog/timestamping) are monotonic ms-epoch with timestamp-indexed reads and a `timestamping.mode` knob. Used to journal `fireAt` deterministically and to bound recovery scans; the forced-monotonicity property is the reason `fireAt` is a body field rather than the stream timestamp for out-of-order schedules.

**Linearizability tooling → the validation spike.** [S2's linearizability work](https://s2.dev/blog/linearizability) uses **Porcupine** + deterministic simulation, modeling the stream as a register and handling indefinite-append-failures by extending their end-time past all ops. The flow build plan adopts the same: model the orchestrator + fence as a register and check ownership/strong-read histories with Porcupine (`s2-streamstore/s2-verification` as the reference), validating the single-writer claim empirically.

**Access control & isolation → higher layer, aligned with existing work.** Per-key-stream isolation is already the structural boundary (one tenant/agent ⇒ its own streams; a different key is a different stream, unreachable without its token). S2 [scoped access tokens](https://s2.dev/blog/access-control) and [infrastructure-layer isolation for agents](https://s2.dev/blog/distributed-ai-agents#enforcing-isolation-at-the-infrastructure-layer) map onto issuing per-namespace/per-key scoped tokens at the invocation boundary — which dovetails with the existing biscuit-scoping / `agent.pw` credential-proxy approach rather than introducing a new mechanism. Not specified in detail here (it is a deployment/security concern above the runtime), but the per-key-stream substrate makes it a token-scoping exercise, not an architectural change.

# References

- S2 — concurrency control (match_seq_num, fencing): https://s2.dev/docs/concepts/concurrency-control
- S2 — appends (atomic batch, durability, storage classes): https://s2.dev/docs/concepts/appends
- S2 — command records (fence, trim): https://s2.dev/docs/concepts/command-records
- S2 — shared-log KV store (orchestrator, check_tail, bus-stand): https://s2.dev/blog/kv-store · `s2-streamstore/s2-kv-demo`
- Pulsar — Functions / processing guarantees: https://pulsar.apache.org/docs/next/functions-concepts/
- Pulsar — transactions: https://pulsar.apache.org/docs/next/txn-how/
- Pulsar — IO overview: https://pulsar.apache.org/docs/next/io-overview/
- Pulsar — TableView: https://pulsar.apache.org/docs/next/concepts-clients/#tableview
- Pulsar — schema overview: https://pulsar.apache.org/docs/next/schema-overview/
- effect-smol (Effect v4): https://github.com/Effect-TS/effect-smol/blob/main/LLMS.md
- effect-smol cluster `Entity` (read for the non-adoption verdict): https://github.com/Effect-TS/effect-smol/blob/main/packages/effect/src/unstable/cluster/Entity.ts
- S2 — distributed mutex / leases (the ownership recipe): https://s2.dev/blog/durable-yjs-rooms#a-distributed-mutex
- S2 — timestamping (TimerService): https://s2.dev/blog/timestamping
- S2 — linearizability testing with Porcupine + DST (validation spike): https://s2.dev/blog/linearizability · `s2-streamstore/s2-verification`
- S2 — access control & agent isolation: https://s2.dev/blog/access-control · https://s2.dev/blog/distributed-ai-agents#enforcing-isolation-at-the-infrastructure-layer
- S2 — Rust `SeqNum(u64)`: `s2-streamstore/s2` › `sdk/src/types.rs`
- Restate — `restate-sdk-gen` (authoring shape, Operation/Future, cancellation/interrupt): `restatedev/sdk-typescript` › `packages/libs/restate-sdk-gen`
- Restate — service communication (child-call model): https://docs.restate.dev/develop/ts/service-communication
- Companion SDDs: `effect-s2-flow-sdd.md` (folded in here), `effect-durable-execution-sdd.md`, `effect-s2-durable-consolidation-sdd.md`
- Baseline: `effect-s2/{S2Client,Channel}.ts`, `effect-s2-stream-db/{StreamDb,MaterializedState,ChangeMessage}.ts`
